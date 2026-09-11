-- reverse.lua
-- 控制面管理员“冲正”一笔已经调成（confirmed）的真用掉。在 Redis 单线程内
-- 原子完成：业务号鉴真 → 校验（有效密钥 / 已调成 / 没冲过 / 不超原量）→
-- 把额度退回原池原维度 → 在同一条只追加流水上 XADD 一笔 reversal。
--
-- 冲正语义（硬约束）：
--   * 只能冲【还有效的密钥】：cfg 不存在 / revoked=1 一律拒绝。停用之后这道
--     门永久关上；但已经冲过的流水仍可翻、对账单仍可拉（读路径不碰 revoked）。
--   * 只能冲【已经调成的那一笔】：以业务号索引（lii ZSET）翻出这笔业务的
--     全部流水，必须存在 confirm；只有 reserve（还占着）/ release（没调成、
--     超时退回）/ 什么都没有的，一律拒。
--   * 一笔业务号只能冲一次：索引里已存在 reversal（或占用单上留过标记），
--     再来只回原冲正结论（code=2），绝不二次退额度、不写第二笔流水。
--   * 冲的数量不能超过那笔当时真用掉的：0 < amount <= confirm.cost。
--     （当前每请求 cost=1，amount 只能为 1；cost=N 的场景支持部分冲正，
--       但一笔业务号仍只允许冲一次，不接受分多次。）
--   * 冲过的额度立刻回到“还能再占”：退回这笔 confirm 当时真扣的两个维度——
--     突发：令牌加回原池令牌桶（允许高于桶容量，溢余令牌不随补充被抹掉，
--       与 confirm 允许扣成负数的“先花后补”对称）；
--     窗口：从 confirm 落的那个固定桶 INCRBY -amount。
--     计数 key 已不在（维度早已自然老化、账上本来也不再算它真用掉）的维度
--     跳过，绝不凭空新建一个负数/溢余 key——账已经平了就不重复退。
--   * 冲正只退回额度，绝不改写历史：原来的 reserve/confirm/release 流水一笔
--     不动，只追加 reversal 一笔；Stream 没有修改单条的命令，本脚本也没有
--     任何 XDEL/XTRIM/改写路径。
--
-- 冲正退回的池/计数 key 以【那笔 confirm 流水与占用单】为准，不信任请求传入：
-- 占用单还在（24h 内）直接读单上记的 tb/cap/win_prefix/cbucket；单子已过期，
-- 按 confirm 流水里的 pool/caller 与当前 cfg/shares 重建（与 reserve.lua 的
-- 选池规则一致），原窗口桶由 confirm 流水的 entry id 时间反推。
--
-- KEYS[1] = cfg    配置 hash，例如 {qk:<kid>}cfg
-- KEYS[2] = res    目标占用单（由业务号命名），例如 {qk:<kid>}res:<sha256(业务号)>
--                 （单子可能已过期不在：以流水为准，仍可冲正）
-- KEYS[3] = ledger 占用流水 Stream，例如 {qk:<kid>}ledger（只 XADD，永不修改）
--
-- ARGV:
--  1  kid      密钥ID
--  2  idem     业务号（Idempotency-Key）：冲正按业务号点名，必填
--  3  amount   冲回数量（整数，>0，<= 该笔 confirm 的 cost）
--  4  note     管理员备注（可空串，随流水留档，长度由控制面限制）
--
-- 返回：
--   {1, reversal_eid, amount, confirmed_cost, tb_refunded, win_refunded,
--    pool, caller, pool_burst_refunded, pool_window_refunded, pool_pid}  冲正成功
--        tb_refunded/win_refunded 为 1 表示该维度计数还在、额度已退回，
--        为 0 表示该维度已老化、无账可退（不是错误）
--   {2, reversal_eid, amount, confirmed_cost}         这笔业务已经冲过（幂等重放）
--   {0, "key not found"}                                              密钥不存在
--   {0, "revoked"}                                                    密钥已停用
--   {0, "idem_required"}                                              没给业务号
--   {0, "bad_amount"}                                                 数量非法
--   {0, "not_confirmed"}      这笔业务没有调成（还占着/已退回/查无此单）
--   {0, "amount_exceeds", confirmed_cost}   冲的数量超过当时真用掉的

local cfg_key    = KEYS[1]
local res_key    = KEYS[2]
local ledger_key = KEYS[3]

local kid    = ARGV[1]
local idem   = ARGV[2]
local amount = tonumber(ARGV[3])
local note   = ARGV[4] or ""
-- ARGV[5]（可选）：这笔冲正实际落在备钥 kid 上、但要镜像回该主钥的账本/统计。
-- 主备顶上备钥确认的调用，管理员仍对主钥点名冲正，由控制面跨钥转发。
local mirror_master = ARGV[5] or ""

-- 非池密钥不进下面的池退款分支：先声明为本地变量，避免 XADD/返回表
-- 引用到未声明全局（Redis 7 对未声明全局访问直接脚本报错）。
local pool_pid = ""
local pool_burst_refunded = 0
local pool_window_refunded = 0

if idem == "" then
  return {0, "idem_required"}
end
if not amount or amount <= 0 or amount ~= math.floor(amount) then
  return {0, "bad_amount"}
end
if redis.call("EXISTS", cfg_key) == 0 then
  return {0, "key not found"}
end
-- 密钥停了以后不能再冲（已经冲过的走 code=2 幂等分支，照样查得到）
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked"}
end
-- 跨钥冲正：主钥停了也拒绝新冲正（与“只能冲有效密钥”同一道门）
if mirror_master ~= ""
   and redis.call("HGET", "{qk:" .. mirror_master .. "}cfg", "revoked") == "1" then
  return {0, "revoked"}
end

local SCALE = 1000
local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]

-- 把 XRANGE/HGETALL 的扁平 {k,v,...} 收成 table
local function to_map(flat)
  local m = {}
  for i = 1, #flat, 2 do m[flat[i]] = flat[i + 1] end
  return m
end

-- 按业务号索引把这笔业务的全部事件翻出来（reserve/confirm/reversal 至多几笔）
local lii_key = "{qk:" .. kid .. "}lii:" .. redis.sha1hex(idem)
local eids = redis.call("ZRANGE", lii_key, 0, -1)
local conf, resv, rev, mirror_conf, mirror_rev
for i = 1, #eids do
  local rows = redis.call("XRANGE", ledger_key, eids[i], eids[i])
  if rows[1] then
    local eid = rows[1][1]
    local f = to_map(rows[1][2])
    local kind = f["kind"] or ""
    local is_mirror = f["fbmirror"] == "1"
    if kind == "confirm" then
      if is_mirror then mirror_conf = {eid = eid, f = f}
      else conf = {eid = eid, f = f} end
    elseif kind == "reversal" then
      if is_mirror then mirror_rev = {eid = eid, f = f}
      else rev = {eid = eid, f = f} end
    elseif kind == "reserve" and not is_mirror then
      resv = {eid = eid, f = f}
    end
  end
end

local res_flat = redis.call("HGETALL", res_key)
local rh = to_map(res_flat)

-- 一笔业务号只能冲一次：以本钥【原生】reversal 或占用单冲正标记为准。
-- 镜像（fbmirror=1）是顶上备钥那笔在主钥账本里的投影，绝不据此判定已冲，
-- 否则主钥侧冲正会被自己账本里的镜像短路。
if rev or (rh["rev_amount"] and rh["rev_amount"] ~= "") then
  local eid, amt, orig_cost
  if rev then
    eid = rev.eid
    amt = tonumber(rev.f["cost"]) or 0
    orig_cost = tonumber(rev.f["of"]) or (conf and tonumber(conf.f["cost"])) or amt
  else
    eid = rh["rev_eid"] or ""
    amt = tonumber(rh["rev_amount"]) or 0
    orig_cost = conf and tonumber(conf.f["cost"]) or amt
  end
  return {2, eid, amt, orig_cost, mirror_master ~= "" and mirror_master or ""}
end

-- 只能冲已经调成的那一笔（以本钥原生 confirm 为准）
if not conf then
  -- 主钥账本里已有这笔业务的【镜像 reversal】：说明顶上备钥那笔已经冲过，
  -- 幂等回原结论，不能再跨到备钥重复冲。
  if mirror_rev then
    -- 第 5 段回带备钥 kid（镜像上的 fb_key），控制面据此标明这笔实际冲在备钥
    return {2, mirror_rev.eid,
            tonumber(mirror_rev.f["cost"]) or 0,
            tonumber(mirror_rev.f["of"]) or 0,
            mirror_rev.f["fb_key"] or ""}
  end
  -- 主备顶上：这笔业务可能实际占到了绑定的备钥。返回 not_here 让控制面
  -- 跨到备钥账本确认并冲正（主钥 lii 里只有镜像、没有原生 confirm）。
  local bkid_v = redis.call("GET", "{qk:" .. kid .. "}fbb") or ""
  if bkid_v ~= "" then
    local blii = "{qk:" .. bkid_v .. "}lii:" .. redis.sha1hex(idem)
    local beids = redis.call("ZRANGE", blii, 0, -1)
    for i = 1, #beids do
      local brows = redis.call(
        "XRANGE", "{qk:" .. bkid_v .. "}ledger", beids[i], beids[i])
      if brows[1] then
        local bf = {}
        for j = 1, #brows[1][2], 2 do bf[brows[1][2][j]] = brows[1][2][j + 1] end
        if bf["kind"] == "confirm" and bf["fbmirror"] ~= "1" then
          -- 这笔业务确实占到备钥并调成了：跨到备钥冲正并镜像回本钥账本
          return {0, "not_here", bkid_v}
        end
      end
    end
  end
  return {0, "not_confirmed"}
end
local cost = tonumber(conf.f["cost"] or "1") or 1
if amount > cost then
  return {0, "amount_exceeds", cost}
end

local pool   = conf.f["pool"] or ""
local caller = conf.f["caller"] or ""

-- 定位当初真扣的计数 key：占用单还在就以单子为准；不在则按流水+配置重建
local tb_key, cap, refill_ms, win_prefix
if #res_flat > 0 and rh["tb"] and rh["tb"] ~= "" then
  tb_key     = rh["tb"]
  cap        = tonumber(rh["cap"])
  refill_ms  = tonumber(rh["refill_ms"])
  win_prefix = rh["win_prefix"]
else
  if pool == "share" and caller ~= "" then
    local ch = redis.sha1hex(caller)
    tb_key     = "{qk:" .. kid .. "}stb:" .. ch
    win_prefix = "{qk:" .. kid .. "}swin:" .. ch .. ":"
    local sj = redis.call("HGET", "{qk:" .. kid .. "}shares", caller) or ""
    if sj ~= "" then
      local ok, s = pcall(cjson.decode, sj)
      if ok and type(s) == "table" then
        cap = tonumber(s["burst_capacity"])
      end
    end
  else
    tb_key     = "{qk:" .. kid .. "}tb"
    win_prefix = "{qk:" .. kid .. "}win:"
    local reserved_burst = tonumber(redis.call("HGET", cfg_key, "reserved_burst") or "0") or 0
    local total_cap = tonumber(redis.call("HGET", cfg_key, "burst_capacity"))
    if total_cap then cap = total_cap - reserved_burst end
  end
  refill_ms = tonumber(redis.call("HGET", cfg_key, "burst_refill_ms"))
end

local win_seconds
if #res_flat > 0 and rh["win_seconds"] and rh["win_seconds"] ~= "" then
  win_seconds = tonumber(rh["win_seconds"])
else
  win_seconds = tonumber(redis.call("HGET", cfg_key, "window_seconds"))
end

-- ── 退回突发维度：令牌加回原池令牌桶 ──────────────────────────────────────
-- 只在令牌桶还在时退：桶 key 已按 TTL 老化，说明这笔扣减早已被时间补平，
-- 余量口径里本就不再算它真用掉，再新建桶反而是凭空多发。
local tb_refunded = 0
if tb_key and cap and refill_ms and cap > 0 and refill_ms > 0
   and redis.call("EXISTS", tb_key) == 1 then
  local old_tokens = tonumber(redis.call("HGET", tb_key, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", tb_key, "ts"))
  local tokens
  if not old_tokens or not old_ts then
    tokens = cap * SCALE
  elseif old_tokens >= cap * SCALE then
    -- 桶里已是溢余状态（此前冲正退回的）：补充只补到容量为止，溢余不被抹掉
    tokens = old_tokens
  else
    local elapsed = now_us - old_ts
    if elapsed < 0 then elapsed = 0 end
    local refilled = math.floor((elapsed / 1000) * SCALE / refill_ms)
    tokens = math.min(cap * SCALE, old_tokens + refilled)
  end
  tokens = tokens + amount * SCALE
  redis.call("HSET", tb_key, "tokens", tokens, "ts", now_us)
  local ttl_ms = math.ceil((cap * SCALE - tokens) * refill_ms / SCALE) + 5000
  if ttl_ms < 5000 then ttl_ms = 5000 end
  -- 溢余令牌（高于桶容量的部分）不会被“补满时间”自然消化，给足留存地板，
  -- 避免冲正退回的额度因 key TTL 凭空消失；期间任何活动都会续期。
  if tokens > cap * SCALE then
    ttl_ms = math.max(ttl_ms, 86400000)
  end
  redis.call("PEXPIRE", tb_key, ttl_ms)
  tb_refunded = 1
end

-- ── 退回窗口维度：从当初 confirm 落的那个固定桶扣减 ────────────────────────
-- 优先用占用单记下的桶 key（settle.lua 在确认时落盘）；老单子没有则按
-- confirm 流水的 entry id（即确认时刻 Redis 毫秒时间戳）反推桶 index。
-- 桶已过期不在的：窗口早已把这笔老化掉，不新建负数桶（无账可退）。
local win_refunded = 0
if win_seconds and win_seconds > 0 then
  local cbucket = rh["cbucket"]
  if (not cbucket or cbucket == "") and win_prefix and win_prefix ~= "" then
    local cut = string.find(conf.eid, "-", 1, true)
    local confirm_ms = tonumber(string.sub(conf.eid, 1, cut - 1)) or now_ms
    local win_ms = win_seconds * 1000
    cbucket = win_prefix .. win_seconds .. ":" .. math.floor(confirm_ms / win_ms)
  end
  if cbucket and cbucket ~= "" and redis.call("EXISTS", cbucket) == 1 then
    redis.call("INCRBY", cbucket, -amount)
    win_refunded = 1
  end
end

-- 跨密钥池：如果原确认带 pool_pid，同步把聚合池真用掉的那一笔记回池计数器。
-- 密钥有效但池已停时，原池口径仍可退款；池配置缺失/已老化的维度按无账可退。
local resv_pool_pid = rh["pool_pid"] or conf.f["pool_pid"] or ""
if resv_pool_pid ~= "" then
  pool_pid = resv_pool_pid
  local pcfg = "{qp:" .. pool_pid .. "}cfg"
  local ptb = "{qp:" .. pool_pid .. "}tb"
  local pcap = tonumber(redis.call("HGET", pcfg, "burst_capacity"))
  local pref = tonumber(redis.call("HGET", pcfg, "burst_refill_ms"))
  if pcap and pref and redis.call("EXISTS", ptb) == 1 then
    local ptokens
    local pold = tonumber(redis.call("HGET", ptb, "tokens"))
    local pts = tonumber(redis.call("HGET", ptb, "ts"))
    if not pold or not pts then
      ptokens = pcap * SCALE
    elseif pold >= pcap * SCALE then
      ptokens = pold
    else
      local pelapsed = now_us - pts
      if pelapsed < 0 then pelapsed = 0 end
      ptokens = math.min(pcap * SCALE,
        pold + math.floor((pelapsed / 1000) * SCALE / pref))
    end
    ptokens = ptokens + amount * SCALE
    redis.call("HSET", ptb, "tokens", ptokens, "ts", now_us)
    local pttl = math.ceil((pcap * SCALE - ptokens) * pref / SCALE) + 5000
    if pttl < 5000 then pttl = 5000 end
    if ptokens > pcap * SCALE then pttl = math.max(pttl, 86400000) end
    redis.call("PEXPIRE", ptb, pttl)
    pool_burst_refunded = 1
  end

  local pwin = tonumber(rh["pool_win_seconds"])
  local pcb = rh["pool_cbucket"]
  if not pwin then
    pwin = tonumber(redis.call("HGET", pcfg, "window_seconds"))
  end
  if (not pcb or pcb == "") and pwin then
    local cut = string.find(conf.eid, "-", 1, true)
    local cms = tonumber(string.sub(conf.eid, 1, cut - 1)) or now_ms
    pcb = "{qp:" .. pool_pid .. "}win:" .. pwin .. ":"
      .. math.floor(cms / (pwin * 1000))
  end
  if pcb and pcb ~= "" and redis.call("EXISTS", pcb) == 1 then
    redis.call("INCRBY", pcb, -amount)
    pool_window_refunded = 1
  end
end

-- ── 冲正自己留一笔：与上面的计数退回同一原子动作 ──────────────────────────
local reserve_at = "0"
if resv then
  reserve_at = resv.f["reserve_at"] or "0"
elseif conf.f["reserve_at"] then
  reserve_at = conf.f["reserve_at"]
end
local reversal_eid = redis.call("XADD", ledger_key, "*",
  "kind", "reversal", "kid", kid,
  "res", conf.f["res"] or res_key,
  "caller", caller,
  "idem", idem,
  "pool", pool,
  "cost", tostring(amount),
  "reason", "reversal",
  "late", "0",
  "reserve_at", reserve_at,
  "of", tostring(cost),
  "note", note,
  "pool_pid", pool_pid,
  "fb_for", mirror_master)
local score = tonumber(string.sub(reversal_eid, 1,
  string.find(reversal_eid, "-") - 1)) or now_ms
if caller ~= "" then
  redis.call("ZADD", "{qk:" .. kid .. "}lci:" .. redis.sha1hex(caller),
             score, reversal_eid)
end
redis.call("ZADD", lii_key, score, reversal_eid)

-- 跨钥冲正：把 reversal 也镜像到主钥账本（主钥流水/对账可见、可幂等），
-- 并把“备钥已替主钥真用掉”的累计减回来。计数退回仍只发生在备钥原桶。
local mirrored = 0
if mirror_master ~= "" then
  local mk = "{qk:" .. mirror_master .. "}ledger"
  local meid = redis.call("XADD", mk, "*",
    "kind", "reversal", "kid", mirror_master,
    "res", conf.f["res"] or res_key,
    "caller", caller,
    "idem", idem,
    "pool", pool,
    "cost", tostring(amount),
    "reason", "reversal",
    "late", "0",
    "reserve_at", reserve_at,
    "of", tostring(cost),
    "note", note,
    "pool_pid", pool_pid,
    "fb_for", mirror_master,
    "fbmirror", "1",
    "fb_key", kid)
  local mscore = tonumber(string.sub(meid, 1, string.find(meid, "-") - 1)) or now_ms
  if caller ~= "" then
    redis.call("ZADD", "{qk:" .. mirror_master .. "}lci:" .. redis.sha1hex(caller),
               mscore, meid)
  end
  redis.call("ZADD", "{qk:" .. mirror_master .. "}lii:" .. redis.sha1hex(idem),
             mscore, meid)
  redis.call("HINCRBY", "{qk:" .. mirror_master .. "}fbsv", "used", -amount)
  mirrored = 1
end

-- 占用单还在时留个冲正标记（与流水互为双保险；单子 24h TTL，长期事实以流水为准）
if #res_flat > 0 then
  redis.call("HSET", res_key,
    "rev_amount", amount,
    "rev_at_ms", now_ms,
    "rev_eid", reversal_eid,
    "pool_pid", pool_pid,
    "pool_rev_burst", pool_burst_refunded,
    "pool_rev_window", pool_window_refunded)
end

return {1, reversal_eid, amount, cost, tb_refunded, win_refunded, pool, caller,
        pool_burst_refunded, pool_window_refunded, pool_pid, mirrored,
        mirror_master ~= "" and mirror_master or ""}
