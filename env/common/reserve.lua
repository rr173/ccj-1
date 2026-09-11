-- reserve.lua
-- 数据面每真实调用执行一次“先占额度”。在 Redis 单线程内原子完成：
--   1) 幂等去重：同一 Idempotency-Key（业务号）已有占用单 → 返回同一张单子，
--      绝不二次占额；单子上记着上游结论，数据面据此补确认或补退回
--   2) 读取密钥配置（不存在 / 已停用直接拒绝；停用后所有份额立即失效）
--   3) 选择本次生效的“池”：
--        - 调用方被点名（shares 表中有其份额）：只占用自己的预留池，
--          自己的占满了公共池还有也不能拿；
--        - 未被点名：只占用公共池（总量 − 全部预留），吃不到别人留的份额
--   4) 短周期突发：令牌桶（允许突发 + 平滑补充）
--   5) 长周期总量：两个相邻固定桶加权的滑动窗口（避免固定窗口边界双倍突发）
--   6) 判定 + 登记占用同一原子动作：占成才登记，占不住不产生任何占用
--
-- 占用只登记、不真扣：占用期间这笔额度从“可占余量”里消失（别人拿不走），
-- 但不进已确认计数；等 settle.lua 了结——
--   * confirm（调成了）：占用转为真用掉，此刻才扣令牌桶、才进窗口计数；
--   * release（没调成 / 过了约定回音时限）：占用登记作废，额度立刻能再占。
-- 占用登记放在两个 ZSET 里（member=占用单号#序号，score=约定回音时限的毫秒
-- 时间戳）：每次判定先清掉已过期的登记，因此数据面崩溃、调用方失联都不会
-- 把额度占死——过了约定时限没回音，占着的自动退回给别人用。
--
-- 时间一律取 Redis 的 TIME，调用方本地时钟不参与判定。
--
-- KEYS[1] = cfg    配置 hash，例如 {qk:<kid>}cfg
-- KEYS[2] = tb     公共池令牌桶 hash，例如 {qk:<kid>}tb
-- KEYS[3] = shares 份额配置 hash，例如 {qk:<kid>}shares（控制面写，此处只读）
-- KEYS[4] = res    占用单 hash，例如 {qk:<kid>}res:<sha256(业务号)>（匿名调用
--                  由数据面生成随机单号；同一业务号命中同一张单子）
-- KEYS[5] = ledger 占用流水 Stream，例如 {qk:<kid>}ledger（只 XADD，永不修改）
--
-- ARGV:
--  1  kid                密钥ID（仅用于拼窗口桶 key）
--  2  burst_capacity     密钥令牌桶总容量（短周期突发上限）
--  3  burst_refill_ms    每补充 1 个令牌所需毫秒（份额池沿用同一速率）
--  4  window_seconds     长周期窗口长度（秒；份额池沿用同一窗口长度）
--  5  window_quota       密钥长周期总量
--  6  cost               本次占用（通常 1）
--  7  idem               幂等键 / 业务号（空串表示匿名调用，不做去重）
--  8  lease_seconds      约定回音时限（秒）：占用后超时未确认/未退回，
--                        占用登记自动失效，额度退回池里给别人用
--  9  client             调用方名（X-Client-Id；空串表示匿名，只可能命中公共池）
--  10 pool_pid           跨密钥共享池 ID；空串表示该密钥当前不进池（可选）
--  11 pool_pres          预生成的池侧占用单 key；与密钥侧占用单成对超时释放（可选）
--
-- 返回（全部为整/字符串数组）：
--   {1, remaining_burst, remaining_window, lease_expires_unix}        占成功
--   {0, reason, retry_after_ms, remaining_burst, remaining_window}    占失败
--   {2, lease_expires_unix, outcome, remaining_burst, remaining_window} 幂等重放
--       outcome: ""（占用还在）/ "confirmed" / "released" /
--                "timeout"（约定回音时限过了没回音，已按超时退回，不能再占）
--   {3, error_message}                                                参数/配置错误
--
-- 设计要点：
--  * 令牌桶以千分之一令牌为单位存整数，避免浮点漂移导致“凭空多出额度”。
--  * 公共池计数沿用 {qk:<kid>}tb / {qk:<kid>}win:<len>:<idx>，与旧版“调用即扣”
--    的历史计数完全连续；占用登记在 {qk:<kid>}holds / {qk:<kid>}wholds。
--    份额池用 {qk:<kid>}stb:<sha1(caller)> / {qk:<kid>}swin:<sha1(caller)>:*
--    与 {qk:<kid>}sholds:<sha1(caller)> / {qk:<kid>}swholds:<sha1(caller)>，
--    互不干扰。
--  * 所有 key 都带 {qk:<kid>} hash tag：同一密钥落在同一集群 slot。
--  * 份额配置只在控制面经 set_share.lua 原子校验后写入（Σ份额 ≤ 总量），
--    本脚本信任 shares 表与 cfg.reserved_* 的一致性，只读不改。

local cfg_key    = KEYS[1]
local tb_key     = KEYS[2]
local shares_key = KEYS[3]
local res_key    = KEYS[4]
local ledger_key = KEYS[5]

local kid             = ARGV[1]
local capacity        = tonumber(ARGV[2])
local refill_ms       = tonumber(ARGV[3])
local window_seconds  = tonumber(ARGV[4])
local window_quota    = tonumber(ARGV[5])
local cost            = tonumber(ARGV[6])
local idem            = ARGV[7]
local lease_seconds   = tonumber(ARGV[8])
local client          = ARGV[9] or ""
local pool_pid        = ARGV[10] or ""
local pool_pres       = ARGV[11] or ""

if not capacity or not refill_ms or not window_seconds or not window_quota
   or not cost or not lease_seconds or capacity <= 0 or refill_ms <= 0
   or window_seconds <= 0 or window_quota <= 0 or cost <= 0 or lease_seconds <= 0 then
  return {3, "bad arguments"}
end

-- Redis 服务器时间（秒 + 微秒），毫秒与微秒精度时间戳
local t          = redis.call("TIME")
local now_ms     = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us     = t[1] * 1000000 + t[2]

-- 1) 配置存在性：控制面写入；数据面只读。
if redis.call("EXISTS", cfg_key) == 0 then
  return {3, "key not found"}
end

-- 占用流水：只追加，绝不修改。XADD 到 per-key Stream（entry id 即 Redis
-- 毫秒时间戳），同时按调用方 / 业务号各挂一个 ZSET 索引供翻账。
-- reserve/settle 三条事件都在这里写，停用密钥也照样留笔（只不允许新占）。
local function ledger_add(kind, rk, cost_val, reason, late, rfields)
  local eid = redis.call("XADD", ledger_key, "*",
    "kind", kind, "kid", kid,
    "res", rk,
    "caller", rfields.caller or "",
    "idem", rfields.idem or "",
    "pool", rfields.pool or "",
    "cost", tostring(cost_val),
    "reason", reason or "",
    "late", late and "1" or "0",
    "reserve_at", tostring(rfields.reserve_at_ms or 0),
    "pool_pid", pool_pid)
  local score = tonumber(string.sub(eid, 1, string.find(eid, "-") - 1)) or now_ms
  local caller = rfields.caller or ""
  if caller ~= "" then
    local ci = "{qk:" .. kid .. "}lci:" .. redis.sha1hex(caller)
    redis.call("ZADD", ci, score, eid)
  end
  local idem = rfields.idem or ""
  if idem ~= "" then
    local ii = "{qk:" .. kid .. "}lii:" .. redis.sha1hex(idem)
    redis.call("ZADD", ii, score, eid)
  end
  return eid
end

-- 把一张“过了约定回音时限还没回音”的占用单终结为超时退回：
-- 占用登记从 ZSET 删除（额度立刻能再占），单子置 outcome="expired"，
-- 留一笔 release/reason=timeout 流水。幂等：只处理还占着（outcome=""）
-- 的单子；密钥已停用也照样退、照样留笔（流水在停用后仍可拉）。
-- 迟到的真实确认随后仍可把 expired 推进为 confirmed（见 settle.lua），
-- 不丢真实调用。
local function finalize_expired(rkey)
  local f = redis.call("HMGET", rkey, "outcome", "lease_exp_ms", "cost",
                       "holds", "wholds", "caller", "idem", "pool",
                       "reserved_at_ms", "pool_pid", "pool_pres")
  local fo = f[1]
  if not fo or fo ~= "" then
    return false
  end
  local exp_ms = tonumber(f[2] or "0") or 0
  if exp_ms > now_ms then
    return false
  end
  local cost_val = tonumber(f[3] or "1") or 1
  local members = {}
  for i = 1, cost_val do
    members[#members + 1] = rkey .. "#" .. i
  end
  if f[4] then
    redis.call("ZREM", f[4], unpack(members))
  end
  if f[5] then
    redis.call("ZREM", f[5], unpack(members))
  end
  redis.call("HSET", rkey, "outcome", "expired")
  ledger_add("release", rkey, cost_val, "timeout", false,
             {caller = f[6] or "", idem = f[7] or "", pool = f[8] or "",
              reserve_at_ms = f[9] or "0"})
  -- 跨密钥池请求的密钥侧单子先超时：同步摘掉池侧占用，避免另一边还挂着。
  -- 池侧 timeout 流水由 pool_reserve.lua 在从池侧发现时补；这里已经为密钥
  -- 账本记过同一原因，不能在密钥流水上重复记。
  local ppid = f[10] or ""
  local ppr  = f[11] or ""
  if ppid ~= "" and ppr ~= "" and redis.call("EXISTS", ppr) == 1 then
    local pf = redis.call("HMGET", ppr, "outcome", "lease_exp_ms", "cost",
                          "holds", "wholds")
    if pf[1] == "" and (tonumber(pf[2] or "0") or 0) <= now_ms then
      local pcost = tonumber(pf[3] or "1") or 1
      local pmm = {}
      for i = 1, pcost do pmm[#pmm + 1] = ppr .. "#" .. i end
      if pf[4] and pf[4] ~= "" then redis.call("ZREM", pf[4], unpack(pmm)) end
      if pf[5] and pf[5] ~= "" then redis.call("ZREM", pf[5], unpack(pmm)) end
      redis.call("HSET", ppr, "outcome", "expired")
    end
  end
  return true
end

-- 2) 幂等：同一业务号再来，命中同一张占用单（匿名调用单号随机，必不命中）
if idem ~= "" then
  local prev = redis.call("HGETALL", res_key)
  if #prev > 0 then
    local h = {}
    for i = 1, #prev, 2 do h[prev[i]] = prev[i + 1] end
    local outcome = h.outcome or ""
    if outcome ~= "" then
      -- 已了结：直接给原结论（只是回报历史，密钥停用后也一样），绝不二次占额。
      -- 超时退回（expired）对调用方等同“这笔业务没调成，占用已退回”，
      -- 换一个业务号才能再试——同一业务号的占/退都只记一笔。
      local shown = outcome == "expired" and "timeout" or outcome
      return {2, tonumber(h.lease_exp or 0), shown,
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0)}
    end
    if tonumber(h.lease_exp_ms or "0") > now_ms then
      -- 占用还在约定回音时限内（上次可能调到一半数据面崩了，调用方在重试）。
      -- 密钥已停用的，不能再把这张单子递出去调上游——占用等时限自动退回
      if redis.call("HGET", cfg_key, "revoked") == "1" then
        return {0, "revoked", 0, 0, 0}
      end
      -- 同一张单子继续用，绝不二次占额
      return {2, tonumber(h.lease_exp or 0), "",
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0),
              h.pool_pid or "", h.pool_pres or ""}
    end
    -- 占用已超约定时限还没回音：先按超时退回旧占用（留 timeout 流水，额度回池），
    -- 再以“这笔业务已了结为退回”拒绝重占。同一业务号的占、退各只有一笔，
    -- 再来不能再记；迟到的真实回音若赶到仍会补 confirmed（settle.lua）。
    finalize_expired(res_key)
    return {2, tonumber(h.lease_exp or 0), "timeout",
            tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0)}
  end
end

-- 3) revoked 检查在任何份额逻辑之前：密钥一停用，所有份额与公共池立即失效
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked", 0, 0, 0}
end

local SCALE = 1000  -- 令牌内部放大倍数

-- 4) 选定本次生效的池：点名调用方占自己那份，未点名占公共池
local eff_capacity   -- 生效令牌桶容量
local eff_win_quota  -- 生效窗口总量
local eff_tb_key     -- 生效令牌桶 key（确认时才真正扣它）
local win_prefix     -- 生效窗口计数 key 前缀（":<len>:<idx>" 之前）
local holds_key      -- 生效池突发占用登记（ZSET）
local wholds_key     -- 生效池窗口占用登记（ZSET）
local pool           -- "shared" / "share"（记入占用单，了结时按单退回/转结）

local share_json = ""
if client ~= "" then
  share_json = redis.call("HGET", shares_key, client) or ""
end

if share_json ~= "" then
  -- 点名调用方：只能用自己那份，配额与计数都独立于公共池
  local s = cjson.decode(share_json)
  eff_capacity  = tonumber(s["burst_capacity"]) or 0
  eff_win_quota = tonumber(s["window_quota"]) or 0
  local ch = redis.sha1hex(client)
  eff_tb_key = "{qk:" .. kid .. "}stb:" .. ch
  win_prefix = "{qk:" .. kid .. "}swin:" .. ch .. ":"
  holds_key  = "{qk:" .. kid .. "}sholds:" .. ch
  wholds_key = "{qk:" .. kid .. "}swholds:" .. ch
  pool = "share"
else
  -- 未点名：公共池 = 总量 − 全部预留（reserved_* 由控制面维护，可能为 0）
  local reserved_burst  = tonumber(redis.call("HGET", cfg_key, "reserved_burst") or "0") or 0
  local reserved_window = tonumber(redis.call("HGET", cfg_key, "reserved_window") or "0") or 0
  eff_capacity  = capacity - reserved_burst
  eff_win_quota = window_quota - reserved_window
  eff_tb_key    = tb_key
  win_prefix    = "{qk:" .. kid .. "}win:"
  holds_key     = "{qk:" .. kid .. "}holds"
  wholds_key    = "{qk:" .. kid .. "}wholds"
  pool = "shared"
end

-- 5) 清掉已过约定回音时限的占用登记：没回音的占额自动退回，别人可再占。
--    清出来的 member 找到对应占用单，把还挂着 outcome="" 的单子终结为
--    超时退回并留一笔 release/timeout 流水（幂等；两个 ZSET 各清一遍，
--    finalize_expired 自身按单子状态去重）。
local function sweep_holds(zkey)
  local expired_members = redis.call("ZRANGEBYSCORE", zkey, "-inf", now_ms)
  if #expired_members > 0 then
    redis.call("ZREMRANGEBYSCORE", zkey, "-inf", now_ms)
    local seen = {}
    for _, m in ipairs(expired_members) do
      local cut = string.find(m, "#", 1, true)
      if cut then
        local rkey = string.sub(m, 1, cut - 1)
        if not seen[rkey] then
          seen[rkey] = true
          finalize_expired(rkey)
        end
      end
    end
  end
end
sweep_holds(holds_key)
sweep_holds(wholds_key)
local held_burst  = redis.call("ZCARD", holds_key)
local held_window = redis.call("ZCARD", wholds_key)

-- 6) 滑动窗口（长周期总量）：当前桶 + 前一桶加权，再减去还在占用中的
local win_ms    = window_seconds * 1000
local cur_idx   = math.floor(now_ms / win_ms)
local cur_start = cur_idx * win_ms
local prev_key  = win_prefix .. window_seconds .. ":" .. (cur_idx - 1)
local cur_key   = win_prefix .. window_seconds .. ":" .. cur_idx

local cur_used  = tonumber(redis.call("GET", cur_key) or "0")
local prev_used = tonumber(redis.call("GET", prev_key) or "0")
local elapsed_in_bucket = now_ms - cur_start  -- 0 .. win_ms-1
local prev_weight = (win_ms - elapsed_in_bucket) / win_ms
local window_used = math.floor(cur_used + prev_used * prev_weight)

local need_tokens = cost * SCALE

-- 生效池突发容量为 0（额度全部预留给点名调用方）：本窗口内等令牌没有意义
-- （桶永远填不进 1 个），让调用方等到窗口边界再试——届时份额配置可能已调整。
-- 宁可保守也不给出偏小的 Retry-After。
if eff_capacity <= 0 then
  local retry_ms = win_ms - elapsed_in_bucket
  if retry_ms < 1 then retry_ms = 1 end
  return {0, "burst_limited", retry_ms, 0,
          math.max(eff_win_quota - window_used - held_window, 0)}
end

-- 7) 令牌桶（短周期突发）：占用阶段只读不扣，确认时才真正扣减
local tokens
if redis.call("EXISTS", eff_tb_key) == 0 then
  tokens = eff_capacity * SCALE
else
  local old_tokens = tonumber(redis.call("HGET", eff_tb_key, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", eff_tb_key, "ts"))
  if not old_tokens or not old_ts then
    tokens = eff_capacity * SCALE
  elseif old_tokens >= eff_capacity * SCALE then
    -- 桶里已是溢余状态（冲正退回的额度加回了令牌桶）：时间补充只补到
    -- 容量为止，溢余部分不随补充被抹掉，冲回去的额度立刻能再占。
    tokens = old_tokens
  else
    local elapsed = now_us - old_ts  -- 微秒
    if elapsed < 0 then elapsed = 0 end
    -- refill_ms 是“每补充 1 个令牌的毫秒数”：elapsed 先换算成毫秒
    local elapsed_ms = elapsed / 1000
    local refilled = math.floor(elapsed_ms * SCALE / refill_ms)
    tokens = math.min(eff_capacity * SCALE, old_tokens + refilled)
  end
end

-- 可占余量 = 桶内令牌 − 已确认未补充的 − 还在占用中的（占着的别人不能拿）
local avail_tokens  = tokens - held_burst * SCALE
local rem_burst     = math.max(math.floor(avail_tokens / SCALE), 0)
local rem_window    = math.max(eff_win_quota - window_used - held_window, 0)

-- 8) 判定：占不住时告诉调用方还要等多久
if avail_tokens < need_tokens then
  -- 令牌不足：等到补充满 cost 个令牌（向上取整），并以填满整桶时间为保守上界。
  -- 占用中的额度随时可能退回，实际可占时刻只会更早——这里给的是不依赖
  -- 别人行为的、可证明的等待上界。
  local wait_ms = math.ceil((need_tokens - avail_tokens) * refill_ms / SCALE)
  local full_ms = eff_capacity * refill_ms
  if wait_ms > full_ms then wait_ms = full_ms end
  if wait_ms < 1 then wait_ms = 1 end
  return {0, "burst_limited", wait_ms, rem_burst, rem_window}
end

if window_used + held_window + cost > eff_win_quota then
  -- 总量用尽：等到跨入下一个固定桶。届时上一桶整体滑出加权窗口，至少释放
  -- prev_used 个额度——这是不依赖扣减时间分布的、可证明的最早放行时刻，
  -- 宁可让调用方多等，也不给出偏小的 Retry-After 引导其过早重试。
  local retry_ms = win_ms - elapsed_in_bucket
  if retry_ms < 1 then retry_ms = 1 end
  return {0, "window_limited", retry_ms, rem_burst, 0}
end

-- 9) 占成功：登记占用（ZSET，member=占用单号#序号，score=回音时限），
--    并落盘占用单。令牌桶与窗口计数此刻不动——确认时才真扣。
local lease_exp_ms = now_ms + lease_seconds * 1000
local lease_exp    = math.floor(lease_exp_ms / 1000)
local zadd_argv = {}
for i = 1, cost do
  zadd_argv[#zadd_argv + 1] = lease_exp_ms
  zadd_argv[#zadd_argv + 1] = res_key .. "#" .. i
end
redis.call("ZADD", holds_key, unpack(zadd_argv))
redis.call("ZADD", wholds_key, unpack(zadd_argv))
-- 登记 key 的 TTL：最后一批登记失效即无信息量，留少量缓冲
local holds_ttl_ms = lease_seconds * 1000 + 5000
redis.call("PEXPIRE", holds_key, holds_ttl_ms)
redis.call("PEXPIRE", wholds_key, holds_ttl_ms)

local new_rem_burst  = math.max(math.floor((avail_tokens - need_tokens) / SCALE), 0)
local new_rem_window = math.max(eff_win_quota - window_used - held_window - cost, 0)

redis.call("HSET", res_key,
  "kid", kid,
  "pool", pool,
  "caller", client,
  "idem", idem,
  "cost", cost,
  "tb", eff_tb_key,
  "win_prefix", win_prefix,
  "holds", holds_key,
  "wholds", wholds_key,
  "cap", eff_capacity,
  "refill_ms", refill_ms,
  "win_seconds", window_seconds,
  "lease_exp", lease_exp,
  "lease_exp_ms", lease_exp_ms,
  "reserved_at_ms", now_ms,
  "rem_burst", new_rem_burst,
  "rem_window", new_rem_window,
  "pool_pid", pool_pid,
  "pool_pres", pool_pres,
  "outcome", "")
-- 占用流水：占成功就留一笔（与占用登记在同一原子动作里落盘）。
-- 之后 confirm/release 由 settle.lua 各留一笔；同一业务号状态机保证
-- 每种结局只记一次，再来只回原结论、不再留笔。
ledger_add("reserve", res_key, cost, "", false,
           {caller = client, idem = idem, pool = pool,
            reserve_at_ms = now_ms})
-- 占用单留存：带业务号的留 24h（与旧版幂等记录一致），期间同一业务号重放
-- 都能拿到原结论；匿名单子没人会按号查，留到约定回音时限加宽限即可。
-- 注意：占用的“额度效力”只到约定回音时限（ZSET score），单子本身留得再久
-- 也不会把额度占死——过期的占用登记在下次判定时被清掉，额度自动退回。
if idem ~= "" then
  redis.call("EXPIRE", res_key, 86400)
else
  redis.call("EXPIRE", res_key, lease_seconds + 300)
end

return {1, new_rem_burst, new_rem_window, lease_exp}
