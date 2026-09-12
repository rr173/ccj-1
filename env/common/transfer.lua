-- transfer.lua
-- 控制面在两把【都还有效】的密钥之间划转“此刻还能再占”的突发/窗口额度。
-- 整个校验、两边计数变更、幂等落账在 Redis 单线程的一段脚本内原子完成：
-- 成功就是两边同时成功；任一头不满足，整笔不动，不存在只动一头。
--
-- 划转的不是配额容量，也不是历史账：
--   * 已经真用掉的不能划：它们沉在令牌桶扣减/窗口计数里，本脚本只计算剩余；
--   * 还占着的不能划，也不会被放掉：active holds 仍由原占用单按原池结完；
--   * 划的只是“现在还能再占”的那一截：突发从源令牌桶预支走，窗口给源当前
--     固定桶记一笔用量；目标侧对称地变成突发令牌/窗口负计数（一笔可再占额度）。
--   * 不能超过目标此刻自己还装得下的空位：空位=原配额减去当前可再占，包含
--     已真用掉和已在飞的部分；划转的新余量可在旧在飞单确认后使用，不释放旧单。
-- 旧占用之后 confirm/release 仍走 settle.lua 的原路径；本脚本不改占用单。
--
-- 公共池与每个点名份额池都按各自独立桶/窗口参与总量划转。脚本按“公共池优先、
-- 份额按调用方名排序”从源的空位扣，并按同一顺序填目标空位；因此每把密钥的
-- 总可占余量立即变化；目标旧在飞单不释放，新余量可在其确认后使用。
--
-- KEYS:
--  1 record        全局划转单 hash，例如 {qt:<transfer_id>}tr
--  2 transfers_idx 全部划转单时间索引 {qt:0}transfers
--  3 src_cfg       源密钥配置
--  4 src_tb        源公共池令牌桶
--  5 src_holds     源公共池突发占用 ZSET
--  6 src_wholds    源公共池窗口占用 ZSET
--  7 dst_cfg       目标密钥配置
--  8 dst_tb        目标公共池令牌桶
--  9 dst_holds     目标公共池突发占用 ZSET
-- 10 dst_wholds    目标公共池窗口占用 ZSET
-- 11 src_shares    源点名份额配置 hash
-- 12 dst_shares    目标点名份额配置 hash
-- 13 src_transfers 源划出/划入累计 hash
-- 14 dst_transfers 目标划出/划入累计 hash
--
-- ARGV:
--  1 transfer_id, 2 source_kid, 3 target_kid,
--  4 burst_amount, 5 window_amount
--
-- 返回：
--  {1, transfer_id, src, dst, burst, window, created_ms,
--     src_remaining_burst, src_remaining_window,
--     dst_remaining_burst, dst_remaining_window} 成功
--  {2, transfer_id, src, dst, burst, window, created_ms} 同号幂等重放
--  {0, reason, ...} 拒绝
--
-- 管理脚本会跨密钥 hash tag 动态访问窗口桶与双边索引；这与共享池/主备能力
-- 一样，适用于单机或主从 Redis。Cluster 需要外部事务协调或同 slot 数据模型。

local record_key    = KEYS[1]
local transfers_idx = KEYS[2]
local src_cfg       = KEYS[3]
local src_tb        = KEYS[4]
local src_holds     = KEYS[5]
local src_wholds    = KEYS[6]
local dst_cfg       = KEYS[7]
local dst_tb        = KEYS[8]
local dst_holds     = KEYS[9]
local dst_wholds    = KEYS[10]
local src_shares    = KEYS[11]
local dst_shares    = KEYS[12]
local src_transfers = KEYS[13]
local dst_transfers = KEYS[14]

local transfer_id = ARGV[1]
local src_kid     = ARGV[2]
local dst_kid     = ARGV[3]
local burst_amt   = tonumber(ARGV[4])
local window_amt  = tonumber(ARGV[5])

if not transfer_id or transfer_id == "" then
  return {0, "transfer_id_required"}
end
if not burst_amt or not window_amt
   or burst_amt < 0 or window_amt < 0
   or burst_amt ~= math.floor(burst_amt)
   or window_amt ~= math.floor(window_amt)
   or (burst_amt == 0 and window_amt == 0) then
  return {0, "bad_amount"}
end
if src_kid == dst_kid then
  return {0, "same_key"}
end

-- 幂等：同一划转号只能有一笔。参数不同是明确的调用错误，不能借同号改账。
local existing = redis.call("HGETALL", record_key)
if #existing > 0 then
  local old = {}
  for i = 1, #existing, 2 do old[existing[i]] = existing[i + 1] end
  if old["source_key_id"] ~= src_kid or old["target_key_id"] ~= dst_kid
     or tonumber(old["burst_amount"] or "0") ~= burst_amt
     or tonumber(old["window_amount"] or "0") ~= window_amt then
    return {0, "transfer_id_conflict",
            old["source_key_id"] or "", old["target_key_id"] or "",
            old["burst_amount"] or "0", old["window_amount"] or "0"}
  end
  return {2, old["transfer_id"], old["source_key_id"], old["target_key_id"],
          old["burst_amount"], old["window_amount"], old["created_at_ms"]}
end

if redis.call("EXISTS", src_cfg) == 0 then
  return {0, "source_not_found"}
end
if redis.call("EXISTS", dst_cfg) == 0 then
  return {0, "target_not_found"}
end
if redis.call("HGET", src_cfg, "revoked") == "1" then
  return {0, "source_revoked"}
end
if redis.call("HGET", dst_cfg, "revoked") == "1" then
  return {0, "target_revoked"}
end

local SCALE = 1000
local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]

local function cfg_num(cfg, field)
  return tonumber(redis.call("HGET", cfg, field) or "0") or 0
end

-- 构造一把密钥的公共池 + 点名份额池。顺序稳定：公共池第一，份额按名排序。
local function build_pools(cfg, kid, common_tb, common_holds, common_wholds, shares_key)
  local total_cap = cfg_num(cfg, "burst_capacity")
  local total_win = cfg_num(cfg, "window_quota")
  local refill = cfg_num(cfg, "burst_refill_ms")
  local win_seconds = cfg_num(cfg, "window_seconds")
  local reserved_burst = cfg_num(cfg, "reserved_burst")
  local reserved_win = cfg_num(cfg, "reserved_window")
  local pools = {
    {
      name = "",
      scope = "shared",
      cap = total_cap - reserved_burst,
      win_quota = total_win - reserved_win,
      refill = refill,
      win_seconds = win_seconds,
      tb = common_tb,
      win_prefix = "{qk:" .. kid .. "}win:",
      holds = common_holds,
      wholds = common_wholds,
    }
  }
  local flat = redis.call("HGETALL", shares_key)
  local names = {}
  local values = {}
  for i = 1, #flat, 2 do
    names[#names + 1] = flat[i]
    values[flat[i]] = flat[i + 1]
  end
  table.sort(names)
  for _, name in ipairs(names) do
    local ok, share = pcall(cjson.decode, values[name])
    if ok and type(share) == "table" then
      local ch = redis.sha1hex(name)
      pools[#pools + 1] = {
        name = name,
        scope = "share",
        cap = tonumber(share["burst_capacity"]) or 0,
        win_quota = tonumber(share["window_quota"]) or 0,
        refill = refill,
        win_seconds = win_seconds,
        tb = "{qk:" .. kid .. "}stb:" .. ch,
        win_prefix = "{qk:" .. kid .. "}swin:" .. ch .. ":",
        holds = "{qk:" .. kid .. "}sholds:" .. ch,
        wholds = "{qk:" .. kid .. "}swholds:" .. ch,
      }
    end
  end
  return pools
end

-- 只数还没到回音时限的占用。已过期登记按 reserve/quota 的同一口径视为已退回；
-- 这里不抢着终结，只不能把它们当作可划额度，sweep/reserve 之后仍会补 timeout 账。
local function active_count(zkey)
  return tonumber(redis.call("ZCOUNT", zkey, "(" .. now_ms, "+inf")) or 0
end

-- 令牌桶按各自补充速率推进到当前时刻，但不改变 key；返回推进后的整数千分令牌。
local function current_tokens(pool)
  local cap, refill = pool.cap, pool.refill
  if cap <= 0 or refill <= 0 then return 0 end
  if redis.call("EXISTS", pool.tb) == 0 then
    return cap * SCALE
  end
  local old_tokens = tonumber(redis.call("HGET", pool.tb, "tokens"))
  local old_ts = tonumber(redis.call("HGET", pool.tb, "ts"))
  if not old_tokens or not old_ts then
    return cap * SCALE
  end
  if old_tokens >= cap * SCALE then
    -- 溢余（例如冲正退回）保留，时间补充不抹掉；可划余额在下面按 held 折算。
    return old_tokens
  end
  local elapsed = now_us - old_ts
  if elapsed < 0 then elapsed = 0 end
  local refilled = math.floor((elapsed / 1000) * SCALE / refill)
  return math.min(cap * SCALE, old_tokens + refilled)
end

local function current_window(pool)
  local win_seconds = pool.win_seconds
  if win_seconds <= 0 then return 0, "", 0 end
  local win_ms = win_seconds * 1000
  local idx = math.floor(now_ms / win_ms)
  local start_ms = idx * win_ms
  local cur_key = pool.win_prefix .. win_seconds .. ":" .. idx
  local prev_key = pool.win_prefix .. win_seconds .. ":" .. (idx - 1)
  local cur_used = tonumber(redis.call("GET", cur_key) or "0") or 0
  local prev_used = tonumber(redis.call("GET", prev_key) or "0") or 0
  local weight = (win_ms - (now_ms - start_ms)) / win_ms
  local used = math.floor(cur_used + prev_used * weight)
  return used, cur_key, win_ms
end

local src_pools = build_pools(src_cfg, src_kid, src_tb, src_holds, src_wholds, src_shares)
local dst_pools = build_pools(dst_cfg, dst_kid, dst_tb, dst_holds, dst_wholds, dst_shares)
local src_held_total_burst, src_held_total_window = 0, 0
local dst_held_total_burst, dst_held_total_window = 0, 0

local function snapshot(pools, is_source)
  local total_burst_avail, total_window_avail = 0, 0
  local total_burst_room, total_window_room = 0, 0
  for _, p in ipairs(pools) do
    p.held_burst = active_count(p.holds)
    p.held_window = active_count(p.wholds)
    p.tokens = current_tokens(p)
    p.window_used, p.cur_win, p.win_ms = current_window(p)
    if p.cap > 0 then
      p.burst_avail = math.max(
        math.floor((p.tokens - p.held_burst * SCALE) / SCALE), 0)
      -- 目标桶初始是满的：可接收的“空位”是已真用掉/已在占导致现在装不进
      -- 新请求的那部分（容量 - 当前可再占），不是当前可再占。冲正溢余不会
      -- 产生空位。划入令牌可与旧在飞单后续确认相抵，但不会释放/改写旧单。
      p.burst_room = p.cap - p.burst_avail
    else
      p.burst_avail = 0
      p.burst_room = 0
    end
    if p.win_quota > 0 then
      p.window_avail = math.max(p.win_quota - p.window_used - p.held_window, 0)
      -- 同突发：空位是当前不可再占的容量（真用掉 + 在飞），不把溢余负计数当空位。
      p.window_room = p.win_quota - p.window_avail
    else
      p.window_avail = 0
      p.window_room = 0
    end
    total_burst_avail = total_burst_avail + p.burst_avail
    total_window_avail = total_window_avail + p.window_avail
    total_burst_room = total_burst_room + p.burst_room
    total_window_room = total_window_room + p.window_room
    if is_source then
      src_held_total_burst = src_held_total_burst + p.held_burst
      src_held_total_window = src_held_total_window + p.held_window
    else
      dst_held_total_burst = dst_held_total_burst + p.held_burst
      dst_held_total_window = dst_held_total_window + p.held_window
    end
  end
  return total_burst_avail, total_window_avail, total_burst_room, total_window_room
end

local src_burst_avail, src_window_avail, src_burst_room, src_window_room = snapshot(src_pools, true)
local dst_burst_avail, dst_window_avail, dst_burst_room, dst_window_room = snapshot(dst_pools, false)

if burst_amt > src_burst_avail then
  return {0, "insufficient_burst", "source", burst_amt, src_burst_avail,
          dst_burst_room, src_held_total_burst}
end
if burst_amt > dst_burst_room then
  return {0, "insufficient_burst", "target", burst_amt, src_burst_avail,
          dst_burst_room, dst_held_total_burst}
end
if window_amt > src_window_avail then
  return {0, "insufficient_window", "source", window_amt, src_window_avail,
          dst_window_room, src_held_total_window}
end
if window_amt > dst_window_room then
  return {0, "insufficient_window", "target", window_amt, src_window_avail,
          dst_window_room, dst_held_total_window}
end

-- 按池顺序分配要扣/要填的量。所有总量校验已在前面完成；每个池再按当时算出的
-- 空位夹紧一次，份额配置若在本脚本读快照后异常变化也不会越界。
local function allocate(pools, field, amount)
  local left = amount
  for _, p in ipairs(pools) do
    if left > 0 then
      local n = math.min(p[field], left)
      p[field .. "_take"] = n
      left = left - n
    else
      p[field .. "_take"] = 0
    end
  end
  return left
end
local left_burst_src = allocate(src_pools, "burst_avail", burst_amt)
local left_window_src = allocate(src_pools, "window_avail", window_amt)
local left_burst_dst = allocate(dst_pools, "burst_room", burst_amt)
local left_window_dst = allocate(dst_pools, "window_room", window_amt)
if left_burst_src ~= 0 or left_window_src ~= 0
   or left_burst_dst ~= 0 or left_window_dst ~= 0 then
  -- 正常不会走到：总量与各池快照在同一 Redis 脚本原子视图内。宁可整笔拒绝，
  -- 也不留下半笔。
  return {0, "allocation_changed"}
end

local function apply_burst(pool, take, sign)
  if take <= 0 then return end
  -- sign=-1：源预支可再占令牌；sign=+1：目标补入空位。时间戳统一推进到 Redis
  -- TIME，之后两边自然补充仍按各自速率；已在飞 held 不在此处理。
  local after_tokens = pool.tokens + sign * take * SCALE
  redis.call("HSET", pool.tb, "tokens", after_tokens, "ts", now_us)
  local ttl = math.ceil(
    (pool.cap * SCALE - after_tokens) * pool.refill / SCALE) + 5000
  if ttl < 5000 then ttl = 5000 end
  -- 边界上若目标已有冲正溢余，划入后可能高于容量；溢余不能靠 TTL 消失。
  if after_tokens > pool.cap * SCALE then
    ttl = math.max(ttl, 86400000)
  end
  redis.call("PEXPIRE", pool.tb, ttl)
end

local function apply_window(pool, take, sign)
  if take <= 0 then return end
  -- 源当前固定桶立即“用掉”可划量；目标当前桶立即记负用量。它们随各自滑动
  -- 窗口自然老化；旧 held 确认时仍由 settle 往同一批桶里记账。
  redis.call("INCRBY", pool.cur_win, sign * take)
  redis.call("PEXPIRE", pool.cur_win, pool.win_ms * 2 + 5000)
end

for _, p in ipairs(src_pools) do
  apply_burst(p, p.burst_avail_take, -1)
  apply_window(p, p.window_avail_take, 1)
end
for _, p in ipairs(dst_pools) do
  apply_burst(p, p.burst_room_take, 1)
  apply_window(p, p.window_room_take, -1)
end

-- 幂等单 + 双边索引 + 双向累计，全部与上面的计数改动同原子落盘。
redis.call("HSET", record_key,
  "transfer_id", transfer_id,
  "source_key_id", src_kid,
  "target_key_id", dst_kid,
  "burst_amount", tostring(burst_amt),
  "window_amount", tostring(window_amt),
  "created_at_ms", tostring(now_ms),
  "state", "completed")
redis.call("ZADD", transfers_idx, now_ms, transfer_id)
redis.call("ZADD", "{qk:" .. src_kid .. "}trout", now_ms, transfer_id)
redis.call("ZADD", "{qk:" .. dst_kid .. "}trin", now_ms, transfer_id)
redis.call("HINCRBY", src_transfers, "out_burst", burst_amt)
redis.call("HINCRBY", src_transfers, "out_window", window_amt)
redis.call("HINCRBY", dst_transfers, "in_burst", burst_amt)
redis.call("HINCRBY", dst_transfers, "in_window", window_amt)

return {1, transfer_id, src_kid, dst_kid,
        tostring(burst_amt), tostring(window_amt), tostring(now_ms),
        tostring(src_burst_avail - burst_amt),
        tostring(src_window_avail - window_amt),
        tostring(dst_burst_avail + burst_amt),
        tostring(dst_window_avail + window_amt)}
