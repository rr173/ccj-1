-- consume.lua
-- 数据面每真实调用执行一次。在 Redis 单线程内原子完成：
--   1) 幂等去重（同一 Idempotency-Key 只扣一次）
--   2) 读取密钥配置（不存在 / 已停用直接拒绝）
--   3) 短周期突发：令牌桶（允许突发 + 平滑补充）
--   4) 长周期总量：两个相邻固定桶加权的滑动窗口（避免固定窗口边界双倍突发）
--   5) 判定 + 落盘扣减同一原子动作：允许才写，拒绝不产生任何扣减
--
-- 时间一律取 Redis 的 TIME，调用方本地时钟不参与判定。
--
-- KEYS[1] = cfg   配置 hash，例如 {qk}cfg:<kid>
-- KEYS[2] = tb    令牌桶 hash，例如 {qk}tb:<kid>
-- KEYS[3] = dedup 幂等记录 key，例如 {qk}dedup:<kid>:<idem>（无幂等键时给空串占位）
--
-- ARGV:
--  1  kid                密钥ID（仅用于拼窗口桶 key）
--  2  burst_capacity     令牌桶容量（短周期突发上限）
--  3  burst_refill_ms    每补充 1 个令牌所需毫秒
--  4  window_seconds     长周期窗口长度（秒）
--  5  window_quota       长周期总量
--  6  cost               本次消耗（通常 1）
--  7  idem               幂等键（空串表示不做去重）
--  8  idem_ttl_seconds   幂等记录保留秒数
--
-- 返回（全部为整/字符串数组）：
--   {1, remaining_burst, remaining_window, tokens_left_hint}            允许
--   {0, reason, retry_after_ms, remaining_burst, remaining_window}      拒绝
--   {2, status, remaining_burst, remaining_window}                      幂等重放
--       status: "allow" / "deny:<reason>"
--   {3, error_message}                                                   参数/配置错误
--
-- 设计要点：
--  * 令牌桶以千分之一令牌为单位存整数，避免浮点漂移导致“凭空多出额度”。
--  * 窗口计数 key 为 {qk}win:<kid>:<window_len>:<bucket_index>，
--    hash tag {qk} 保证同一密钥的 key 落在同一集群 slot，脚本可安全访问。

local cfg_key   = KEYS[1]
local tb_key    = KEYS[2]
local dedup_key = KEYS[3] or ""  -- 无幂等键时数据面只传 2 个 KEYS

local kid             = ARGV[1]
local capacity        = tonumber(ARGV[2])
local refill_ms       = tonumber(ARGV[3])
local window_seconds  = tonumber(ARGV[4])
local window_quota    = tonumber(ARGV[5])
local cost            = tonumber(ARGV[6])
local idem            = ARGV[7]
local idem_ttl        = tonumber(ARGV[8])

if not capacity or not refill_ms or not window_seconds or not window_quota
   or not cost or capacity <= 0 or refill_ms <= 0
   or window_seconds <= 0 or window_quota <= 0 or cost <= 0 then
  return {3, "bad arguments"}
end

-- 1) 幂等：同一请求重放，返回第一次的判定，绝不二次扣减
if idem ~= "" then
  local prev = redis.call("HGETALL", dedup_key)
  if #prev > 0 then
    local h = {}
    for i = 1, #prev, 2 do h[prev[i]] = prev[i + 1] end
    return {2, h.status or "unknown", tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0)}
  end
end

-- 2) 配置：控制面写入；数据面只读
if redis.call("EXISTS", cfg_key) == 0 then
  return {3, "key not found"}
end
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked", 0, 0, 0}
end

-- Redis 服务器时间（秒 + 微秒），毫秒与微秒精度时间戳
local t          = redis.call("TIME")
local now_ms     = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us     = t[1] * 1000000 + t[2]

local SCALE = 1000  -- 令牌内部放大倍数

-- 3) 令牌桶（短周期突发）
local tokens
if redis.call("EXISTS", tb_key) == 0 then
  tokens = capacity * SCALE
else
  local old_tokens = tonumber(redis.call("HGET", tb_key, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", tb_key, "ts"))
  if not old_tokens or not old_ts then
    tokens = capacity * SCALE
  else
    local elapsed = now_us - old_ts  -- 微秒
    if elapsed < 0 then elapsed = 0 end
    -- refill_ms 是“每补充 1 个令牌的毫秒数”：elapsed 先换算成毫秒
    local elapsed_ms = elapsed / 1000
    local refilled = math.floor(elapsed_ms * SCALE / refill_ms)
    tokens = math.min(capacity * SCALE, old_tokens + refilled)
  end
end

-- 4) 滑动窗口（长周期总量）：当前桶 + 前一桶加权
local win_ms    = window_seconds * 1000
local cur_idx   = math.floor(now_ms / win_ms)
local cur_start = cur_idx * win_ms
local prev_key  = "{qk:" .. kid .. "}win:" .. window_seconds .. ":" .. (cur_idx - 1)
local cur_key   = "{qk:" .. kid .. "}win:" .. window_seconds .. ":" .. cur_idx

local cur_used  = tonumber(redis.call("GET", cur_key) or "0")
local prev_used = tonumber(redis.call("GET", prev_key) or "0")
local elapsed_in_bucket = now_ms - cur_start  -- 0 .. win_ms-1
local prev_weight = (win_ms - elapsed_in_bucket) / win_ms
local window_used = math.floor(cur_used + prev_used * prev_weight)

local need_tokens = cost * SCALE
local rem_burst  = math.floor(tokens / SCALE)
local rem_window = window_quota - window_used

-- 5) 判定
if tokens < need_tokens then
  -- 令牌不足：等到补充满 cost 个令牌（向上取整），并以填满整桶时间为保守上界
  local wait_ms = math.ceil((need_tokens - tokens) * refill_ms / SCALE)
  local full_ms = capacity * refill_ms
  if wait_ms > full_ms then wait_ms = full_ms end
  if wait_ms < 1 then wait_ms = 1 end
  return {0, "burst_limited", wait_ms, rem_burst, math.max(rem_window, 0)}
end

if window_used + cost > window_quota then
  -- 总量用尽：等到跨入下一个固定桶。届时上一桶整体滑出加权窗口，至少释放
  -- prev_used 个额度——这是不依赖扣减时间分布的、可证明的最早放行时刻，
  -- 宁可让调用方多等，也不给出偏小的 Retry-After 引导其过早重试。
  local retry_ms = win_ms - elapsed_in_bucket
  if retry_ms < 1 then retry_ms = 1 end
  return {0, "window_limited", retry_ms, rem_burst, 0}
end

-- 允许：一次性落盘全部扣减（原子，崩溃不会出现“判定了没扣”或“扣了一半”）
tokens = tokens - need_tokens
redis.call("HSET", tb_key, "tokens", tokens, "ts", now_us)
-- TTL：填满整桶后桶即无信息量，留少量缓冲
local tb_ttl_ms = capacity * refill_ms + 5000
redis.call("PEXPIRE", tb_key, tb_ttl_ms)

redis.call("INCRBY", cur_key, cost)
redis.call("PEXPIRE", cur_key, win_ms * 2 + 5000)

local new_rem_burst  = math.floor(tokens / SCALE)
local new_rem_window = window_quota - (window_used + cost)

if idem ~= "" then
  redis.call("HSET", dedup_key,
    "status", "allow",
    "rem_burst", new_rem_burst,
    "rem_window", new_rem_window)
  redis.call("EXPIRE", dedup_key, idem_ttl)
end

return {1, new_rem_burst, new_rem_window, math.floor(tokens / SCALE)}
