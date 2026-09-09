-- consume.lua
-- 数据面每真实调用执行一次。在 Redis 单线程内原子完成：
--   1) 幂等去重（同一 Idempotency-Key 只扣一次）
--   2) 读取密钥配置（不存在 / 已停用直接拒绝；停用后所有份额立即失效）
--   3) 选择本次生效的“池”：
--        - 调用方被点名（shares 表中有其份额）：只用自己的预留池，
--          自己的用完了公共池还有也不能拿；
--        - 未被点名：只用公共池（总量 − 全部预留），吃不到别人留的份额
--   4) 短周期突发：令牌桶（允许突发 + 平滑补充）
--   5) 长周期总量：两个相邻固定桶加权的滑动窗口（避免固定窗口边界双倍突发）
--   6) 判定 + 落盘扣减同一原子动作：允许才写，拒绝不产生任何扣减
--
-- 时间一律取 Redis 的 TIME，调用方本地时钟不参与判定。
--
-- KEYS[1] = cfg    配置 hash，例如 {qk:<kid>}cfg
-- KEYS[2] = tb     公共池令牌桶 hash，例如 {qk:<kid>}tb
-- KEYS[3] = shares 份额配置 hash，例如 {qk:<kid>}shares（控制面写，此处只读）
-- KEYS[4] = dedup  幂等记录 key（可选；无幂等键时数据面只传 3 个 KEYS）
--
-- ARGV:
--  1  kid                密钥ID（仅用于拼窗口桶 key）
--  2  burst_capacity     密钥令牌桶总容量（短周期突发上限）
--  3  burst_refill_ms    每补充 1 个令牌所需毫秒（份额池沿用同一速率）
--  4  window_seconds     长周期窗口长度（秒；份额池沿用同一窗口长度）
--  5  window_quota       密钥长周期总量
--  6  cost               本次消耗（通常 1）
--  7  idem               幂等键（空串表示不做去重）
--  8  idem_ttl_seconds   幂等记录保留秒数
--  9  client             调用方名（X-Client-Id；空串表示匿名，只可能命中公共池）
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
--  * 公共池计数沿用 {qk:<kid>}tb / {qk:<kid>}win:<len>:<idx>，与未启用份额
--    时的历史计数完全连续；份额池用 {qk:<kid>}stb:<sha1(caller)> 与
--    {qk:<kid>}swin:<sha1(caller)>:<len>:<idx>，互不干扰。
--  * 所有 key 都带 {qk:<kid>} hash tag：同一密钥落在同一集群 slot。
--  * 份额配置只在控制面经 set_share.lua 原子校验后写入（Σ份额 ≤ 总量），
--    本脚本信任 shares 表与 cfg.reserved_* 的一致性，只读不改。

local cfg_key    = KEYS[1]
local tb_key     = KEYS[2]
local shares_key = KEYS[3]
local dedup_key  = KEYS[4] or ""  -- 无幂等键时数据面只传 3 个 KEYS

local kid             = ARGV[1]
local capacity        = tonumber(ARGV[2])
local refill_ms       = tonumber(ARGV[3])
local window_seconds  = tonumber(ARGV[4])
local window_quota    = tonumber(ARGV[5])
local cost            = tonumber(ARGV[6])
local idem            = ARGV[7]
local idem_ttl        = tonumber(ARGV[8])
local client          = ARGV[9] or ""

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

-- 2) 配置：控制面写入；数据面只读。
--    revoked 检查在任何份额逻辑之前：密钥一停用，所有份额与公共池立即失效。
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

-- 3) 选定本次生效的池：点名调用方用预留份额，未点名用公共池
local eff_capacity   -- 生效令牌桶容量
local eff_win_quota  -- 生效窗口总量
local eff_tb_key     -- 生效令牌桶 key
local win_prefix     -- 生效窗口计数 key 前缀（":<len>:<idx>" 之前）

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
else
  -- 未点名：公共池 = 总量 − 全部预留（reserved_* 由控制面维护，可能为 0）
  local reserved_burst  = tonumber(redis.call("HGET", cfg_key, "reserved_burst") or "0") or 0
  local reserved_window = tonumber(redis.call("HGET", cfg_key, "reserved_window") or "0") or 0
  eff_capacity  = capacity - reserved_burst
  eff_win_quota = window_quota - reserved_window
  eff_tb_key    = tb_key
  win_prefix    = "{qk:" .. kid .. "}win:"
end

-- 4) 滑动窗口（长周期总量）：当前桶 + 前一桶加权
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
  return {0, "burst_limited", retry_ms, 0, math.max(eff_win_quota - window_used, 0)}
end

-- 5) 令牌桶（短周期突发）
local tokens
if redis.call("EXISTS", eff_tb_key) == 0 then
  tokens = eff_capacity * SCALE
else
  local old_tokens = tonumber(redis.call("HGET", eff_tb_key, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", eff_tb_key, "ts"))
  if not old_tokens or not old_ts then
    tokens = eff_capacity * SCALE
  else
    local elapsed = now_us - old_ts  -- 微秒
    if elapsed < 0 then elapsed = 0 end
    -- refill_ms 是“每补充 1 个令牌的毫秒数”：elapsed 先换算成毫秒
    local elapsed_ms = elapsed / 1000
    local refilled = math.floor(elapsed_ms * SCALE / refill_ms)
    tokens = math.min(eff_capacity * SCALE, old_tokens + refilled)
  end
end

local rem_burst  = math.floor(tokens / SCALE)
local rem_window = eff_win_quota - window_used

-- 6) 判定
if tokens < need_tokens then
  -- 令牌不足：等到补充满 cost 个令牌（向上取整），并以填满整桶时间为保守上界
  local wait_ms = math.ceil((need_tokens - tokens) * refill_ms / SCALE)
  local full_ms = eff_capacity * refill_ms
  if wait_ms > full_ms then wait_ms = full_ms end
  if wait_ms < 1 then wait_ms = 1 end
  return {0, "burst_limited", wait_ms, rem_burst, math.max(rem_window, 0)}
end

if window_used + cost > eff_win_quota then
  -- 总量用尽：等到跨入下一个固定桶。届时上一桶整体滑出加权窗口，至少释放
  -- prev_used 个额度——这是不依赖扣减时间分布的、可证明的最早放行时刻，
  -- 宁可让调用方多等，也不给出偏小的 Retry-After 引导其过早重试。
  local retry_ms = win_ms - elapsed_in_bucket
  if retry_ms < 1 then retry_ms = 1 end
  return {0, "window_limited", retry_ms, rem_burst, 0}
end

-- 允许：一次性落盘全部扣减（原子，崩溃不会出现“判定了没扣”或“扣了一半”）
tokens = tokens - need_tokens
redis.call("HSET", eff_tb_key, "tokens", tokens, "ts", now_us)
-- TTL：填满整桶后桶即无信息量，留少量缓冲
local tb_ttl_ms = eff_capacity * refill_ms + 5000
redis.call("PEXPIRE", eff_tb_key, tb_ttl_ms)

redis.call("INCRBY", cur_key, cost)
redis.call("PEXPIRE", cur_key, win_ms * 2 + 5000)

local new_rem_burst  = math.floor(tokens / SCALE)
local new_rem_window = eff_win_quota - (window_used + cost)

if idem ~= "" then
  redis.call("HSET", dedup_key,
    "status", "allow",
    "rem_burst", new_rem_burst,
    "rem_window", new_rem_window)
  redis.call("EXPIRE", dedup_key, idem_ttl)
end

return {1, new_rem_burst, new_rem_window, math.floor(tokens / SCALE)}
