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
--
-- 返回（全部为整/字符串数组）：
--   {1, remaining_burst, remaining_window, lease_expires_unix}        占成功
--   {0, reason, retry_after_ms, remaining_burst, remaining_window}    占失败
--   {2, lease_expires_unix, outcome, remaining_burst, remaining_window} 幂等重放
--       outcome: ""（占用还在）/ "confirmed" / "released"
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

local kid             = ARGV[1]
local capacity        = tonumber(ARGV[2])
local refill_ms       = tonumber(ARGV[3])
local window_seconds  = tonumber(ARGV[4])
local window_quota    = tonumber(ARGV[5])
local cost            = tonumber(ARGV[6])
local idem            = ARGV[7]
local lease_seconds   = tonumber(ARGV[8])
local client          = ARGV[9] or ""

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

-- 2) 幂等：同一业务号再来，命中同一张占用单（匿名调用单号随机，必不命中）
if idem ~= "" then
  local prev = redis.call("HGETALL", res_key)
  if #prev > 0 then
    local h = {}
    for i = 1, #prev, 2 do h[prev[i]] = prev[i + 1] end
    local outcome = h.outcome or ""
    if outcome ~= "" then
      -- 已了结：直接给原结论（只是回报历史，密钥停用后也一样），绝不二次占额
      return {2, tonumber(h.lease_exp or 0), outcome,
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
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0)}
    end
    -- 占用已超约定时限还没回音：旧占用登记已失效（额度已退回池里），
    -- 按同一业务号重新占一次——单子还是这张单子，确认仍只生效一次。
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

-- 5) 清掉已过约定回音时限的占用登记：没回音的占额自动退回，别人可再占
redis.call("ZREMRANGEBYSCORE", holds_key, "-inf", now_ms)
redis.call("ZREMRANGEBYSCORE", wholds_key, "-inf", now_ms)
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
  "rem_burst", new_rem_burst,
  "rem_window", new_rem_window,
  "outcome", "")
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
