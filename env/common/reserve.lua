-- reserve.lua
-- 数据面每真实调用执行一次“先占额度”。在 Redis 单线程内原子完成：
--   1) 幂等去重：同一 Idempotency-Key（业务号）已有占用单 → 返回同一张单子，
--      绝不二次占额；单子上记着上游结论，数据面据此补确认或补退回
--   2) 读取密钥配置（不存在 / 已停用直接拒绝；停用后所有份额、通行证立即失效）
--   3) 选择本次生效的“池”：
--        - 持通行证（pid 非空）：只占这张通行证自己那份。通行证必须还没到
--          作废时刻、没被吊销；否则一律 pass_expired 拒绝（密钥停了同样拒绝）；
--        - 调用方被点名（shares 表中有其份额）：只占用自己的预留池，
--          自己的占满了公共池还有也不能拿；
--        - 未被点名：只占用公共池（总量 − 全部份额 − 全部有效通行证），
--          吃不到别人留的份额、也拿不到通行证切走的那段
--   4) 短周期突发：令牌桶（允许突发 + 平滑补充）
--   5) 长周期总量：两个相邻固定桶加权的滑动窗口（避免固定窗口边界双倍突发）
--   6) 判定 + 登记占用同一原子动作：占成才登记，占不住不产生任何占用
--
-- 通行证作废（到点/被吊销/密钥停用）后的额度去向：
--   通行证一失效就不再为它扣容量——它没占完的额度立刻随容量恢复回到密钥
--   公共池（窗口立即恢复，突发令牌按补充速率回补，与删除份额同口径）；
--   但它在作废时刻还占着（在飞）的那几笔不能凭空消失，否则公共池会在
--   约定回音时限内超发。因此“已作废通行证的在占登记”在判定前并入公共池
--   占用计数，继续替密钥把这段挡住，等它们回音确认（确认被拒）或超时
--   自动退回后才真正放出来。
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
-- KEYS[1] = cfg    密钥配置 hash
-- KEYS[2] = tb     公共池令牌桶 hash
-- KEYS[3] = shares 份额配置 hash（控制面写，此处只读）
-- KEYS[4] = res    占用单 hash（密钥 {qk:<kid>}res:* / 通行证 {qk:<kid>}pres:*）
-- KEYS[5] = passes 通行证配置 hash（field=pass_id, value=JSON，控制面写，只读）
--
-- ARGV:
--  1  kid
--  2  burst_capacity     密钥令牌桶总容量
--  3  burst_refill_ms    每补充 1 个令牌所需毫秒（份额池/通行证沿用同一速率）
--  4  window_seconds     长周期窗口长度（秒；份额池/通行证沿用同一窗口长度）
--  5  window_quota       密钥长周期总量
--  6  cost               本次占用（通常 1）
--  7  idem               幂等键 / 业务号（空串表示匿名调用，不做去重）
--  8  lease_seconds      约定回音时限（秒）
--  9  client             调用方名（X-Client-Id；空串表示匿名）
-- 10  pid                通行证ID（空串表示用密钥本体调用）
--
-- 返回（全部为整/字符串数组）：
--   {1, remaining_burst, remaining_window, lease_expires_unix}        占成功
--   {0, reason, retry_after_ms, remaining_burst, remaining_window}    占失败
--       reason: burst_limited / window_limited / revoked / pass_expired
--   {2, lease_expires_unix, outcome, remaining_burst, remaining_window} 幂等重放
--       outcome: ""（占用还在）/ "confirmed" / "released"
--   {3, error_message}                                                参数/配置错误

local cfg_key    = KEYS[1]
local tb_key     = KEYS[2]
local shares_key = KEYS[3]
local res_key    = KEYS[4]
local passes_key = KEYS[5]

local kid             = ARGV[1]
local capacity        = tonumber(ARGV[2])
local refill_ms       = tonumber(ARGV[3])
local window_seconds  = tonumber(ARGV[4])
local window_quota    = tonumber(ARGV[5])
local cost            = tonumber(ARGV[6])
local idem            = ARGV[7]
local lease_seconds   = tonumber(ARGV[8])
local client          = ARGV[9] or ""
local pid             = ARGV[10] or ""

if not capacity or not refill_ms or not window_seconds or not window_quota
   or not cost or not lease_seconds or capacity <= 0 or refill_ms <= 0
   or window_seconds <= 0 or window_quota <= 0 or cost <= 0 or lease_seconds <= 0 then
  return {3, "bad arguments"}
end

-- Redis 服务器时间（秒 + 微秒），毫秒与微秒精度时间戳
local t          = redis.call("TIME")
local now_ms     = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us     = t[1] * 1000000 + t[2]

-- 1) 配置存在性
if redis.call("EXISTS", cfg_key) == 0 then
  return {3, "key not found"}
end

local function key_revoked()
  return redis.call("HGET", cfg_key, "revoked") == "1"
end

-- 读取通行证配置：返回 (state, decoded)
--   state = "ok" / "missing" / "expired"（到点或已吊销）
local function load_pass()
  if pid == "" then return "missing", nil end
  local raw = redis.call("HGET", passes_key, pid)
  if not raw then return "missing", nil end
  local ok, s = pcall(cjson.decode, raw)
  if not ok or type(s) ~= "table" then return "missing", nil end
  if s["r"] == "1" or (tonumber(s["e"]) or 0) <= now_ms then
    return "expired", s
  end
  return "ok", s
end

-- 2) 幂等：同一业务号再来，命中同一张占用单
-- （密钥与通行证占用单分命名空间，通行证还把 pid 编进 key，绝不互相串单）
if idem ~= "" then
  local prev = redis.call("HGETALL", res_key)
  if #prev > 0 then
    local h = {}
    for i = 1, #prev, 2 do h[prev[i]] = prev[i + 1] end
    local outcome = h.outcome or ""
    if outcome ~= "" then
      return {2, tonumber(h.lease_exp or 0), outcome,
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0)}
    end
    if tonumber(h.lease_exp_ms or "0") > now_ms then
      -- 占用还在约定回音时限内：密钥停用 / 通行证作废的单子不能再递出去调上游
      if key_revoked() then
        return {0, "revoked", 0, 0, 0}
      end
      if pid ~= "" then
        local pstate = load_pass()
        if pstate ~= "ok" then
          return {0, "pass_expired", 0, 0, 0}
        end
      end
      return {2, tonumber(h.lease_exp or 0), "",
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0)}
    end
    -- 占用已超约定时限：旧登记已失效，按同一业务号重新占一次
  end
end

-- 3) revoked 检查在任何池逻辑之前：密钥一停用，份额/通行证/公共池全部立即失效
if key_revoked() then
  return {0, "revoked", 0, 0, 0}
end

local SCALE = 1000  -- 令牌内部放大倍数

-- 4) 选定本次生效的池
local eff_capacity   -- 生效令牌桶容量
local eff_win_quota  -- 生效窗口总量
local eff_tb_key     -- 生效令牌桶 key（确认时才真正扣它）
local win_prefix     -- 生效窗口计数 key 前缀
local holds_key      -- 生效池突发占用登记（ZSET）
local wholds_key     -- 生效池窗口占用登记（ZSET）
local pool           -- "shared" / "share" / "pass"

-- 通行证表只在密钥模式下需要遍历（计算有效合计 + 收集作废证的在飞占用）
local pass_records = {}
local function read_passes()
  local raw = redis.call("HGETALL", passes_key)
  for i = 1, #raw, 2 do
    local ok, s = pcall(cjson.decode, raw[i + 1])
    if ok and type(s) == "table" then
      pass_records[#pass_records + 1] = {id = raw[i], s = s}
    end
  end
end

if pid ~= "" then
  -- 持通行证：只能用这张证自己那份；到点/吊销立刻不能再占
  local pstate, ps = load_pass()
  if pstate == "missing" then
    return {3, "pass not found"}
  end
  if pstate == "expired" then
    return {0, "pass_expired", 0, 0, 0}
  end
  eff_capacity  = tonumber(ps["b"]) or 0
  eff_win_quota = tonumber(ps["w"]) or 0
  eff_tb_key = "{qk:" .. kid .. "}ptb:" .. pid
  win_prefix = "{qk:" .. kid .. "}pwin:" .. pid .. ":"
  holds_key  = "{qk:" .. kid .. "}pholds:" .. pid
  wholds_key = "{qk:" .. kid .. "}pwholds:" .. pid
  pool = "pass"
else
  local share_json = ""
  if client ~= "" then
    share_json = redis.call("HGET", shares_key, client) or ""
  end

  if share_json ~= "" then
    -- 点名调用方：只能用自己那份
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
    -- 公共池 = 总量 − 全部份额 − 全部【有效】通行证。
    -- 有效通行证合计实时遍历通行证表得出（不依赖 cfg.reserved_*：
    -- 通行证到点即失效，没必要等到控制面回写字段）。
    local reserved_burst  = tonumber(redis.call("HGET", cfg_key, "reserved_burst") or "0") or 0
    local reserved_window = tonumber(redis.call("HGET", cfg_key, "reserved_window") or "0") or 0
    local share_b, share_w = 0, 0
    do
      local all = redis.call("HGETALL", shares_key)
      for i = 1, #all, 2 do
        local ok, s = pcall(cjson.decode, all[i + 1])
        if ok and type(s) == "table" then
          share_b = share_b + (tonumber(s["burst_capacity"]) or 0)
          share_w = share_w + (tonumber(s["window_quota"]) or 0)
        end
      end
    end
    read_passes()
    local live_pass_b, live_pass_w = 0, 0
    for _, p in ipairs(pass_records) do
      local s = p.s
      if s["r"] ~= "1" and (tonumber(s["e"]) or 0) > now_ms then
        live_pass_b = live_pass_b + (tonumber(s["b"]) or 0)
        live_pass_w = live_pass_w + (tonumber(s["w"]) or 0)
      end
    end
    eff_capacity  = capacity - share_b - live_pass_b
    eff_win_quota = window_quota - share_w - live_pass_w
    eff_tb_key    = tb_key
    win_prefix    = "{qk:" .. kid .. "}win:"
    holds_key     = "{qk:" .. kid .. "}holds"
    wholds_key    = "{qk:" .. kid .. "}wholds"
    pool = "shared"
  end
end

-- 5) 清掉本池已过约定回音时限的占用登记
redis.call("ZREMRANGEBYSCORE", holds_key, "-inf", now_ms)
redis.call("ZREMRANGEBYSCORE", wholds_key, "-inf", now_ms)

-- 公共池还要替“已作废通行证的在飞占用”挡额度：把这些证自己的占用登记
-- （同样先按回音时限清过期）并入公共池的两个 ZSET 再统计。
-- 只并入作废证：有效证的占用已经通过“有效通行证合计”扣过容量，不能重复。
if pool == "shared" then
  if #pass_records == 0 then read_passes() end
  -- 把“尚未并入过公共池”的在占成员并进去（成员名带各自占用单号，天然不与
  -- 公共池自身成员重名）；已并入过的用 ZSCORE 跳过，避免重复刷新记分。
  local function merge(src, dst)
    local entries = redis.call("ZRANGE", src, 0, -1, "WITHSCORES")
    local argv = nil
    for i = 1, #entries, 2 do
      if redis.call("ZSCORE", dst, entries[i]) == false then
        argv = argv or {}
        argv[#argv + 1] = entries[i + 1]
        argv[#argv + 1] = entries[i]
      end
    end
    if argv then redis.call("ZADD", dst, unpack(argv)) end
  end
  for _, p in ipairs(pass_records) do
    local s = p.s
    if s["r"] == "1" or (tonumber(s["e"]) or 0) <= now_ms then
      local ph  = "{qk:" .. kid .. "}pholds:" .. p.id
      local pwh = "{qk:" .. kid .. "}pwholds:" .. p.id
      redis.call("ZREMRANGEBYSCORE", ph, "-inf", now_ms)
      redis.call("ZREMRANGEBYSCORE", pwh, "-inf", now_ms)
      merge(ph, holds_key)
      merge(pwh, wholds_key)
    end
  end
end

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
local elapsed_in_bucket = now_ms - cur_start
local prev_weight = (win_ms - elapsed_in_bucket) / win_ms
local window_used = math.floor(cur_used + prev_used * prev_weight)

local need_tokens = cost * SCALE

-- 生效池突发容量为 0：本窗口内等令牌没有意义，让调用方等到窗口边界再试
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
    local elapsed = now_us - old_ts
    if elapsed < 0 then elapsed = 0 end
    local elapsed_ms = elapsed / 1000
    local refilled = math.floor(elapsed_ms * SCALE / refill_ms)
    -- 容量被新切出的份额/通行证调小时，min 即把桶读到新容量，无需回写计数
    tokens = math.min(eff_capacity * SCALE, old_tokens + refilled)
  end
end

-- 可占余量 = 桶内令牌 − 已确认未补充的 − 还在占用中的（占着的别人不能拿）
local avail_tokens  = tokens - held_burst * SCALE
local rem_burst     = math.max(math.floor(avail_tokens / SCALE), 0)
local rem_window    = math.max(eff_win_quota - window_used - held_window, 0)

-- 8) 判定：占不住时告诉调用方还要等多久
if avail_tokens < need_tokens then
  local wait_ms = math.ceil((need_tokens - avail_tokens) * refill_ms / SCALE)
  local full_ms = eff_capacity * refill_ms
  if wait_ms > full_ms then wait_ms = full_ms end
  if wait_ms < 1 then wait_ms = 1 end
  return {0, "burst_limited", wait_ms, rem_burst, rem_window}
end

if window_used + held_window + cost > eff_win_quota then
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
local holds_ttl_ms = lease_seconds * 1000 + 5000
redis.call("PEXPIRE", holds_key, holds_ttl_ms)
redis.call("PEXPIRE", wholds_key, holds_ttl_ms)

local new_rem_burst  = math.max(math.floor((avail_tokens - need_tokens) / SCALE), 0)
local new_rem_window = math.max(eff_win_quota - window_used - held_window - cost, 0)

redis.call("HSET", res_key,
  "kid", kid,
  "pool", pool,
  "pid", pid,
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
-- 占用单留存：带业务号的留 24h，匿名留到约定回音时限加宽限。
if idem ~= "" then
  redis.call("EXPIRE", res_key, 86400)
else
  redis.call("EXPIRE", res_key, lease_seconds + 300)
end

return {1, new_rem_burst, new_rem_window, lease_exp}
