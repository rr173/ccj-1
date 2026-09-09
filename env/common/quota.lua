-- quota.lua
-- 控制面“查看某把密钥当前窗口剩余额度”使用：只读，绝不扣减。
-- 计算口径与 consume.lua 完全一致（令牌桶 + 滑动窗口 + 份额隔离），
-- 这样控制面展示的数字就是数据面下一次判定的依据。
--
-- 一次返回三个视角，满足“同时看到总盘和各人还剩多少”：
--   * 总盘    ：整把密钥的余量（公共池 + 各份额之和，上限即密钥总量）
--   * 公共池  ：总量 − 全部预留，未点名调用方可用
--   * 各份额  ：每个点名调用方自己的配额与余量
--
-- KEYS[1] = cfg
-- KEYS[2] = tb      公共池令牌桶
-- KEYS[3] = shares  份额配置 hash
-- ARGV 与 consume.lua 前 5 个参数相同（kid, capacity, refill_ms, window_seconds, window_quota）
--
-- 返回（扁平数组）：
--   {state, rem_burst_total, rem_window_total, window_seconds, refill_ms,
--    capacity, window_quota, reserved_burst, reserved_window,
--    pool_rem_burst, pool_rem_window, n_shares,
--    caller1, share_capacity1, share_window_quota1, share_rem_burst1, share_rem_window1,
--    caller2, ...}
--   state = "active" | "revoked" | "unknown"
--   revoked 时各余量为 0（份额立即失效），但配置口径照常返回。

local cfg_key    = KEYS[1]
local tb_key     = KEYS[2]
local shares_key = KEYS[3]

local kid            = ARGV[1]
local capacity       = tonumber(ARGV[2])
local refill_ms      = tonumber(ARGV[3])
local window_seconds = tonumber(ARGV[4])
local window_quota   = tonumber(ARGV[5])

if redis.call("EXISTS", cfg_key) == 0 then
  return {"unknown", 0, 0, window_seconds, refill_ms, capacity, window_quota,
          0, 0, 0, 0, 0}
end

local reserved_burst  = tonumber(redis.call("HGET", cfg_key, "reserved_burst") or "0") or 0
local reserved_window = tonumber(redis.call("HGET", cfg_key, "reserved_window") or "0") or 0

-- 份额清单（field=调用方名, value=JSON），按名排序保证返回顺序稳定
local raw = redis.call("HGETALL", shares_key)
local names = {}
local share_cfg = {}
for i = 1, #raw, 2 do
  names[#names + 1] = raw[i]
  share_cfg[raw[i]] = cjson.decode(raw[i + 1])
end
table.sort(names)

local revoked = redis.call("HGET", cfg_key, "revoked") == "1"

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]
local SCALE  = 1000

local win_ms    = window_seconds * 1000
local cur_idx   = math.floor(now_ms / win_ms)
local cur_start = cur_idx * win_ms
local prev_weight = (win_ms - (now_ms - cur_start)) / win_ms

-- 令牌桶余量（与 consume.lua 同一套补充算法）
local function bucket_tokens(tb, cap)
  if redis.call("EXISTS", tb) == 0 then
    return cap * SCALE
  end
  local old_tokens = tonumber(redis.call("HGET", tb, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", tb, "ts"))
  if not old_tokens or not old_ts then
    return cap * SCALE
  end
  local elapsed = now_us - old_ts
  if elapsed < 0 then elapsed = 0 end
  local elapsed_ms = elapsed / 1000
  return math.min(cap * SCALE, old_tokens + math.floor(elapsed_ms * SCALE / refill_ms))
end

-- 窗口余量（当前桶 + 前一桶加权）
local function window_remaining(prefix, quota)
  local prev_key = prefix .. window_seconds .. ":" .. (cur_idx - 1)
  local cur_key  = prefix .. window_seconds .. ":" .. cur_idx
  local cur_used  = tonumber(redis.call("GET", cur_key) or "0")
  local prev_used = tonumber(redis.call("GET", prev_key) or "0")
  local used = math.floor(cur_used + prev_used * prev_weight)
  return quota - used
end

-- 公共池：总量 − 全部预留，沿用密钥级计数 key
local pool_capacity   = capacity - reserved_burst
local pool_win_quota  = window_quota - reserved_window
local pool_rem_burst  = 0
local pool_rem_window = 0
if not revoked then
  if pool_capacity > 0 then
    pool_rem_burst = math.floor(bucket_tokens(tb_key, pool_capacity) / SCALE)
  end
  pool_rem_window = math.max(window_remaining("{qk:" .. kid .. "}win:", pool_win_quota), 0)
end

-- 各份额明细
local result = {"active", 0, 0, window_seconds, refill_ms, capacity, window_quota,
                reserved_burst, reserved_window,
                pool_rem_burst, pool_rem_window, #names}
if revoked then
  result[1] = "revoked"
end

local total_burst  = pool_rem_burst
local total_window = pool_rem_window
for _, name in ipairs(names) do
  local s   = share_cfg[name]
  local cap = tonumber(s["burst_capacity"]) or 0
  local wq  = tonumber(s["window_quota"]) or 0
  local rem_b, rem_w = 0, 0
  if not revoked then
    local ch = redis.sha1hex(name)
    if cap > 0 then
      rem_b = math.floor(bucket_tokens("{qk:" .. kid .. "}stb:" .. ch, cap) / SCALE)
    end
    rem_w = math.max(window_remaining("{qk:" .. kid .. "}swin:" .. ch .. ":", wq), 0)
  end
  total_burst  = total_burst + rem_b
  total_window = total_window + rem_w
  result[#result + 1] = name
  result[#result + 1] = cap
  result[#result + 1] = wq
  result[#result + 1] = rem_b
  result[#result + 1] = rem_w
end

result[2] = total_burst
result[3] = total_window
return result
