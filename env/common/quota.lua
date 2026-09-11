-- quota.lua
-- 控制面“查看某把密钥当前窗口剩余额度”使用：只读，绝不扣减、绝不改动占用。
-- 计算口径与 reserve.lua 完全一致（令牌桶 + 滑动窗口 + 份额隔离），
-- 这样控制面展示的数字就是数据面下一次判定的依据。
--
-- 余量分两种状态，一次看齐：
--   * 还占着（held）   ：已预扣、还没回音的占用——别人现在拿不走，
--                        上游没调成或过了约定时限会退回；
--   * 真用掉（consumed）：已确认的扣减，沉在令牌桶/窗口计数里。
--   余量（remaining）= 配额 − 真用掉 − 还占着，即可再占的部分。
--
-- 一次返回三个视角，满足“同时看到总盘和各人还剩多少”：
--   * 总盘    ：整把密钥（公共池 + 各份额之和，上限即密钥总量）
--   * 公共池  ：总量 − 全部预留，未点名调用方可用
--   * 各份额  ：每个点名调用方自己的配额、余量与在占数
--
-- KEYS[1] = cfg
-- KEYS[2] = tb      公共池令牌桶
-- KEYS[3] = shares  份额配置 hash
-- ARGV 与 reserve.lua 前 5 个参数相同（kid, capacity, refill_ms, window_seconds, window_quota）
--
-- 返回（扁平数组）：
--   {state, rem_burst_total, rem_window_total, held_burst_total, held_window_total,
--    window_seconds, refill_ms, capacity, window_quota, reserved_burst, reserved_window,
--    pool_rem_burst, pool_rem_window, pool_held_burst, pool_held_window, n_shares,
--    caller1, share_capacity1, share_window_quota1, share_rem_burst1, share_rem_window1,
--    share_held_burst1, share_held_window1,
--    caller2, ...}
--   state = "active" | "revoked" | "unknown"
--   余量始终按自然口径返回（停用也照算）；是否可用由 state 表达——控制面展示时
--   对 revoked 密钥把“可再占”置 0，但“还占着 / 真用掉”保持真实，不抹账。

local cfg_key    = KEYS[1]
local tb_key     = KEYS[2]
local shares_key = KEYS[3]

local kid            = ARGV[1]
local capacity       = tonumber(ARGV[2])
local refill_ms      = tonumber(ARGV[3])
local window_seconds = tonumber(ARGV[4])
local window_quota   = tonumber(ARGV[5])

if redis.call("EXISTS", cfg_key) == 0 then
  return {"unknown", 0, 0, 0, 0, window_seconds, refill_ms, capacity, window_quota,
          0, 0, 0, 0, 0, 0, 0}
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

-- 令牌桶余量（与 reserve.lua 同一套补充算法）
local function bucket_tokens(tb, cap)
  if redis.call("EXISTS", tb) == 0 then
    return cap * SCALE
  end
  local old_tokens = tonumber(redis.call("HGET", tb, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", tb, "ts"))
  if not old_tokens or not old_ts then
    return cap * SCALE
  end
  -- 冲正退回会把令牌加回桶里并允许高于容量（溢余）。溢余不随时间补充被
  -- min(cap) 抹掉——这正是“冲回去的真用掉立刻回到可再占”，控制面只读视图
  -- 与数据面下一次判定（reserve.lua）保持同一口径。
  if old_tokens >= cap * SCALE then
    return old_tokens
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

-- 还占着的量：占用登记 ZSET 里 score（约定回音时限）尚未到期的成员数。
-- 已过期的登记视为自动退回（与 reserve.lua 判定前的清理口径一致），
-- 只读统计，不清理、不改动任何 key。
local function held(holds_key, wholds_key)
  local hb = redis.call("ZCOUNT", holds_key, "(" .. now_ms, "+inf")
  local hw = redis.call("ZCOUNT", wholds_key, "(" .. now_ms, "+inf")
  return tonumber(hb) or 0, tonumber(hw) or 0
end

-- 公共池：总量 − 全部预留，沿用密钥级计数 key。
-- 余量 = 桶内/窗口余量 − 还占着的（占着的这段别人不能拿）。
-- 注意：余量按“自然口径”计算（即使密钥已停用也照算），是否可用由 state 表达；
-- 这样“还占着 / 真用掉”在停用后依然真实，不会出现“停用就把账抹了”。
local pool_capacity   = capacity - reserved_burst
local pool_win_quota  = window_quota - reserved_window
local pool_held_burst, pool_held_window = held(
  "{qk:" .. kid .. "}holds",
  "{qk:" .. kid .. "}wholds")
local pool_rem_burst  = 0
local pool_rem_window = 0
if pool_capacity > 0 then
  pool_rem_burst = math.max(
    math.floor((bucket_tokens(tb_key, pool_capacity) - pool_held_burst * SCALE) / SCALE), 0)
end
pool_rem_window = math.max(
  window_remaining("{qk:" .. kid .. "}win:", pool_win_quota) - pool_held_window, 0)

-- 各份额明细
local result = {"active", 0, 0, 0, 0, window_seconds, refill_ms, capacity, window_quota,
                reserved_burst, reserved_window,
                pool_rem_burst, pool_rem_window, pool_held_burst, pool_held_window,
                #names}
if revoked then
  result[1] = "revoked"
end

local total_burst  = pool_rem_burst
local total_window = pool_rem_window
local total_held_burst  = pool_held_burst
local total_held_window = pool_held_window
for _, name in ipairs(names) do
  local s   = share_cfg[name]
  local cap = tonumber(s["burst_capacity"]) or 0
  local wq  = tonumber(s["window_quota"]) or 0
  local ch = redis.sha1hex(name)
  local held_b, held_w = held(
    "{qk:" .. kid .. "}sholds:" .. ch,
    "{qk:" .. kid .. "}swholds:" .. ch)
  local rem_b, rem_w = 0, 0
  if cap > 0 then
    rem_b = math.max(
      math.floor((bucket_tokens("{qk:" .. kid .. "}stb:" .. ch, cap) - held_b * SCALE) / SCALE), 0)
  end
  rem_w = math.max(window_remaining("{qk:" .. kid .. "}swin:" .. ch .. ":", wq) - held_w, 0)
  total_burst  = total_burst + rem_b
  total_window = total_window + rem_w
  total_held_burst  = total_held_burst + held_b
  total_held_window = total_held_window + held_w
  result[#result + 1] = name
  result[#result + 1] = cap
  result[#result + 1] = wq
  result[#result + 1] = rem_b
  result[#result + 1] = rem_w
  result[#result + 1] = held_b
  result[#result + 1] = held_w
end

result[2] = total_burst
result[3] = total_window
result[4] = total_held_burst
result[5] = total_held_window
return result
