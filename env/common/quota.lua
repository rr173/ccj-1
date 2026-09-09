-- quota.lua
-- 控制面“查看某把密钥当前窗口剩余额度”使用：只读，绝不扣减。
-- 计算口径与 consume.lua 完全一致（令牌桶 + 滑动窗口），
-- 这样控制面展示的数字就是数据面下一次判定的依据。
--
-- KEYS[1] = cfg
-- KEYS[2] = tb
-- ARGV 与 consume.lua 前 6 个参数相同（kid, capacity, refill_ms, window_seconds, window_quota, cost）
--
-- 返回：
--   {state, rem_burst, rem_window, window_seconds, refill_ms, capacity, window_quota}
--   state = "active" | "revoked" | "unknown"

local cfg_key = KEYS[1]
local tb_key  = KEYS[2]

local kid            = ARGV[1]
local capacity       = tonumber(ARGV[2])
local refill_ms      = tonumber(ARGV[3])
local window_seconds = tonumber(ARGV[4])
local window_quota   = tonumber(ARGV[5])

if redis.call("EXISTS", cfg_key) == 0 then
  return {"unknown", 0, 0, window_seconds, refill_ms, capacity, window_quota}
end
if redis.call("HGET", cfg_key, "revoked") == "1" then
  -- 停用后仍要能查余量：返回真实的配置参数（尤其 refill_ms 不能给 0，
  -- 否则控制面换算每秒补充速率时会除零），余量按 0 展示。
  return {"revoked", 0, 0, window_seconds, refill_ms, capacity, window_quota}
end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]
local SCALE  = 1000

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
    local elapsed_ms = elapsed / 1000
    tokens = math.min(capacity * SCALE, old_tokens + math.floor(elapsed_ms * SCALE / refill_ms))
  end
end

local win_ms    = window_seconds * 1000
local cur_idx   = math.floor(now_ms / win_ms)
local cur_start = cur_idx * win_ms
local prev_key  = "{qk:" .. kid .. "}win:" .. window_seconds .. ":" .. (cur_idx - 1)
local cur_key   = "{qk:" .. kid .. "}win:" .. window_seconds .. ":" .. cur_idx

local cur_used  = tonumber(redis.call("GET", cur_key) or "0")
local prev_used = tonumber(redis.call("GET", prev_key) or "0")
local prev_weight = (win_ms - (now_ms - cur_start)) / win_ms
local window_used = math.floor(cur_used + prev_used * prev_weight)

return {
  "active",
  math.floor(tokens / SCALE),
  window_quota - window_used,
  window_seconds,
  refill_ms,
  capacity,
  window_quota
}
