-- pool_quota.lua
-- 控制面只读查询跨密钥共享池的聚合计数：
--   remaining：现在还能再占多少
--   held     ：成员密钥已占、还没调成/退回的数量
--   consumed ：已经确认、沉在池聚合计数里的数量（由控制面换算）
-- 不遍历各成员密钥自己的计数器；成员明细由控制面逐把调用 quota.lua 展示。
--
-- KEYS[1] = pcfg
-- KEYS[2] = ptb
-- KEYS[3] = members
-- ARGV: pid,capacity,refill_ms,window_seconds,window_quota

local pcfg_key    = KEYS[1]
local tb_key      = KEYS[2]
local members_key = KEYS[3]
local pid            = ARGV[1]
local capacity       = tonumber(ARGV[2])
local refill_ms      = tonumber(ARGV[3])
local window_seconds = tonumber(ARGV[4])
local window_quota   = tonumber(ARGV[5])

if redis.call("EXISTS", pcfg_key) == 0 then
  return {"unknown", 0, 0, 0, 0, window_seconds, refill_ms, capacity,
          window_quota, 0}
end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]
local SCALE  = 1000
local state  = redis.call("HGET", pcfg_key, "stopped") == "1" and "stopped" or "active"

local holds_key  = "{qp:" .. pid .. "}holds"
local wholds_key = "{qp:" .. pid .. "}wholds"
local held_b = tonumber(redis.call("ZCOUNT", holds_key, "(" .. now_ms, "+inf")) or 0
local held_w = tonumber(redis.call("ZCOUNT", wholds_key, "(" .. now_ms, "+inf")) or 0

local tokens
if redis.call("EXISTS", tb_key) == 0 then
  tokens = capacity * SCALE
else
  local old_tokens = tonumber(redis.call("HGET", tb_key, "tokens"))
  local old_ts = tonumber(redis.call("HGET", tb_key, "ts"))
  if not old_tokens or not old_ts then
    tokens = capacity * SCALE
  elseif old_tokens >= capacity * SCALE then
    tokens = old_tokens
  else
    local elapsed = now_us - old_ts
    if elapsed < 0 then elapsed = 0 end
    tokens = math.min(capacity * SCALE,
      old_tokens + math.floor((elapsed / 1000) * SCALE / refill_ms))
  end
end
local rem_b = math.max(math.floor((tokens - held_b * SCALE) / SCALE), 0)

local win_ms = window_seconds * 1000
local idx = math.floor(now_ms / win_ms)
local start_ms = idx * win_ms
local prev = tonumber(redis.call("GET",
  "{qp:" .. pid .. "}win:" .. window_seconds .. ":" .. (idx - 1)) or "0")
local cur = tonumber(redis.call("GET",
  "{qp:" .. pid .. "}win:" .. window_seconds .. ":" .. idx) or "0")
local weight = (win_ms - (now_ms - start_ms)) / win_ms
local used = math.floor(cur + prev * weight)
local rem_w = math.max(window_quota - used - held_w, 0)
local n_members = redis.call("HLEN", members_key)

return {state, rem_b, rem_w, held_b, held_w, window_seconds, refill_ms,
        capacity, window_quota, n_members}
