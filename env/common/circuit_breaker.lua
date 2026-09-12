-- circuit_breaker.lua
-- 按调用方熔断的管理动作：配置阈值 / 手动提前解开。运行态只读查询由控制面
-- 直接组装；实际失败、试探与冷静状态由 reserve.lua / settle.lua / sweep.lua
-- 在数据面调用路径里原子推进。
--
-- KEYS[1] = cfg
-- KEYS[2] = policy
-- 管理某调用方时额外传：
-- KEYS[3] = state（可选）
--
-- ARGV:
--   action = "configure" / "reset"
-- configure: threshold, cooldown_ms, enabled
-- reset:     caller_hash
-- 新代际在脚本内按当前 gen + 1 原子生成，避免并发手动解开与旧试探互相覆盖。
local cfg_key    = KEYS[1]
local policy_key = KEYS[2]
local action     = ARGV[1]

if redis.call("EXISTS", cfg_key) == 0 then
  return {0, "key not found"}
end
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked"}
end

if action == "configure" then
  local threshold = tonumber(ARGV[4])
  local cooldown = tonumber(ARGV[5])
  local enabled = ARGV[6] == "1"
  if not threshold or threshold < 1 or not cooldown or cooldown < 1000 then
    return {0, "bad_arguments"}
  end
  redis.call("HSET", policy_key,
    "failure_threshold", threshold,
    "cooldown_ms", cooldown,
    "enabled", enabled and "1" or "0")
  return {1, threshold, cooldown, enabled and "1" or "0"}
end

if action == "reset" then
  local state_key = KEYS[3]
  local caller_hash = ARGV[2]
  local caller_name = ARGV[3] or caller_hash
  local t = redis.call("TIME")
  local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
  local next_gen = 1
  local callers_key = "{qk:" .. string.match(KEYS[3], "^{qk:([^}]+)}") .. "}cbci"
  if redis.call("EXISTS", state_key) == 0 then
    redis.call("HSET", callers_key, caller_hash, caller_name)
    redis.call("HSET", state_key,
      "state", "closed", "gen", tostring(next_gen),
      "failures", "0", "probe", "0",
      "opened_at_ms", "0", "open_until_ms", "0",
      "manually_reset_at_ms", now_ms)
    redis.call("PEXPIRE", state_key, 86400000)
    return {2, caller_hash, next_gen, now_ms}
  end
  next_gen = tonumber(redis.call("HGET", state_key, "gen") or "0") + 1
  redis.call("HSET", callers_key, caller_hash, caller_name)
  redis.call("HSET", state_key,
    "state", "closed", "gen", tostring(next_gen),
    "failures", "0", "probe", "0", "probe_id", "",
    "opened_at_ms", "0", "open_until_ms", "0",
    "manually_reset_at_ms", now_ms)
  redis.call("PEXPIRE", state_key, 86400000)
  return {1, caller_hash, next_gen, now_ms}
end

return {0, "bad_action"}
