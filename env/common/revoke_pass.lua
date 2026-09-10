-- revoke_pass.lua
-- 控制面“立即作废旧一张通行证”使用（密钥停用作废全部通行证由 revoked 位表达，
-- 数据面/查询面实时判定，无需逐张改）。
--
-- 语义：
--   * 只给通行证打 revoked 标记与吊销时刻，绝不删它的占用登记/计数；
--   * 标记之后数据面立刻不能再用它占额，在飞占用确认也会被拒
--     （settle.lua 确认前实时查标记），占着的等约定回音时限自动退回；
--   * 它没占完的额度按两种口径退回这把密钥的公共池：
--       窗口额度立刻随容量恢复；突发容量恢复，令牌按补充速率回补
--       （与删除份额同一口径）；
--   * 在飞占用还占着的那段，在其约定回音时限到期前继续挡住公共池
--     （reserve.lua/quota.lua 把“已作废通行证的在占登记”计入公共池占用），
--     因此不会因为提前作废而超发；
--   * 幂等：重复吊销、通行证已到期，都返回成功，状态如实带回。
--
-- KEYS[1] = cfg
-- KEYS[2] = passes
--
-- ARGV:
--  1  pass_id
--
-- 返回：
--   {1, state}              state = "revoked"（本次或此前已吊销）/"expired"（已到期）
--   {0, "key not found"}
--   {0, "pass not found"}

local cfg_key    = KEYS[1]
local passes_key = KEYS[2]
local pid        = ARGV[1]

if pid == "" then
  return {0, "pass not found"}
end
if redis.call("EXISTS", cfg_key) == 0 then
  return {0, "key not found"}
end

local raw = redis.call("HGET", passes_key, pid)
if not raw then
  return {0, "pass not found"}
end
local ok, s = pcall(cjson.decode, raw)
if not ok or type(s) ~= "table" then
  return {0, "pass not found"}
end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

local expired = (tonumber(s["e"]) or 0) <= now_ms
if s["r"] == "1" then
  return {1, "revoked"}
end
if expired then
  -- 已自然到期：无需改任何东西，状态如实返回
  return {1, "expired"}
end

s["r"]  = "1"
s["ra"] = now_ms
redis.call("HSET", passes_key, pid, cjson.encode(s))
return {1, "revoked"}
