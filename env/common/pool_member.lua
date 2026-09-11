-- pool_member.lua
-- 控制面把一把有效密钥放进/拿出跨密钥共享池。所有归属判断在同一脚本内完成，
-- 保证“一把密钥同时只能待在一个池里”：
--   membership {qk:<kid>}pool 是唯一归属；池 members hash 是反向清单。
-- 只改配置归属，不碰密钥/池计数，也不释放已在飞的占用。移出后：
--   * 新请求不再受该池限制；
--   * 移出前已经占着的配对单仍记录原 pid，settle 时按原池确认/退回，走完整笔。
--
-- KEYS[1] = pcfg       {qp:<pid>}cfg
-- KEYS[2] = members    {qp:<pid>}members
-- KEYS[3] = membership {qk:<kid>}pool
-- KEYS[4] = kcfg       {qk:<kid>}cfg
-- ARGV[1] = action     "add" / "remove"
-- ARGV[2] = pid
-- ARGV[3] = kid

local pcfg_key       = KEYS[1]
local members_key    = KEYS[2]
local membership_key = KEYS[3]
local kcfg_key       = KEYS[4]
local action         = ARGV[1]
local pid            = ARGV[2]
local kid            = ARGV[3]

local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

if redis.call("EXISTS", pcfg_key) == 0 then
  return {0, "pool not found"}
end
if redis.call("EXISTS", kcfg_key) == 0 then
  return {0, "key not found"}
end

local current = redis.call("GET", membership_key) or ""

if action == "add" then
  if redis.call("HGET", pcfg_key, "stopped") == "1" then
    return {0, "pool_stopped"}
  end
  if redis.call("HGET", kcfg_key, "revoked") == "1" then
    return {0, "key_revoked"}
  end
  if current ~= "" and current ~= pid then
    return {0, "already_in_pool", current}
  end
  redis.call("SET", membership_key, pid)
  redis.call("HSET", members_key, kid, tostring(now_ms))
  return {1, redis.call("HLEN", members_key)}
end

if action == "remove" then
  if current ~= pid or redis.call("HEXISTS", members_key, kid) == 0 then
    return {0, "not_member"}
  end
  redis.call("DEL", membership_key)
  redis.call("HDEL", members_key, kid)
  return {1, redis.call("HLEN", members_key)}
end

return {0, "bad action"}
