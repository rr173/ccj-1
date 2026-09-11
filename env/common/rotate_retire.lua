-- rotate_retire.lua
-- 控制面在“说好的宽限时间还没到”时，提前把上一版旧明文收掉。
--
-- 收掉只改换新状态（rstat.prev_state=retired）：
--   * 旧明文立刻不能再占【新的】额度（reserve.lua 换新门实时读 rstat）；
--   * 不删别名、不碰任何计数与占用——用旧明文开了头还没结的在飞占用仍在
--     同一份突发/窗口里占着，settle.lua 不看换新状态，照常用完、照常确认，
--     不会因为明文作废就把额度放掉；
--   * 已经真用掉的账一笔不动，突发/窗口也不会被重新装满。
--
-- KEYS[1] = rstat  {qk:<kid>}rstat
--
-- ARGV[1] = kid
--
-- 返回：
--   {1, prev_cred, grace_until_ms}   本次提前收掉成功
--   {2, prev_cred, grace_until_ms}   已收过 / 宽限已自然到期（幂等，效果相同）
--   {0, "not found"}                 密钥从没换过新，没有上一版可收
--   {0, "bad arguments"}

local rstat_key = KEYS[1]
local kid       = ARGV[1]

if not kid or kid == "" then
  return {0, "bad arguments"}
end
if redis.call("EXISTS", rstat_key) == 0 then
  return {0, "not found"}
end

local prev_cred = redis.call("HGET", rstat_key, "prev") or ""
if prev_cred == "" then
  return {0, "not found"}
end
local grace_until = redis.call("HGET", rstat_key, "grace_until_ms") or "0"
local state       = redis.call("HGET", rstat_key, "prev_state") or "active"

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
if state == "retired" or tonumber(grace_until) <= now_ms then
  -- 已提前收过是幂等；宽限已自然到期时旧明文同样不能再占新的，效果一致。
  return {2, prev_cred, tonumber(grace_until) or 0}
end

redis.call("HSET", rstat_key, "prev_state", "retired")
return {1, prev_cred, tonumber(grace_until) or 0}
