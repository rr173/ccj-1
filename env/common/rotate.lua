-- rotate.lua
-- 控制面“密钥换新”：给一把还有效的密钥换一把新的调用明文。
--
-- 换的是认证明文，不是配额身份：逻辑 kid（=签发时那把明文的哈希）名下的
-- cfg / tb / win:* / holds / shares / ledger / 共享池 / 主备关系全部原样，
-- 本脚本只新增“明文哈希 → 逻辑 kid”的别名与换新状态：
--   * {qk:<newcred>}rcr = kid              新明文的别名
--   * {qk:<oldcred>}rcr = kid              上一版明文的别名（宽限期内继续认）
--   * {qk:<kid>}rstat = {current,prev,grace_until_ms,rotated_at_ms,prev_state}
--
-- 硬规则（全部在 Redis 单线程同一段脚本内原子保证）：
--   1. 只能给存在、未停用的密钥换新；
--   2. 上一档换新说好的宽限时间还没到，不能再换一次（上一版还在 active）；
--      宽限自然到期、或上一版已被提前收掉（retired）后，才允许再换；
--   3. 换新绝不清零已真用掉、绝不重填突发/窗口——本脚本不碰任何计数 key；
--   4. 新旧明文吃同一份突发/窗口：别名只做“认证明文 → 逻辑 kid”的解析，
--      占用/确认/退回全部仍在逻辑 kid 名下的同一批计数与占用登记上；
--   5. 新明文必须是系统里没出现过的哈希（防止把两把密钥认成同一把）。
--
-- KEYS[1] = cfg    {qk:<kid>}cfg
-- KEYS[2] = rstat  {qk:<kid>}rstat
-- KEYS[3] = rcr    {qk:<newcred>}rcr  （新明文别名，调用方预生成）
--
-- ARGV:
--  1  kid             逻辑密钥 ID
--  2  newcred         新明文的 cred32（sha256 前 32 位）
--  3  grace_seconds   上一版明文还能再用多久（秒）
--
-- 返回：
--   {1, current_cred, prev_cred, grace_until_ms, rotated_at_ms}  换新成功
--   {0, "key not found"}
--   {0, "revoked"}                    密钥已停用
--   {0, "grace_active", prev_cred, grace_until_ms}
--                                    上一档换新的宽限时间还没到，不能再换
--   {0, "credential_in_use", cred}   新明文哈希已被别的（或这把）密钥登记
--   {0, "bad arguments"}

local cfg_key   = KEYS[1]
local rstat_key = KEYS[2]
local rcr_key   = KEYS[3]

local kid           = ARGV[1]
local newcred       = ARGV[2]
local grace_seconds = tonumber(ARGV[3])

if not newcred or newcred == "" or not grace_seconds
   or grace_seconds <= 0 or grace_seconds ~= math.floor(grace_seconds) then
  return {0, "bad arguments"}
end

if redis.call("EXISTS", cfg_key) == 0 then
  return {0, "key not found"}
end
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked"}
end

-- 新明文哈希不能已被任何密钥登记为别名（包括恰好等于这把密钥自己 kid 的
-- 极端情况——那等于没换）。签发时那把原始明文没有别名键，因此“第一次换新”
-- 时 oldcred=kid 不需要别名；这里只拦系统里真实存在的别名冲突。
if newcred == kid then
  return {0, "credential_in_use", newcred}
end
local owner = redis.call("GET", rcr_key)
if owner then
  return {0, "credential_in_use", newcred}
end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

-- 上一档换新还在宽限（上一版仍是 active 且说好的时间没到）：不能再换。
-- 提前收掉（prev_state=retired）后即使时间没到也允许换；宽限自然到期后
-- （grace_until_ms <= now）也允许换——旧别名保留，只是数据面不再认它占新的。
local prev_cred       = redis.call("HGET", rstat_key, "prev") or ""
local grace_until     = tonumber(redis.call("HGET", rstat_key, "grace_until_ms") or "0") or 0
local prev_state      = redis.call("HGET", rstat_key, "prev_state") or ""
if prev_cred ~= "" and prev_state ~= "retired" and grace_until > now_ms then
  return {0, "grace_active", prev_cred, grace_until}
end

-- 当前明文是谁：从没换过就是签发时那把（kid 本身）；换过就是 rstat.current。
local current_cred = redis.call("HGET", rstat_key, "current") or kid
local grace_until_ms = now_ms + grace_seconds * 1000

-- 上一版明文落别名（签发时那把原始明文的 kid 即 cfg 所在 tag，别名指向自己
-- 虽无解析必要，但统一登记后“此刻哪些明文还能用”的查询只需扫别名+rstat）。
redis.call("SET", "{qk:" .. current_cred .. "}rcr", kid)
redis.call("SET", rcr_key, kid)

redis.call("HSET", rstat_key,
  "current", newcred,
  "prev", current_cred,
  "prev_state", "active",
  "grace_until_ms", grace_until_ms,
  "rotated_at_ms", now_ms)
-- rstat 长期保留（换新历史/查询要用），别名 key 也不设 TTL：宽限到期是
-- 数据面按 grace_until_ms 实时判定的语义，不靠 key 老化，避免时钟/TTL 偏差。
return {1, newcred, current_cred, grace_until_ms, now_ms}
