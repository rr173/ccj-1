-- failover_member.lua
-- 控制面给一把主钥绑定 / 解绑一把备钥。所有关系判断在同一脚本内完成，
-- 保证两条硬规则：
--   * 一把主钥同时只能绑一把备钥（{qk:<kid>}fbb 是唯一正向事实）；
--   * 一把备钥不能同时给好几把主钥顶（{qk:<bkid>}fbm 是反向索引）。
-- 只改配置关系，不碰任何计数与在飞占用：
--   * 解绑前已经占在备钥上的单子仍按备钥走完（占用单自记一切，见 settle.lua）；
--   * 解绑只阻止“新来的”再选这把备钥——主钥侧没有路由的新请求不再顶上；
--   * 同一业务号已经在备钥上占过的，凭主钥侧路由键 {qk:<kid>}fbrt:<h>
--     仍走备钥那张原单，路由键不随解绑删除。
--
-- KEYS[1] = backup_ptr   {qk:<kid>}fbb
-- KEYS[2] = master_ptr   {qk:<bkid>}fbm
-- KEYS[3] = kcfg         {qk:<kid>}cfg
-- KEYS[4] = bcfg         {qk:<bkid>}cfg
--
-- ARGV[1] = action  "bind" / "unbind"
-- ARGV[2] = kid     主钥 ID
-- ARGV[3] = bkid    备钥 ID（unbind 时由调用方按当前绑定回填，脚本只认这一对）
--
-- 返回：
--   {1}                       绑定/解绑成功
--   {0, "key not found"}      主钥或备钥不存在
--   {0, "self_backup"}        备钥就是主钥自己
--   {0, "key_revoked", who}   who="master"/"backup"：两把都必须还有效
--   {0, "already_bound", bkid}      主钥已绑另一把（先解绑才能换）
--   {0, "backup_taken", otherkid}   备钥已在给别的主钥顶
--   {0, "failover_chain"}     备钥自己也绑着备钥（不允许链式顶上）
--   {0, "not_bound"}          解绑时主钥没绑
--   {0, "binding_mismatch", bkid}  解绑时当前绑的不是这把
--   {0, "bad action"}

local backup_ptr = KEYS[1]
local master_ptr = KEYS[2]
local kcfg_key   = KEYS[3]
local bcfg_key   = KEYS[4]

local action = ARGV[1]
local kid    = ARGV[2]
local bkid   = ARGV[3]

if action == "bind" then
  if kid == bkid then
    return {0, "self_backup"}
  end
  if redis.call("EXISTS", kcfg_key) == 0 or redis.call("EXISTS", bcfg_key) == 0 then
    return {0, "key not found"}
  end
  if redis.call("HGET", kcfg_key, "revoked") == "1" then
    return {0, "key_revoked", "master"}
  end
  if redis.call("HGET", bcfg_key, "revoked") == "1" then
    return {0, "key_revoked", "backup"}
  end
  -- 主钥自己正给别的主钥当备钥：它不能再开一层（顶上只能走一层）
  if redis.call("EXISTS", "{qk:" .. kid .. "}fbm") == 1 then
    return {0, "failover_chain"}
  end

  local current = redis.call("GET", backup_ptr) or ""
  if current == bkid then
    -- 重复绑定同一把：幂等成功
    return {1}
  end
  if current ~= "" then
    return {0, "already_bound", current}
  end
  local owner = redis.call("GET", master_ptr) or ""
  if owner ~= "" and owner ~= kid then
    return {0, "backup_taken", owner}
  end
  if owner == kid then
    -- 备钥已经在给本主钥顶：幂等成功（与 current == bkid 分支等价兜底）
    return {1}
  end
  -- 要当备钥的这把 key 自己正作为别人的主钥绑着备钥，或者自己正给别的
  -- 主钥当备钥（owner 已在上面拦住）——顶上只能一层，不允许链式
  if redis.call("EXISTS", "{qk:" .. bkid .. "}fbb") == 1 then
    return {0, "failover_chain"}
  end
  if redis.call("EXISTS", "{qk:" .. bkid .. "}fbm") == 1 then
    return {0, "backup_taken", redis.call("GET", "{qk:" .. bkid .. "}fbm")}
  end

  redis.call("SET", backup_ptr, bkid)
  redis.call("SET", master_ptr, kid)
  return {1}
end

if action == "unbind" then
  local current = redis.call("GET", backup_ptr)
  if not current or current == "" then
    return {0, "not_bound"}
  end
  if current ~= bkid then
    return {0, "binding_mismatch", current}
  end
  redis.call("DEL", backup_ptr)
  -- 只清与这对关系一致的反向索引，避免并发换绑时误删新关系
  if redis.call("GET", master_ptr) == kid then
    redis.call("DEL", master_ptr)
  end
  return {1}
end

return {0, "bad action"}
