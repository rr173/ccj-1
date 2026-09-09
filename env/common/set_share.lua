-- set_share.lua
-- 控制面“给密钥的某个调用方预留一份额度 / 取消预留”使用。
-- 校验与写入在同一脚本内原子完成，多控制面副本并发也不会出现
-- “各自校验通过、加起来超总量”的竞态。
--
-- 只写配置（shares 表与 cfg.reserved_*），绝不触碰任何计数 key——
-- 已发生的扣减永远以数据面写入的 tb/win 为准。
--
-- KEYS[1] = cfg     配置 hash
-- KEYS[2] = shares  份额配置 hash（field=调用方名, value=JSON）
--
-- ARGV:
--  1  caller      调用方名（与数据面 X-Client-Id 对应）
--  2  share_json  份额配置 '{"burst_capacity":N,"window_quota":K}'；
--                 空串表示删除该调用方的份额（其后续调用回到公共池）
--
-- 返回：
--   {1, reserved_burst, reserved_window}                              成功
--   {0, "key not found"}                                              密钥不存在
--   {0, "revoked"}                                                    密钥已停用（只能给有效密钥指定份额）
--   {0, "bad share"}                                                  份额参数非法
--   {0, "share not found"}                                            删除不存在的份额
--   {0, "exceeds_total", sum_burst, sum_window, capacity, quota}      Σ份额超总量

local cfg_key    = KEYS[1]
local shares_key = KEYS[2]

local caller     = ARGV[1]
local share_json = ARGV[2]

if caller == "" then
  return {0, "bad share"}
end
if redis.call("EXISTS", cfg_key) == 0 then
  return {0, "key not found"}
end
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked"}
end

local capacity     = tonumber(redis.call("HGET", cfg_key, "burst_capacity"))
local window_quota = tonumber(redis.call("HGET", cfg_key, "window_quota"))
if not capacity or not window_quota then
  return {0, "key not found"}
end

-- 现有份额求和（可跳过某个 caller：用于“换上新值后再校验”）
local function sum_shares(skip)
  local sum_b, sum_w = 0, 0
  local all = redis.call("HGETALL", shares_key)
  for i = 1, #all, 2 do
    if all[i] ~= skip then
      local s = cjson.decode(all[i + 1])
      sum_b = sum_b + (tonumber(s["burst_capacity"]) or 0)
      sum_w = sum_w + (tonumber(s["window_quota"]) or 0)
    end
  end
  return sum_b, sum_w
end

if share_json == "" then
  -- 删除份额：预留立即释放回公共池；该调用方已有的份额计数 key 靠 TTL 自然过期
  if redis.call("HEXISTS", shares_key, caller) == 0 then
    return {0, "share not found"}
  end
  redis.call("HDEL", shares_key, caller)
else
  local ok, s = pcall(cjson.decode, share_json)
  if not ok or type(s) ~= "table" then
    return {0, "bad share"}
  end
  local nb = tonumber(s["burst_capacity"])
  local nw = tonumber(s["window_quota"])
  if not nb or not nw or nb <= 0 or nw <= 0
     or nb ~= math.floor(nb) or nw ~= math.floor(nw) then
    return {0, "bad share"}
  end
  -- 核心约束：所有人加起来不能超过这把密钥原来的总量
  local sum_b, sum_w = sum_shares(caller)
  sum_b = sum_b + nb
  sum_w = sum_w + nw
  if sum_b > capacity or sum_w > window_quota then
    return {0, "exceeds_total", sum_b, sum_w, capacity, window_quota}
  end
  redis.call("HSET", shares_key, caller, share_json)
end

-- 预留合计写回 cfg：数据面公共池配额 = 总量 − reserved_*（只读此字段）
local sum_b, sum_w = sum_shares(nil)
redis.call("HSET", cfg_key, "reserved_burst", sum_b, "reserved_window", sum_w)
return {1, sum_b, sum_w}
