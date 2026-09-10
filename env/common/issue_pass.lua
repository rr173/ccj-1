-- issue_pass.lua
-- 控制面“给还有效的密钥开一张临时通行证”使用。校验与写入在同一脚本内原子
-- 完成，多控制面副本并发也不会超切。
--
-- 通行证从这把密钥【当时还能再占】的额度里切出来：
--   1) 密钥必须存在且未停用（只给还有效的密钥开）；
--   2) 各份额 + 各张【有效】通行证合计不得超过密钥原来的总量（突发、窗口
--      两个维度分别约束）。到期/已吊销的通行证不再占位，其没占完的额度
--      已退回给这把密钥；
--   3) 即便总额度够，也要这一刻公共池（总量 − 全部份额 − 有效通行证）
--      【还能再占】的突发/窗口不少于申请量——切走的这段别人立刻拿不到，
--      不能把别人已经占着或已经真用掉的部分再切一遍。占不住就不开，
--      并把当前还能切多少如实带回去。
--
-- 切出方式与“份额”同构：通行证拥有独立的令牌桶/窗口计数/占用登记，
-- 公共池有效容量立即减去有效通行证合计；通行证到期或被吊销后，下一次
-- reserve/quota 实时判定即不再为它扣容量（额度立刻回到密钥），其在飞
-- 占用作为“释放中占用”继续挡住公共池，直到约定回音时限自动退回。
--
-- 只写通行证配置表（hash field=pass_id, value=JSON），绝不触碰任何计数 key。
--
-- KEYS[1] = cfg     密钥配置 hash
-- KEYS[2] = tb      公共池令牌桶（只读取其当前补充后的令牌数）
-- KEYS[3] = shares  份额配置 hash（计入总额校验）
-- KEYS[4] = passes  通行证配置 hash（本脚本写）
--
-- ARGV:
--  1  kid
--  2  pass_id          通行证ID（明文令牌的 SHA-256 前 32 位，控制面算好）
--  3  grantee          给谁（备注名，原样保存、原样展示）
--  4  burst_capacity   能占多少突发
--  5  window_quota     能占多少窗口总量
--  6  ttl_seconds      过多久作废（秒）；作废时刻按 Redis TIME 计算
--
-- 返回：
--   {1, expires_at_ms, reserved_burst, reserved_window,
--    avail_burst, avail_window}                            成功
--   {0, "key not found"}
--   {0, "revoked"}                                         密钥已停用
--   {0, "bad pass"}                                        参数非法
--   {0, "exceeds_total", sum_b, sum_w, cap, quota}         份额+通行证超总量
--   {0, "insufficient_available", free_b, free_w, nb, nw}  此刻可切额度不足
--
-- 通行证 JSON 字段：
--   g=grantee  b=burst_capacity  w=window_quota
--   e=expires_at_ms  c=created_at_ms  r=revoked("0"/"1")  ra=revoked_at_ms

local cfg_key    = KEYS[1]
local tb_key     = KEYS[2]
local shares_key = KEYS[3]
local passes_key = KEYS[4]

local kid        = ARGV[1]
local pid        = ARGV[2]
local grantee    = ARGV[3]
local nb         = tonumber(ARGV[4])
local nw         = tonumber(ARGV[5])
local ttl_s      = tonumber(ARGV[6])

if pid == "" or grantee == "" or string.len(grantee) > 200
   or not nb or not nw or not ttl_s
   or nb <= 0 or nw <= 0 or ttl_s <= 0
   or nb ~= math.floor(nb) or nw ~= math.floor(nw)
   or ttl_s ~= math.floor(ttl_s) then
  return {0, "bad pass"}
end
if redis.call("EXISTS", cfg_key) == 0 then
  return {0, "key not found"}
end
if redis.call("HGET", cfg_key, "revoked") == "1" then
  return {0, "revoked"}
end

local capacity     = tonumber(redis.call("HGET", cfg_key, "burst_capacity"))
local refill_ms    = tonumber(redis.call("HGET", cfg_key, "burst_refill_ms"))
local win_seconds  = tonumber(redis.call("HGET", cfg_key, "window_seconds"))
local window_quota = tonumber(redis.call("HGET", cfg_key, "window_quota"))
if not capacity or not refill_ms or not win_seconds or not window_quota then
  return {0, "key not found"}
end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]
local SCALE  = 1000

-- 已作废条目保留多久：到期/吊销后再留一个回音时限以上的缓冲，
-- 期间数据面还能凭它拒绝在飞占用的确认、quota 还能展示释放中占用。
local RETENTION_MS = 120000

-- 通行证是否仍有效（未吊销且未到作废时刻）
local function pass_active(s)
  if s["r"] == "1" then return false end
  local e = tonumber(s["e"])
  return (not e) or e > now_ms
end

-- 顺手清掉早已作废、过了保留期的条目（只删配置，计数 key 靠 TTL 自然过期）
local function sweep()
  local raw = redis.call("HGETALL", passes_key)
  for i = 1, #raw, 2 do
    local ok, s = pcall(cjson.decode, raw[i + 1])
    if ok and type(s) == "table" then
      local dead_at = nil
      if s["r"] == "1" then
        dead_at = tonumber(s["ra"]) or tonumber(s["e"])
      else
        local e = tonumber(s["e"])
        if e and e <= now_ms then dead_at = e end
      end
      if dead_at and dead_at + RETENTION_MS <= now_ms then
        redis.call("HDEL", passes_key, raw[i])
      end
    end
  end
end

sweep()

-- 现有份额合计
local share_b, share_w = 0, 0
do
  local all = redis.call("HGETALL", shares_key)
  for i = 1, #all, 2 do
    local ok, s = pcall(cjson.decode, all[i + 1])
    if ok and type(s) == "table" then
      share_b = share_b + (tonumber(s["burst_capacity"]) or 0)
      share_w = share_w + (tonumber(s["window_quota"]) or 0)
    end
  end
end

-- 现有【有效】通行证合计（跳过本次要新开的 pid，重放签发时按替换计）
local pass_b, pass_w = 0, 0
do
  local all = redis.call("HGETALL", passes_key)
  for i = 1, #all, 2 do
    if all[i] ~= pid then
      local ok, s = pcall(cjson.decode, all[i + 1])
      if ok and type(s) == "table" and pass_active(s) then
        pass_b = pass_b + (tonumber(s["b"]) or 0)
        pass_w = pass_w + (tonumber(s["w"]) or 0)
      end
    end
  end
end

-- 约束一：所有份额 + 所有有效通行证加起来不能超过密钥原来的总量
local sum_b = share_b + pass_b + nb
local sum_w = share_w + pass_w + nw
if sum_b > capacity or sum_w > window_quota then
  return {0, "exceeds_total", sum_b, sum_w, capacity, window_quota}
end

-- 公共池此刻的有效容量（还没算上这张新证）
local pool_cap = capacity - share_b - pass_b
local pool_wq  = window_quota - share_w - pass_w

-- 公共池突发：补充后的桶内令牌 − 还占着的（清过期口径与 reserve.lua 一致）
local holds_key  = "{qk:" .. kid .. "}holds"
local wholds_key = "{qk:" .. kid .. "}wholds"
redis.call("ZREMRANGEBYSCORE", holds_key, "-inf", now_ms)
redis.call("ZREMRANGEBYSCORE", wholds_key, "-inf", now_ms)
local held_b = tonumber(redis.call("ZCARD", holds_key)) or 0
local held_w = tonumber(redis.call("ZCARD", wholds_key)) or 0

local tokens
if redis.call("EXISTS", tb_key) == 0 then
  tokens = pool_cap * SCALE
else
  local old_tokens = tonumber(redis.call("HGET", tb_key, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", tb_key, "ts"))
  if not old_tokens or not old_ts then
    tokens = pool_cap * SCALE
  else
    local elapsed = now_us - old_ts
    if elapsed < 0 then elapsed = 0 end
    local refilled = math.floor((elapsed / 1000) * SCALE / refill_ms)
    tokens = math.min(pool_cap * SCALE, old_tokens + refilled)
  end
end
local free_burst = math.max(math.floor((tokens - held_b * SCALE) / SCALE), 0)

-- 公共池窗口：当前桶 + 前一桶加权，再减还占着的
local win_ms     = win_seconds * 1000
local cur_idx    = math.floor(now_ms / win_ms)
local cur_start  = cur_idx * win_ms
local prev_key   = "{qk:" .. kid .. "}win:" .. win_seconds .. ":" .. (cur_idx - 1)
local cur_key    = "{qk:" .. kid .. "}win:" .. win_seconds .. ":" .. cur_idx
local cur_used   = tonumber(redis.call("GET", cur_key) or "0")
local prev_used  = tonumber(redis.call("GET", prev_key) or "0")
local prev_weight = (win_ms - (now_ms - cur_start)) / win_ms
local window_used = math.floor(cur_used + prev_used * prev_weight)
local free_window = math.max(pool_wq - window_used - held_w, 0)

-- 约束二：只能从这把密钥“当时还能再占”的额度里切
if free_burst < nb or free_window < nw then
  return {0, "insufficient_available", free_burst, free_window, nb, nw}
end

local expires_at_ms = now_ms + ttl_s * 1000
local payload = cjson.encode({
  g = grantee, b = nb, w = nw,
  e = expires_at_ms, c = now_ms, r = "0",
})
redis.call("HSET", passes_key, pid, payload)
-- 与 set_share.lua 同一口径：reserved_* = Σ份额 + Σ有效通行证，
-- 数据面密钥公共池配额 = 总量 − reserved_*
redis.call("HSET", cfg_key, "reserved_burst", sum_b, "reserved_window", sum_w)

return {1, expires_at_ms, sum_b, sum_w, free_burst, free_window}
