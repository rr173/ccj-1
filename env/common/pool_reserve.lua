-- pool_reserve.lua
-- 数据面在“密钥自己那份额度”之外，再为跨密钥共享池占一笔额度。
-- 密钥侧占用与池侧占用是两张单子，二者用相同的业务号/匿名调用上下文关联；
-- 只有两边都占住才放行。任一边占不住，调用都不能进入上游。
--
-- 这个脚本只维护共享池自己的聚合计数，绝不碰密钥自身 tb/win：
--   * 池满了，即使密钥自己还有额度，也返回 429；
--   * 池停了（stopped=1），成员密钥立刻不能再占新的池额度；
--   * 非成员密钥由 membership 精确匹配拒绝；
--   * 已占的单子超时后自动释放；若密钥侧配对单也过期，一并终结。
--
-- 单机 Redis 可在 Lua 中访问池和密钥两种 hash tag；Redis Cluster 下这些 key
-- 不同 slot，需要把跨密钥池改为外部协调或同 slot 数据模型。
--
-- KEYS[1] = pcfg       池配置 {qp:<pid>}cfg
-- KEYS[2] = ptb        池令牌桶 {qp:<pid>}tb
-- KEYS[3] = members    池成员 hash {qp:<pid>}members
-- KEYS[4] = membership 密钥当前归属 {qk:<kid>}pool
-- KEYS[5] = pres       池侧占用单 {qp:<pid>}pres:<...>
--
-- ARGV: pid,kid,burst_capacity,burst_refill_ms,window_seconds,window_quota,
--       cost,idem,lease_seconds,client,key_res
--
-- 返回与 reserve.lua 对齐：
--   {1, remaining_burst, remaining_window, lease_expires, pres_key}
--   {0, reason, retry_after_ms, remaining_burst, remaining_window}
--   {2, lease_expires, outcome, remaining_burst, remaining_window, pres_key}
--   {3, error}

local pcfg_key       = KEYS[1]
local ptb_key        = KEYS[2]
local members_key    = KEYS[3]
local membership_key = KEYS[4]
local pres_key       = KEYS[5]

local pid            = ARGV[1]
local kid            = ARGV[2]
local capacity       = tonumber(ARGV[3])
local refill_ms      = tonumber(ARGV[4])
local window_seconds = tonumber(ARGV[5])
local window_quota   = tonumber(ARGV[6])
local cost           = tonumber(ARGV[7])
local idem           = ARGV[8] or ""
local lease_seconds  = tonumber(ARGV[9])
local client         = ARGV[10] or ""
local key_res        = ARGV[11] or ""

if not capacity or not refill_ms or not window_seconds or not window_quota
   or not cost or not lease_seconds or capacity <= 0 or refill_ms <= 0
   or window_seconds <= 0 or window_quota <= 0 or cost <= 0
   or lease_seconds <= 0 then
  return {3, "bad arguments"}
end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]
local SCALE  = 1000

local holds_key  = "{qp:" .. pid .. "}holds"
local wholds_key = "{qp:" .. pid .. "}wholds"

local function members_for_cost(rkey, cost_val)
  local mm = {}
  for i = 1, cost_val do mm[#mm + 1] = rkey .. "#" .. i end
  return mm
end

-- 写一笔密钥侧超时流水。池本身没有独立流水；每把密钥的账本仍能说明这把密钥
-- 在池里占过/确认/释放过哪些调用。
local function append_key_timeout(kid_val, rkey, fields, cost_val)
  if not kid_val or kid_val == "" then return end
  local ledger_key = "{qk:" .. kid_val .. "}ledger"
  local eid = redis.call("XADD", ledger_key, "*",
    "kind", "release", "kid", kid_val,
    "res", rkey,
    "caller", fields.caller or "",
    "idem", fields.idem or "",
    "pool", "pool",
    "cost", tostring(cost_val),
    "reason", "timeout",
    "late", "0",
    "reserve_at", fields.reserve_at or "0",
    "pool_pid", pid)
  local score = tonumber(string.sub(eid, 1, string.find(eid, "-") - 1)) or now_ms
  local caller = fields.caller or ""
  if caller ~= "" then
    redis.call("ZADD", "{qk:" .. kid_val .. "}lci:" .. redis.sha1hex(caller),
               score, eid)
  end
  local idem_val = fields.idem or ""
  if idem_val ~= "" then
    redis.call("ZADD", "{qk:" .. kid_val .. "}lii:" .. redis.sha1hex(idem_val),
               score, eid)
  end
end

-- 池侧单子超时：释放池占用；若它记录了配对的密钥侧单子，且那张单子也未了结、
-- 已到时限，就一起释放。两张单子的时限由同一次请求写入，正常同时到期。
local function finalize_expired_pair(rkey)
  local f = redis.call("HMGET", rkey, "outcome", "lease_exp_ms", "cost",
                       "holds", "wholds", "kid", "caller", "idem",
                       "reserved_at_ms", "key_res")
  if f[1] ~= "" or (tonumber(f[2] or "0") or 0) > now_ms then
    return false
  end
  local cost_val = tonumber(f[3] or "1") or 1
  local mm = members_for_cost(rkey, cost_val)
  if f[4] and f[4] ~= "" then redis.call("ZREM", f[4], unpack(mm)) end
  if f[5] and f[5] ~= "" then redis.call("ZREM", f[5], unpack(mm)) end
  redis.call("HSET", rkey, "outcome", "expired")

  local kr = f[10] or ""
  if kr ~= "" and redis.call("EXISTS", kr) == 1 then
    local kf = redis.call("HMGET", kr, "outcome", "lease_exp_ms", "cost",
                          "holds", "wholds")
    local kexp = tonumber(kf[2] or "0") or 0
    if kf[1] == "" and kexp <= now_ms then
      local kcost = tonumber(kf[3] or "1") or 1
      local kmm = members_for_cost(kr, kcost)
      if kf[4] and kf[4] ~= "" then redis.call("ZREM", kf[4], unpack(kmm)) end
      if kf[5] and kf[5] ~= "" then redis.call("ZREM", kf[5], unpack(kmm)) end
      redis.call("HSET", kr, "outcome", "expired")
      append_key_timeout(f[6], kr,
        {caller = f[7] or "", idem = f[8] or "", reserve_at = f[9] or "0"},
        kcost)
    end
  end
  return true
end

if idem ~= "" then
  local prev = redis.call("HGETALL", pres_key)
  if #prev > 0 then
    local h = {}
    for i = 1, #prev, 2 do h[prev[i]] = prev[i + 1] end
    local outcome = h.outcome or ""
    if outcome == "expired" then outcome = "timeout" end
    if outcome ~= "" then
      return {2, tonumber(h.lease_exp or 0), outcome,
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0), pres_key}
    end
    if tonumber(h.lease_exp_ms or "0") <= now_ms then
      finalize_expired_pair(pres_key)
      return {2, tonumber(h.lease_exp or 0), "timeout",
              tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0), pres_key}
    end
    -- 池停了：在飞单不能继续拿它去调上游；等超时自动释放。
    if redis.call("HGET", pcfg_key, "stopped") == "1" then
      return {0, "pool_stopped", 0, 0, 0}
    end
    return {2, tonumber(h.lease_exp or 0), "",
            tonumber(h.rem_burst or 0), tonumber(h.rem_window or 0), pres_key}
  end
end

if redis.call("EXISTS", pcfg_key) == 0 then
  return {3, "pool not found"}
end
if redis.call("HGET", pcfg_key, "stopped") == "1" then
  return {0, "pool_stopped", 0, 0, 0}
end
-- 成员归属必须精确：同一把密钥同时只能待在一个池里；没进这个池的密钥不受限。
if redis.call("GET", membership_key) ~= pid then
  return {3, "not pool member"}
end
if redis.call("HEXISTS", members_key, kid) == 0 then
  return {3, "not pool member"}
end

local function sweep_pool_holds(zkey)
  local expired = redis.call("ZRANGEBYSCORE", zkey, "-inf", now_ms)
  if #expired > 0 then
    redis.call("ZREMRANGEBYSCORE", zkey, "-inf", now_ms)
    local seen = {}
    for _, m in ipairs(expired) do
      local cut = string.find(m, "#", 1, true)
      if cut then
        local rkey = string.sub(m, 1, cut - 1)
        if not seen[rkey] then
          seen[rkey] = true
          finalize_expired_pair(rkey)
        end
      end
    end
  end
end
sweep_pool_holds(holds_key)
sweep_pool_holds(wholds_key)

local held_burst  = redis.call("ZCARD", holds_key)
local held_window = redis.call("ZCARD", wholds_key)

local win_ms         = window_seconds * 1000
local cur_idx        = math.floor(now_ms / win_ms)
local cur_start      = cur_idx * win_ms
local prev_key       = "{qp:" .. pid .. "}win:" .. window_seconds .. ":" .. (cur_idx - 1)
local cur_key        = "{qp:" .. pid .. "}win:" .. window_seconds .. ":" .. cur_idx
local cur_used       = tonumber(redis.call("GET", cur_key) or "0")
local prev_used      = tonumber(redis.call("GET", prev_key) or "0")
local elapsed        = now_ms - cur_start
local prev_weight    = (win_ms - elapsed) / win_ms
local window_used    = math.floor(cur_used + prev_used * prev_weight)
local need_tokens    = cost * SCALE
local rem_window     = math.max(window_quota - window_used - held_window, 0)

local tokens
if redis.call("EXISTS", ptb_key) == 0 then
  tokens = capacity * SCALE
else
  local old_tokens = tonumber(redis.call("HGET", ptb_key, "tokens"))
  local old_ts     = tonumber(redis.call("HGET", ptb_key, "ts"))
  if not old_tokens or not old_ts then
    tokens = capacity * SCALE
  elseif old_tokens >= capacity * SCALE then
    tokens = old_tokens
  else
    local elapsed_us = now_us - old_ts
    if elapsed_us < 0 then elapsed_us = 0 end
    local refilled = math.floor((elapsed_us / 1000) * SCALE / refill_ms)
    tokens = math.min(capacity * SCALE, old_tokens + refilled)
  end
end

local avail_tokens = tokens - held_burst * SCALE
local rem_burst    = math.max(math.floor(avail_tokens / SCALE), 0)

if avail_tokens < need_tokens then
  local wait_ms = math.ceil((need_tokens - avail_tokens) * refill_ms / SCALE)
  local full_ms = capacity * refill_ms
  if wait_ms > full_ms then wait_ms = full_ms end
  if wait_ms < 1 then wait_ms = 1 end
  return {0, "burst_limited", wait_ms, rem_burst, rem_window}
end
if window_used + held_window + cost > window_quota then
  local retry_ms = win_ms - elapsed
  if retry_ms < 1 then retry_ms = 1 end
  return {0, "window_limited", retry_ms, rem_burst, 0}
end

local lease_exp_ms = now_ms + lease_seconds * 1000
local lease_exp    = math.floor(lease_exp_ms / 1000)
local zargv = {}
for i = 1, cost do
  zargv[#zargv + 1] = lease_exp_ms
  zargv[#zargv + 1] = pres_key .. "#" .. i
end
redis.call("ZADD", holds_key, unpack(zargv))
redis.call("ZADD", wholds_key, unpack(zargv))
local ttl_ms = lease_seconds * 1000 + 5000
redis.call("PEXPIRE", holds_key, ttl_ms)
redis.call("PEXPIRE", wholds_key, ttl_ms)

local new_rem_burst  = math.max(math.floor((avail_tokens - need_tokens) / SCALE), 0)
local new_rem_window = math.max(window_quota - window_used - held_window - cost, 0)

redis.call("HSET", pres_key,
  "pid", pid,
  "kid", kid,
  "caller", client,
  "idem", idem,
  "cost", cost,
  "tb", ptb_key,
  "win_prefix", "{qp:" .. pid .. "}win:",
  "holds", holds_key,
  "wholds", wholds_key,
  "cap", capacity,
  "refill_ms", refill_ms,
  "win_seconds", window_seconds,
  "key_res", key_res,
  "lease_exp", lease_exp,
  "lease_exp_ms", lease_exp_ms,
  "reserved_at_ms", now_ms,
  "rem_burst", new_rem_burst,
  "rem_window", new_rem_window,
  "outcome", "")
if idem ~= "" then
  redis.call("EXPIRE", pres_key, 86400)
else
  redis.call("EXPIRE", pres_key, lease_seconds + 300)
end

return {1, new_rem_burst, new_rem_window, lease_exp, pres_key}
