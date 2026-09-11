-- sweep.lua
-- 数据面后台兜底：周期性把“过了约定回音时限还没回音”的占用单终结为超时退回。
-- 同时支持两种持有登记：
--   * {qk:<kid>}holds / sholds:*：密钥自己的额度池
--   * {qp:<pid>}holds           ：跨密钥共享池的聚合计数
-- 跨密钥请求有两张配对占用单；哪边先被扫到，只要另一边也已到时限就一起摘掉，
-- 避免一把密钥已经退出/池已停止时留下悬挂占用。密钥侧 timeout 流水只写一次：
-- 从密钥登记扫到时写；从池登记扫到时，若配对密钥单也超时则补那一笔。
--
-- KEYS[1] = holds   突发占用登记 ZSET
-- ARGV[1] = limit
-- 返回：{finalized_count}

local holds_key = KEYS[1]
local limit     = tonumber(ARGV[1]) or 100

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

local tag = string.match(holds_key, "^{([^}]+)}") or ""
local is_pool = string.sub(tag, 1, 3) == "qp:"

local function members_for(rkey, cost_val)
  local mm = {}
  for i = 1, cost_val do mm[#mm + 1] = rkey .. "#" .. i end
  return mm
end

local function zremove_all(zh, zw, rkey, cost_val)
  local mm = members_for(rkey, cost_val)
  if zh and zh ~= "" then redis.call("ZREM", zh, unpack(mm)) end
  if zw and zw ~= "" then redis.call("ZREM", zw, unpack(mm)) end
end

local function append_timeout(kid, rkey, caller, idem, reserve_at, cost_val, pool_pid)
  if not kid or kid == "" then return end
  local ledger_key = "{qk:" .. kid .. "}ledger"
  local eid = redis.call("XADD", ledger_key, "*",
    "kind", "release", "kid", kid,
    "res", rkey,
    "caller", caller or "",
    "idem", idem or "",
    "pool", "pool",
    "cost", tostring(cost_val),
    "reason", "timeout",
    "late", "0",
    "reserve_at", reserve_at or "0",
    "pool_pid", pool_pid or "")
  local cut = string.find(eid, "-", 1, true)
  local score = cut and tonumber(string.sub(eid, 1, cut - 1)) or now_ms
  if caller and caller ~= "" then
    redis.call("ZADD", "{qk:" .. kid .. "}lci:" .. redis.sha1hex(caller),
               score, eid)
  end
  if idem and idem ~= "" then
    redis.call("ZADD", "{qk:" .. kid .. "}lii:" .. redis.sha1hex(idem),
               score, eid)
  end
end

local expired_members = redis.call(
  "ZRANGEBYSCORE", holds_key, "-inf", now_ms, "LIMIT", 0, limit)
if #expired_members == 0 then
  return {0}
end

local finalized = 0
local seen = {}
for _, m in ipairs(expired_members) do
  local cut = string.find(m, "#", 1, true)
  if cut then
    local rkey = string.sub(m, 1, cut - 1)
    if not seen[rkey] then
      seen[rkey] = true
      if is_pool then
        local f = redis.call("HMGET", rkey, "outcome", "lease_exp_ms", "cost",
                             "holds", "wholds", "kid", "caller", "idem",
                             "reserved_at_ms", "key_res")
        if f[1] == "" and (tonumber(f[2] or "0") or 0) <= now_ms then
          local cost_val = tonumber(f[3] or "1") or 1
          local paired_timeout = false
          local kr = f[10] or ""
          if kr ~= "" and redis.call("EXISTS", kr) == 1 then
            local kf = redis.call("HMGET", kr, "outcome", "lease_exp_ms", "cost",
                                  "holds", "wholds")
            if kf[1] == "" and (tonumber(kf[2] or "0") or 0) <= now_ms then
              local kcost = tonumber(kf[3] or "1") or 1
              zremove_all(kf[4], kf[5], kr, kcost)
              redis.call("HSET", kr, "outcome", "expired")
              paired_timeout = true
            end
          end
          zremove_all(f[4], f[5], rkey, cost_val)
          redis.call("HSET", rkey, "outcome", "expired")
          if paired_timeout then
            append_timeout(f[6], kr, f[7], f[8], f[9], cost_val,
                           string.sub(tag, 4))
          end
          finalized = finalized + 1
        end
      else
        local f = redis.call("HMGET", rkey, "outcome", "lease_exp_ms", "cost",
                             "holds", "wholds", "caller", "idem", "pool",
                             "reserved_at_ms", "kid", "pool_pid", "pool_pres")
        if f[1] == "" and (tonumber(f[2] or "0") or 0) <= now_ms and f[10] then
          local cost_val = tonumber(f[3] or "1") or 1
          local ppid, ppr = f[11] or "", f[12] or ""
          if ppid ~= "" and ppr ~= "" and redis.call("EXISTS", ppr) == 1 then
            local pf = redis.call("HMGET", ppr, "outcome", "lease_exp_ms", "cost",
                                  "holds", "wholds")
            if pf[1] == "" and (tonumber(pf[2] or "0") or 0) <= now_ms then
              local pcost = tonumber(pf[3] or "1") or 1
              zremove_all(pf[4], pf[5], ppr, pcost)
              redis.call("HSET", ppr, "outcome", "expired")
            end
          end
          zremove_all(f[4], f[5], rkey, cost_val)
          redis.call("HSET", rkey, "outcome", "expired")
          local eid = redis.call("XADD", "{qk:" .. f[10] .. "}ledger", "*",
            "kind", "release", "kid", f[10],
            "res", rkey,
            "caller", f[6] or "",
            "idem", f[7] or "",
            "pool", f[8] or "",
            "cost", tostring(cost_val),
            "reason", "timeout",
            "late", "0",
            "reserve_at", f[9] or "0",
            "pool_pid", ppid)
          local dash = string.find(eid, "-", 1, true)
          local score = dash and tonumber(string.sub(eid, 1, dash - 1)) or now_ms
          if f[6] and f[6] ~= "" then
            redis.call("ZADD", "{qk:" .. f[10] .. "}lci:" .. redis.sha1hex(f[6]),
                       score, eid)
          end
          if f[7] and f[7] ~= "" then
            redis.call("ZADD", "{qk:" .. f[10] .. "}lii:" .. redis.sha1hex(f[7]),
                       score, eid)
          end
          finalized = finalized + 1
        end
      end
    end
  end
end
return {finalized}
