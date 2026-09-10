-- sweep.lua
-- 数据面后台兜底：周期性把“过了约定回音时限还没回音”的占用单终结为超时
-- 退回——reserve.lua 在热池（每次新占用）路径上已做同样的清理，本脚本只
-- 兜住“这把密钥/这个池之后再没人来占”的冷场景，保证还占着的账不会挂死、
-- 每笔超时退回都有流水。
--
-- 只终结 outcome="" 且 lease_exp_ms<=now 的单子：删占用登记 → 置
-- outcome="expired" → XADD 一笔 release/reason=timeout 流水。
-- 幂等：同一批 ZSET 里每个单子只处理一次；sweep 与 reserve/settle 由 Redis
-- 单线程串行化，迟到的真实确认仍可把 expired 推进为 confirmed（见 settle.lua）。
--
-- KEYS[1] = holds   突发占用登记 ZSET（公共池或某份额池）
-- ARGV:
--  1  limit         单轮最多处理多少个过期登记（防止长池一次跑太久）
--
-- 返回：{finalized_count}

local holds_key = KEYS[1]
local limit     = tonumber(ARGV[1]) or 100

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)

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
      local f = redis.call("HMGET", rkey, "outcome", "lease_exp_ms", "cost",
                           "holds", "wholds", "caller", "idem", "pool",
                           "reserved_at_ms", "kid")
      local fo = f[1]
      local exp_ms = tonumber(f[2] or "0") or 0
      if fo == "" and exp_ms <= now_ms and f[10] then
        local cost_val = tonumber(f[3] or "1") or 1
        local mm = {}
        for i = 1, cost_val do mm[#mm + 1] = rkey .. "#" .. i end
        if f[4] then redis.call("ZREM", f[4], unpack(mm)) end
        if f[5] then redis.call("ZREM", f[5], unpack(mm)) end
        redis.call("HSET", rkey, "outcome", "expired")
        local ledger_key = "{qk:" .. f[10] .. "}ledger"
        local eid = redis.call("XADD", ledger_key, "*",
          "kind", "release", "kid", f[10],
          "res", rkey,
          "caller", f[6] or "",
          "idem", f[7] or "",
          "pool", f[8] or "",
          "cost", tostring(cost_val),
          "reason", "timeout",
          "late", "0",
          "reserve_at", f[9] or "0")
        local score = tonumber(string.sub(eid, 1, string.find(eid, "-") - 1)) or now_ms
        if f[6] and f[6] ~= "" then
          redis.call("ZADD",
            "{qk:" .. f[10] .. "}lci:" .. redis.sha1hex(f[6]), score, eid)
        end
        if f[7] and f[7] ~= "" then
          redis.call("ZADD",
            "{qk:" .. f[10] .. "}lii:" .. redis.sha1hex(f[7]), score, eid)
        end
        finalized = finalized + 1
      end
    end
  end
end
return {finalized}
