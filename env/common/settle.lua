-- settle.lua
-- 数据面在上游有回音后执行一次“了结占用”。在 Redis 单线程内原子完成：
--   * confirm（调成了）：占用转为真用掉——此刻才扣令牌桶、才进窗口计数，
--     占用登记从 ZSET 移除；密钥已停用则拒绝确认（409），占用保持原状，
--     等释放或过了约定时限自动退回。
--   * release（没调成 / 过了约定回音时限还没回音）：占用登记作废，
--     没真扣过任何计数，额度立刻能被别人再占。
--
-- 占用单（{qk:<kid>}res:*）由 reserve.lua 写入，本脚本只认单子里的记录，
-- 不信任调用方传来的池/数量，因此结的一定是当初占的那一份、那个池。
--
-- 幂等：占用单已了结（outcome 非空）时直接返回原结论，不重复结；
-- 两种结局都返回 {1, outcome}，调用方按 outcome 区分。
--
-- KEYS[1] = cfg     配置 hash（确认前必须再查一次 revoked）
-- KEYS[2] = res     占用单 hash
-- KEYS[3] = ledger  占用流水 Stream（只 XADD，永不修改）
--
-- ARGV:
--  1  action      "confirm" / "release"
--  2  force       "1" 表示无视约定回音时限立即释放（没调成时的即时退款）；
--                 "0" 表示仅在已过期（lease_exp_ms <= now）时允许释放
--                 （占用方失联，由别人来把占着的额度退回公共池）
--
-- 占用单 outcome 状态机：
--   ""（还占着）
--     ├─ confirm（未停用）        → confirmed，记一笔 confirm
--     └─ release force / 过期     → released，记一笔 release(reason=upstream)
--   "expired"（约定时限过了没回音，reserve.lua/sweep.lua 已按超时退回，
--              并留过一笔 release(reason=timeout)）
--     ├─ confirm（未停用）        → confirmed，记一笔 confirm 且 late=1
--                                   （迟到的真实调用仍记账，不丢真实调用）
--     └─ release                 → released，不再留第二笔退回流水
--   "confirmed" / "released"      → 终态，任何再了结只回原结论
--
-- 返回：
--   {1, outcome, lease_expires_unix}   已了结：outcome = confirmed/released
--   {0, "not_found"}                   占用单不存在（从未占用或已过期清理）
--   {0, "revoked"}                     密钥已停用：不能确认（占用仍在，等退回）
--   {0, "not_expired", retry_after_ms} 未到约定回音时限，不能按过期释放
--   {0, "bad_action"}                  非法动作

local cfg_key    = KEYS[1]
local res_key    = KEYS[2]
local ledger_key = KEYS[3]

local action = ARGV[1]
local force  = ARGV[2] == "1"

if action ~= "confirm" and action ~= "release" then
  return {0, "bad_action"}
end

local prev = redis.call("HGETALL", res_key)
if #prev == 0 then
  return {0, "not_found"}
end
local h = {}
for i = 1, #prev, 2 do h[prev[i]] = prev[i + 1] end

local t      = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local now_us = t[1] * 1000000 + t[2]

local kid          = h.kid
-- 滚动升级边界：老版本占下的在飞占用单没有 kid 字段，从占用单 key 反解
-- （{qk:<kid>}res:...），保证升级前占的单子升级后仍能正常了结、留笔。
if not kid or kid == "" then
  local kp = string.match(res_key, "^{qk:([^}]+)}")
  if kp then kid = kp end
end
local cost         = tonumber(h.cost or "1")
local holds_key    = h.holds
local wholds_key   = h.wholds
local SCALE        = 1000

-- 占用流水只追加：XADD per-key Stream（entry id 即 Redis 毫秒时间戳），
-- 同时按调用方 / 业务号挂 ZSET 索引。停用、重启都不影响已落盘的流水。
local function ledger_add(kind, reason, late)
  local eid = redis.call("XADD", ledger_key, "*",
    "kind", kind, "kid", kid or "",
    "res", res_key,
    "caller", h.caller or "",
    "idem", h.idem or "",
    "pool", h.pool or "",
    "cost", tostring(cost),
    "reason", reason or "",
    "late", late and "1" or "0",
    "reserve_at", tostring(h.reserved_at_ms or 0))
  local score = tonumber(string.sub(eid, 1, string.find(eid, "-") - 1)) or now_ms
  local caller = h.caller or ""
  if caller ~= "" then
    redis.call("ZADD", "{qk:" .. kid .. "}lci:" .. redis.sha1hex(caller), score, eid)
  end
  local idem = h.idem or ""
  if idem ~= "" then
    redis.call("ZADD", "{qk:" .. kid .. "}lii:" .. redis.sha1hex(idem), score, eid)
  end
end

-- 终态幂等：confirmed/released 只回原结论；expired 单子收到 release 也在此
-- 拦截（超时退回时已留过 release 流水，不能再留第二笔）。expired 单子收到
-- confirm 不拦截——迟到的真实应答还要推进成 confirmed 并补记 confirm。
if h.outcome == "confirmed" or h.outcome == "released"
   or (h.outcome == "expired" and action == "release") then
  local shown = h.outcome
  if shown == "expired" then shown = "released" end
  return {1, shown, tonumber(h.lease_exp or 0)}
end

-- 占用登记的 member 清单（与 reserve.lua 的写法一一对应）
local members = {}
for i = 1, cost do
  members[#members + 1] = res_key .. "#" .. i
end

if action == "confirm" then
  local late_confirm = h.outcome == "expired"
  -- 密钥停了以后，已经占着的也不能再当成调成（迟到的确认也一样）
  if redis.call("EXISTS", cfg_key) == 0 then
    return {0, "not_found"}
  end
  if redis.call("HGET", cfg_key, "revoked") == "1" then
    return {0, "revoked"}
  end

  -- 占用登记作废（若已随回音时限过期被清掉，ZREM 为空操作，无副作用）
  if holds_key and holds_key ~= "" then
    redis.call("ZREM", holds_key, unpack(members))
  end
  if wholds_key and wholds_key ~= "" then
    redis.call("ZREM", wholds_key, unpack(members))
  end

  -- 真扣令牌桶：允许扣成负数——确认对应的是占用时判定过的额度，
  -- 负数表示“先花后补”，后续补充先把欠账填平，绝不凭空多出额度
  local tb_key = h.tb
  if tb_key and tb_key ~= "" then
    local cap       = tonumber(h.cap or "0")
    local refill_ms = tonumber(h.refill_ms or "0")
    if cap > 0 and refill_ms > 0 then
      local tokens
      if redis.call("EXISTS", tb_key) == 0 then
        tokens = cap * SCALE
      else
        local old_tokens = tonumber(redis.call("HGET", tb_key, "tokens"))
        local old_ts     = tonumber(redis.call("HGET", tb_key, "ts"))
        if not old_tokens or not old_ts then
          tokens = cap * SCALE
        else
          local elapsed = now_us - old_ts
          if elapsed < 0 then elapsed = 0 end
          local elapsed_ms = elapsed / 1000
          local refilled = math.floor(elapsed_ms * SCALE / refill_ms)
          tokens = math.min(cap * SCALE, old_tokens + refilled)
        end
      end
      tokens = tokens - cost * SCALE
      redis.call("HSET", tb_key, "tokens", tokens, "ts", now_us)
      -- TTL：从当前余量补满整桶所需时间（欠账时为负→更久），留少量缓冲
      local full_ms = math.ceil((cap * SCALE - tokens) * refill_ms / SCALE) + 5000
      if full_ms < 5000 then full_ms = 5000 end
      redis.call("PEXPIRE", tb_key, full_ms)
    end
  end

  -- 真进窗口计数：落在确认时刻的当前固定桶，随滑动窗口自然老化
  local win_prefix = h.win_prefix
  local win_seconds = tonumber(h.win_seconds or "0")
  if win_prefix and win_prefix ~= "" and win_seconds > 0 then
    local win_ms  = win_seconds * 1000
    local cur_idx = math.floor(now_ms / win_ms)
    local cur_key = win_prefix .. win_seconds .. ":" .. cur_idx
    redis.call("INCRBY", cur_key, cost)
    redis.call("PEXPIRE", cur_key, win_ms * 2 + 5000)
  end

  redis.call("HSET", res_key, "outcome", "confirmed")
  -- 流水留一笔 confirm（迟到的真调用标 late=1）；真用掉以此为唯一口径，
  -- 与令牌桶/窗口的真扣在同一原子动作里，对账单据此与余量侧真用掉对账。
  ledger_add("confirm", "", late_confirm)
  return {1, "confirmed", tonumber(h.lease_exp or 0)}
end

-- release：未过期且非强制 → 占用方还有约定时间回音，不能抢
local lease_exp_ms = tonumber(h.lease_exp_ms or "0")
if not force and lease_exp_ms > now_ms then
  local wait_ms = lease_exp_ms - now_ms
  if wait_ms < 1 then wait_ms = 1 end
  return {0, "not_expired", wait_ms}
end

-- 释放 = 占用登记作废。占用阶段没真扣过任何计数，这里也无需退计数——
-- 登记一删，这笔额度立刻回到可占余量里，别人马上能再占。
-- （能走到这里的 outcome 只可能是 ""：expired 单子收到 release 已在上方
-- 按幂等返回，其 timeout 退回流水也已在超时时留过。）
if holds_key and holds_key ~= "" then
  redis.call("ZREM", holds_key, unpack(members))
end
if wholds_key and wholds_key ~= "" then
  redis.call("ZREM", wholds_key, unpack(members))
end

redis.call("HSET", res_key, "outcome", "released")
-- 上游 5xx / 连不上 / 调用方断连的即时退回（force=1），或按过期显式释放，
-- 留一笔 release(reason=upstream) 流水。同一业务号再来只回原结论，不留第二笔。
ledger_add("release", "upstream", false)
return {1, "released", tonumber(h.lease_exp or 0)}
