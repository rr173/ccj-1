"""占用流水查询与对账单（控制面，只读 + 冲正归并）。

流水事实由数据面在 reserve.lua / settle.lua / sweep.lua 内与计数变更原子
写入，冲正由控制面在 reverse.lua 内原子写入：每把密钥一条只追加 Stream
``{qk:<kid>}ledger``，按调用方、业务号各挂一个 ZSET 索引。本模块只做读，
不写、不改任何流水——Stream 没有修改单条的命令，密钥停用、服务重启都不影响
已落盘的流水（AOF appendfsync always）。

两个入口：
  * list_events   ：按密钥（必选）+ 调用方 / 业务号 / 时间段翻流水；
  * build_statement：拉一段时间的对账单——这段占过多少、真用掉多少（净额，
                    已去掉冲回去的）、冲正多少（单独列）、退回多少、还占着的
                    单列，并把“流水侧净真用掉”与“余量计数侧真用掉（win 桶）”
                    对账，对不上两边数都亮出。
"""
from __future__ import annotations

import os
from typing import Optional

import redis

from common import keys

# 单次/交集查询的硬上限：防止一次翻太多拖垮控制面；翻页用 offset/limit
MAX_SCAN = 10000
PAGE_DEFAULT = 500
PAGE_MAX = 2000

# 对账单按“占用单的最终结局”归并：一笔单若先超时退回、之后迟到确认，
# 最终只能算真用掉，不能再算退回。区间结尾附近占的单，其结局事件可能落在
# 区间之后；拉账单时向后多扫这么久，把这些单的最终结局补齐（仅用于归并，
# 不改变流水事实，也不把区间外的占用计入）。默认 5 分钟，需大于
# RESERVATION_TTL_SECONDS（生产默认 60s）；数据面在飞调用最长 30s + lease。
SETTLE_LOOKAHEAD_MS = int(os.environ.get("SETTLE_LOOKAHEAD_MS", "300000"))


def now_ms(r: redis.Redis) -> int:
    """以 Redis 服务器时钟取当前毫秒时间戳（不相信本机时钟）。"""
    sec, micro = r.time()
    return int(sec) * 1000 + int(micro) // 1000


def _eid_ms(eid: str) -> int:
    dash = eid.find("-")
    return int(eid[:dash]) if dash >= 0 else 0


def _parse_event(eid: str, fields: dict, kid: str) -> dict:
    res_full = fields.get("res", "")
    prefix = f"{{qk:{kid}}}res:"
    res_tail = res_full[len(prefix):] if res_full.startswith(prefix) else res_full
    kind = fields.get("kind", "")
    out = {
        "id": eid,
        "ts_ms": _eid_ms(eid),
        "kind": kind,
        "caller": fields.get("caller", ""),
        "idem_key": fields.get("idem", ""),
        "pool": fields.get("pool", ""),
        "quota_pool_id": fields.get("pool_pid", ""),
        "cost": int(fields.get("cost", "1") or "1"),
        "reason": fields.get("reason", ""),
        "late": fields.get("late", "0") == "1",
        "reserved_at_ms": int(fields.get("reserve_at", "0") or "0"),
        "reservation_id": res_tail,
        "credential_id": fields.get("cred", ""),
    }
    if kind == "reversal":
        # 冲正专有：被冲那笔当时真用掉的量、管理员备注
        out["of_cost"] = int(fields.get("of", "0") or "0")
        out["note"] = fields.get("note", "")
    fb_for = fields.get("fb_for", "")
    if fb_for:
        # 这笔是顶上备钥替该主钥发生的（在备钥自己的流水里能看到主钥是谁）
        out["failover_for_key_id"] = fb_for
    if fields.get("fbmirror") == "1":
        # 主钥账本里的“顶上镜像”：计数实际在 fb_key 那把备钥上，镜像只是让
        # 主钥视角的流水/对账能看到备钥替它顶过的每一笔（不参与本钥 win 对账）
        out["failover_mirror"] = True
        out["served_by_key_id"] = fields.get("fb_key", "")
    return out


def _to_ms(value: Optional[float | int | str], name: str) -> Optional[int]:
    """查询参数里的时间：>1e11 视为毫秒（约 1973 年以后），否则视为秒。"""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a unix timestamp") from exc
    if v < 0:
        raise ValueError(f"{name} must be non-negative")
    ms = int(v * 1000) if v < 1e11 else int(v)
    return ms


def _events_by_ids(r: redis.Redis, kid: str, ids: list[str], limit: int,
                   offset: int) -> list[dict]:
    if not ids:
        return []
    page = ids[offset:offset + limit]
    if not page:
        return []
    stream = keys.ledger_stream_key(kid)
    pipe = r.pipeline(transaction=False)
    for eid in page:
        # 用精确 id 的 XRANGE 取单条；流水只追加不删除，索引里的 id 一定在
        pipe.xrange(stream, min=eid, max=eid, count=1)
    out = []
    for eid, rows in zip(page, pipe.execute()):
        if rows:
            out.append(_parse_event(rows[0][0], rows[0][1], kid))
    return out


def list_events(r: redis.Redis, kid: str, *,
                start: Optional[float] = None,
                end: Optional[float] = None,
                caller: Optional[str] = None,
                idem: Optional[str] = None,
                order: str = "desc",
                limit: int = PAGE_DEFAULT,
                offset: int = 0) -> dict:
    """翻流水。

    必选按密钥（kid）；可再按调用方、业务号（Idempotency-Key）、时间段过滤。
    走索引（ZSET member=stream entry id，score=事件毫秒时间戳），不扫全表。
    """
    if order not in ("asc", "desc"):
        raise ValueError("order must be 'asc' or 'desc'")
    limit = max(1, min(int(limit), PAGE_MAX))
    offset = max(0, int(offset))
    start_ms = _to_ms(start, "start")
    end_ms = _to_ms(end, "end")
    if start_ms is not None and end_ms is not None and start_ms >= end_ms:
        raise ValueError("start must be earlier than end")
    lo = start_ms if start_ms is not None else "-inf"
    # ZSET 区间用闭区间：end 参数按“不含”处理，所以上界取 end_ms-1
    hi = (end_ms - 1) if end_ms is not None else "+inf"

    stream = keys.ledger_stream_key(kid)
    candidate: list[str] = []
    n_candidate = 0

    if caller or idem:
        # 取候选 id（按时间段在索引内裁剪）；两个过滤都给时取交集
        id_sets: list[list[str]] = []
        if caller:
            zkey = keys.ledger_caller_key(kid, caller)
            id_sets.append(r.zrangebyscore(zkey, lo, hi, start=0, num=MAX_SCAN))
        if idem:
            zkey = keys.ledger_idem_key(kid, idem)
            id_sets.append(r.zrangebyscore(zkey, lo, hi, start=0, num=MAX_SCAN))
        if len(id_sets) == 1:
            candidate = id_sets[0]
        else:
            common = set(id_sets[0])
            for more in id_sets[1:]:
                common &= set(more)
            # ZRANGEBYSCORE 已按 id/score 升序返回；交集后恢复升序
            candidate = sorted(common)
        if any(len(s) >= MAX_SCAN for s in id_sets):
            # 命中硬上限时交集可能不全：明确报错，让管理员收窄条件
            raise ValueError(f"too many indexed events (>= {MAX_SCAN}); "
                             "narrow the time range or filters")
        n_candidate = len(candidate)
        events = _events_by_ids(r, kid, candidate, limit, offset)
    else:
        # 无调用方/业务号过滤：直接按 Stream 时间段读
        min_id = str(start_ms) if start_ms is not None else "-"
        # XRANGE 支持开区间前缀 '('（Redis 6.2+），end 不含
        max_id = ("(" + str(end_ms)) if end_ms is not None else "+"
        rows = r.xrange(stream, min=min_id, max=max_id, count=offset + limit + 1)
        n_candidate = len(rows)
        events = [_parse_event(eid, f, kid)
                  for eid, f in rows[offset:offset + limit]]

    if order == "desc":
        events.reverse()
    returned = len(events)
    has_more = offset + returned < n_candidate
    return {
        "key_id": kid,
        "range": {"start_ms": start_ms, "end_ms": end_ms},
        "filters": {"caller": caller or None, "idem_key": idem or None},
        "order": order,
        "limit": limit,
        "offset": offset,
        "returned": returned,
        "total_in_range": n_candidate,
        "has_more": has_more,
        "next_offset": offset + returned if has_more else None,
        "events": events,
    }


# ── 对账单 ────────────────────────────────────────────────────────────────

def _iter_stream(r: redis.Redis, stream: str,
                 start_ms: int, end_ms: int, batch: int = 1000):
    """按 [start_ms, end_ms) 从头翻 Stream，批量产出 (id, fields)。"""
    cursor = str(start_ms)
    max_id = "(" + str(end_ms)
    while True:
        rows = r.xrange(stream, min=cursor, max=max_id, count=batch)
        if not rows:
            return
        for row in rows:
            yield row
        if len(rows) < batch:
            return
        last_id = rows[-1][0]
        # 下一批从最后一条之后开始（entry id 严格递增）
        ms_part, seq_part = last_id.split("-")
        cursor = f"{ms_part}-{int(seq_part) + 1}"


def _pool_window_consumed(r: redis.Redis, kid: str,
                          win_seconds: int, idxs: list[int],
                          callers: list[str] | None) -> dict:
    """把对齐窗口内各固定桶的真用掉（win/swin 计数）加起来——这是余量侧
    “真用掉”的事实来源，与 quota.lua 读的是同一批计数 key。冲正时
    reverse.lua 从原确认桶 INCRBY 负数，因此这里读到的天然是冲正后净额。

    callers=None  → 公共池（{qk}win:*）
    callers=[...] → 这些点名调用方各自的份额池（{qk}swin:<sha1>:*）
    """
    pipe = r.pipeline(transaction=False)
    planned: list[tuple[str, int]] = []
    if callers is None:
        for idx in idxs:
            k = f"{{qk:{kid}}}win:{win_seconds}:{idx}"
            planned.append((k, idx))
            pipe.get(k)
    else:
        for c in callers:
            ch = keys.caller_hash(c)
            for idx in idxs:
                k = f"{{qk:{kid}}}swin:{ch}:{win_seconds}:{idx}"
                planned.append((k, idx))
                pipe.get(k)
    vals = pipe.execute()

    per_bucket: dict[int, int] = {idx: 0 for idx in idxs}
    for (k, idx), raw in zip(planned, vals):
        per_bucket[idx] += int(raw) if raw is not None else 0
    return {
        "total": sum(per_bucket.values()),
        "per_bucket": [{"bucket_index": idx, "consumed": per_bucket[idx]}
                       for idx in idxs],
    }


def _held_open(r: redis.Redis, kid: str, now_ms: int,
               start_ms: int, end_ms: int) -> list[dict]:
    """还占着的单子：以占用登记 ZSET（score=回音时限 > now）为准——
    这与 quota.lua 里 held 的口径完全一致。逐单去重后回查占用单。"""
    patterns = [f"{{qk:{kid}}}holds", f"{{qk:{kid}}}sholds:*"]
    res_keys: dict[str, str] = {}
    for pat in patterns:
        for zkey in r.scan_iter(match=pat, count=200):
            for member in r.zrangebyscore(zkey, "(" + str(now_ms), "+inf"):
                cut = member.find("#")
                if cut > 0:
                    res_keys[member[:cut]] = zkey
    if not res_keys:
        return []
    pipe = r.pipeline(transaction=False)
    for rk in res_keys:
        pipe.hmget(rk, "outcome", "lease_exp_ms", "cost", "caller", "idem",
                   "pool", "reserved_at_ms")
    out = []
    for rk, vals in zip(res_keys, pipe.execute()):
        outcome, lease_exp_ms, cost, caller, idem, pool, reserved_at = vals
        if outcome not in ("", None):
            continue  # 已终结但 ZSET 还没来得及清的边缘情况，不算还占着
        reserved_at_ms = int(reserved_at or 0)
        out.append({
            "reservation_id": rk[len(f"{{qk:{kid}}}res:"):],
            "caller": caller or "",
            "idem_key": idem or "",
            "pool": pool or "",
            "cost": int(cost or 1),
            "reserved_at_ms": reserved_at_ms,
            "lease_expires_ms": int(lease_exp_ms or 0),
            "in_range": start_ms <= reserved_at_ms < end_ms,
        })
    out.sort(key=lambda x: x["reserved_at_ms"])
    return out


def _load_idem_events(r: redis.Redis, stream: str, kid: str,
                      idem: str) -> list[dict]:
    """按业务号索引把这笔业务的全部事件（reserve/confirm/release/reversal）
    取回来。一个业务号至多一条 confirm、一条 reversal。"""
    zkey = keys.ledger_idem_key(kid, idem)
    ids = r.zrange(zkey, 0, -1)
    if not ids:
        return []
    pipe = r.pipeline(transaction=False)
    for i in ids:
        pipe.xrange(stream, min=i, max=i, count=1)
    out = []
    for rows in pipe.execute():
        if rows:
            out.append({"eid": rows[0][0], "f": rows[0][1]})
    return out


def build_statement(r: redis.Redis, kid: str, cfg: dict, *,
                    start: float, end: float,
                    caller: Optional[str] = None) -> dict:
    """拉 [start, end) 的对账单并做净真用掉对账（冲正后的口径）。

    cfg 为控制面读到的密钥配置 dict（停用密钥照常出账）；不存在时给空 dict，
    此时只剩流水可统计，窗口计数侧对账标记为不可行。

    冲正口径（硬要求）：
      * 冲正单独列：totals.reversed 是这段【发生】的冲正合计（冲正自己那笔
        流水落在哪段就算哪段），reversals[] 给逐笔明细；
      * 真用掉按净额：totals.confirmed = confirmed_gross −
        reversed_of_confirmed_in_range，绝不还按冲正前的数；更早调成、这段
        才冲回的计入 reversed_carried，单列但不冲减本区间净真用掉；
      * 对账两侧（流水侧 / win 桶计数侧）都按冲正后净额。
    """
    start_ms = _to_ms(start, "start")
    end_ms = _to_ms(end, "end")
    if start_ms is None or end_ms is None:
        raise ValueError("start and end are required")
    if start_ms >= end_ms:
        raise ValueError("start must be earlier than end")

    now = now_ms(r)
    stream = keys.ledger_stream_key(kid)

    # ── 按占用单归并，而不是逐笔流水累加 ──
    # 一笔单在流水里可能有 reserve/confirm/release/reversal 若干事件。归并
    # 只看结局（confirm 含迟到确认盖过早先 timeout release）：
    #   有 confirm → 只算真用掉毛额；只有 release → 只算退回；都没有 → 还占着。
    # 冲正不改写结局，不在归并里处理，另按冲正事件单独列。
    units: dict[str, dict] = {}

    def ingest(rid: str, fields: dict) -> None:
        u = units.get(rid)
        if u is None:
            u = {"cost": 1, "pool": "shared", "caller": "",
                 "reserved_at": 0, "confirmed": False, "late": False,
                 "released": False, "reasons": set(), "in_range": False,
                 "mirror": False}
            units[rid] = u
        u["cost"] = int(fields.get("cost", "1") or "1")
        u["pool"] = fields.get("pool") or u["pool"]
        u["caller"] = fields.get("caller", "")
        if fields.get("fbmirror") == "1":
            # 顶上镜像：计数实际在备钥，不参与本钥 win 桶对账
            u["mirror"] = True
        kind = fields.get("kind", "")
        if kind == "reserve":
            u["reserved_at"] = int(fields.get("reserve_at", "0") or "0")
        elif kind == "confirm":
            u["confirmed"] = True
            if fields.get("late") == "1":
                u["late"] = True
        elif kind == "release":
            u["released"] = True
            u["reasons"].add(fields.get("reason") or "upstream")

    # 区间内发生的冲正事件（冲正单独列；落在哪段算哪段，与被冲业务无关）
    rev_in_range: list[dict] = []

    # 1) 区间内的流水：结局事件按占用单归并；reserve 决定单子归属哪张账单；
    #    reversal 不参与结局归并，单独收集。
    for eid, f in _iter_stream(r, stream, start_ms, end_ms):
        if caller and f.get("caller", "") != caller:
            continue
        rid = f.get("res", "")
        if f.get("kind") == "reversal":
            rev_in_range.append({"eid": eid, "f": f,
                                 "mirror": f.get("fbmirror") == "1"})
            continue
        ingest(rid, f)
        if f.get("kind") == "reserve":
            units[rid]["in_range"] = True

    # 2) 向后多看一个宽限窗，只为补齐“区间结尾刚占、结局落在区间外”的单的
    #    最终结局。只补区间内还【没有结局】的单（只有 reserve）：
    #      * 已在区间内终结的单（含 timeout release）以区间内结局为准，
    #        绝不用区间外之后发生的事件翻案——历史时点的账不能被未来改写；
    #      * pending 单调向终态（结局之后不会再变），补到 confirm/release
    #        即是最终结局，迟到 confirm 也因此能盖过它之前的 timeout release。
    look_end = min(end_ms + SETTLE_LOOKAHEAD_MS, now)
    if look_end > end_ms:
        for eid, f in _iter_stream(r, stream, end_ms, look_end):
            if caller and f.get("caller", "") != caller:
                continue
            rid = f.get("res", "")
            if f.get("kind") == "reversal":
                continue
            u = units.get(rid)
            if u is not None and not u["confirmed"] and not u["released"]:
                ingest(rid, f)

    # 3) 给区间内每笔冲正挂上它冲的那笔确认（确认可能落在区间之前），
    #    判定“冲的是不是本区间调成的业务”（本区间净真用掉只减这部分）。
    idem_events: dict[str, list[dict]] = {}

    def confirm_for(evs: list[dict]):
        for ev in evs:
            if ev["f"].get("kind") == "confirm":
                return ev
        return None

    for item in rev_in_range:
        idem = item["f"].get("idem", "")
        evs = idem_events.get(idem)
        if evs is None:
            evs = _load_idem_events(r, stream, kid, idem)
            idem_events[idem] = evs
        item["confirm"] = confirm_for(evs)
        rid = item["f"].get("res", "")
        u = units.get(rid)
        item["confirmed_in_range"] = bool(u and u["in_range"] and u["confirmed"])

    # 顶上镜像 reversal（实际计数在备钥）与本钥原生 reversal 分开归集
    rev_mirror = sum(
        int(i["f"].get("cost", "1") or "1") for i in rev_in_range if i["mirror"])
    rev_native = [i for i in rev_in_range if not i["mirror"]]
    rev_total = sum(int(i["f"].get("cost", "1") or "1") for i in rev_native)
    rev_in_range_confirmed = sum(
        int(i["f"].get("cost", "1") or "1") for i in rev_native
        if i["confirmed_in_range"])
    rev_carried = rev_total - rev_in_range_confirmed

    totals = {
        "reserved": 0,          # 这段占过的（毛额：确认毛额 + 退回 + 还占着，互斥合计）
        "confirmed": 0,         # 净真用掉 = confirmed_gross − 本区间调成且被冲回
        "confirmed_gross": 0,   # 真用掉毛额（最终调成的，含迟到确认，冲正前）
        "confirmed_late": 0,    #   其中：先超时退回、后迟到调成的
        "reversed": rev_total,  # 这段发生的冲正合计（单独列，冲正自己也算一笔）
        "reversed_of_confirmed_in_range": rev_in_range_confirmed,
        "reversed_carried": rev_carried,  #   其中：更早调成、这段才冲回的
        "released": 0,          # 退回（最终没调成的；已调成的永不进这里）
        "released_upstream": 0,
        "released_timeout": 0,
        "held_pending": 0,      # 还占着没回音的（区间内占、拉账单时仍未结局）
        # 主备顶上：下面四项是“备钥替主钥”的镜像口径（计数实际在备钥），
        # 独立列出，绝不并进本钥自己的 win 桶对账
        "failover_confirmed_gross": 0,
        "failover_confirmed": 0,
        "failover_reversed": 0,
        "failover_released": 0,
        "failover_reserved": 0,
        "failover_held_pending": 0,
    }
    by_pool: dict[str, dict] = {}

    def pool_bucket(pool: str) -> dict:
        return by_pool.setdefault(pool, {
            "reserved": 0, "confirmed_gross": 0, "reversed": 0,
            "confirmed": 0, "released": 0, "held": 0})

    for rid, u in units.items():
        if not u["in_range"]:
            continue
        c = u["cost"]
        if u["mirror"]:
            # 顶上镜像单：只进“备钥替主钥”的单列，不进本钥自身的各池合计
            totals["failover_reserved"] += c
            if u["confirmed"]:
                totals["failover_confirmed_gross"] += c
                if u["late"]:
                    totals["confirmed_late"] += c
            elif u["released"]:
                totals["failover_released"] += c
            else:
                totals["failover_held_pending"] += c
            continue
        b = pool_bucket(u["pool"])
        totals["reserved"] += c
        b["reserved"] += c
        if u["confirmed"]:
            # 最终调成：只算真用掉毛额；即使流水里早先有 release(timeout)，
            # 也绝不再算进退回。
            totals["confirmed_gross"] += c
            b["confirmed_gross"] += c
            if u["late"]:
                totals["confirmed_late"] += c
        elif u["released"]:
            # 最终没调成：只算退回（最终结局是 release，再细分原因）
            totals["released"] += c
            b["released"] += c
            if "timeout" in u["reasons"]:
                totals["released_timeout"] += c
            else:
                totals["released_upstream"] += c
        else:
            # 宽限窗内仍没看到结局：算还占着（held_open 另有明细）
            totals["held_pending"] += c
            b["held"] += c

    # 镜像 reversal 只减“备钥顶上”的单列，不动本钥自身净额
    totals["failover_reversed"] = rev_mirror
    totals["failover_confirmed"] = (
        totals["failover_confirmed_gross"] - rev_mirror)

    totals["confirmed"] = totals["confirmed_gross"] - rev_in_range_confirmed

    # 池维度：冲正按发生段计 reversed；池净额只减“本池本区间调成且本区间冲回”
    for pool in by_pool:
        b = by_pool[pool]
        pool_rev = 0
        pool_rev_in_confirmed = 0
        for item in rev_native:
            if (item["f"].get("pool") or "shared") != pool:
                continue
            cc = int(item["f"].get("cost", "1") or "1")
            pool_rev += cc
            if item["confirmed_in_range"]:
                pool_rev_in_confirmed += cc
        b["reversed"] = pool_rev
        b["confirmed"] = b["confirmed_gross"] - pool_rev_in_confirmed

    held = _held_open(r, kid, now, start_ms, end_ms)
    if caller:
        held = [h for h in held if h["caller"] == caller]
    held_in_range = [h for h in held if h["in_range"]]
    held_before = [h for h in held if not h["in_range"]]
    held_cost = sum(h["cost"] for h in held)
    held_in_range_cost = sum(h["cost"] for h in held_in_range)

    # 冲正逐笔明细（按发生时间升序）：写明冲哪个业务号、冲多少、冲的是不是
    # 本区间调成的那笔
    reversals_out = []
    for item in sorted(rev_in_range, key=lambda x: x["eid"]):
        f = item["f"]
        conf = item["confirm"]
        entry = {
            "id": item["eid"],
            "ts_ms": _eid_ms(item["eid"]),
            "idem_key": f.get("idem", ""),
            "caller": f.get("caller", ""),
            "pool": f.get("pool", ""),
            "amount": int(f.get("cost", "1") or "1"),
            "confirmed_cost": (
                int(conf["f"].get("cost", "1") or "1") if conf
                else int(f.get("of", "0") or "0")),
            "confirmed_in_range": item["confirmed_in_range"],
            "note": f.get("note", ""),
        }
        if item["mirror"]:
            # 顶上备钥的冲正镜像：计数实际退在备钥（fb_key），主钥只留可见事实
            entry["failover_mirror"] = True
            entry["served_by_key_id"] = f.get("fb_key", "")
        reversals_out.append(entry)

    # ── 真用掉对账：流水侧净额 vs 余量侧 win/swin 固定桶计数 ──
    # 两边都按冲正后净额：流水侧把每笔冲正挂回它【原确认落的那个桶】扣减；
    # 计数侧 reverse.lua 也是从原确认桶 INCRBY -amount（桶已老化的不动）。
    win_seconds = int(cfg.get(keys.F_WINDOW_SECONDS, 0) or 0)
    reconciliation = {
        "feasible": False,
        "dimension": "window",
        "window_seconds": win_seconds or None,
        "ledger_confirmed": totals["confirmed"],
        "ledger_confirmed_gross": totals["confirmed_gross"],
        "ledger_reversed": rev_in_range_confirmed,
        "counter_consumed": None,
        "aligned_start_ms": None,
        "aligned_end_ms": None,
        "matched": None,
        "difference": None,
        "note": "",
        "pools": [],
    }
    if win_seconds > 0:
        win_ms = win_seconds * 1000
        a_start_idx = start_ms // win_ms
        a_end_idx = (end_ms + win_ms - 1) // win_ms
        idxs = list(range(a_start_idx, a_end_idx))
        aligned_start_ms = a_start_idx * win_ms
        aligned_end_ms = a_end_idx * win_ms

        # 对齐区间内逐桶统计：confirm 加、reversal 按其原确认桶减。冲正的
        # 原确认若在对齐区间之外（区间扫描看不到那条 confirm），用业务号索引
        # 取该 confirm 的 entry id 定位桶；原确认桶不在对齐区间则计数侧那个
        # 桶也不在本次求和里，自然跳过。
        ledger_gross_aligned = 0
        ledger_rev_aligned = 0
        for eid, f in _iter_stream(r, stream, aligned_start_ms, aligned_end_ms):
            if caller and f.get("caller", "") != caller:
                continue
            if f.get("fbmirror") == "1":
                # 顶上镜像的计数在备钥 win 桶里，绝不并进本钥 win 对账
                continue
            kind = f.get("kind", "")
            c = int(f.get("cost", "1") or "1")
            if kind == "confirm":
                ledger_gross_aligned += c
            elif kind == "reversal":
                idem = f.get("idem", "")
                evs = idem_events.get(idem)
                if evs is None:
                    evs = _load_idem_events(r, stream, kid, idem)
                    idem_events[idem] = evs
                conf = confirm_for(evs)
                if conf is not None:
                    conf_idx = _eid_ms(conf["eid"]) // win_ms
                    if a_start_idx <= conf_idx < a_end_idx:
                        ledger_rev_aligned += c
        ledger_net_aligned = ledger_gross_aligned - ledger_rev_aligned

        # aligned 区间确认毛额（含迟到标记），用于对不上时给原因
        late_aligned = 0
        for eid, f in _iter_stream(r, stream, aligned_start_ms, aligned_end_ms):
            if caller and f.get("caller", "") != caller:
                continue
            if f.get("fbmirror") == "1":
                continue
            if f.get("kind") == "confirm" and f.get("late") == "1":
                late_aligned += int(f.get("cost", "1") or "1")

        shares = r.hgetall(keys.shares_key(kid))
        counter_pools = []
        notes: list[str] = []

        # 计数桶只在确认时写、TTL=2 个窗口+5s（settle.lua）。只要对齐区间的
        # 最早一个桶还落在保留期内，缺失的桶一定是“从没写过(=0)”而不是
        # “写过后过期”，计数侧才可对账；更早的历史只剩流水这一份事实。
        # 冲正只对仍存在的桶 INCRBY 负数、不新建桶，规则相同。
        retained_idx = (now - (2 * win_ms + 5000)) // win_ms
        counters_retained = a_start_idx >= retained_idx
        if not counters_retained:
            notes.append(
                "aligned range starts beyond the 2-window counter retention "
                "(buckets expired); counter_consumed only covers retained "
                "buckets and balance is not conclusive — ledger remains the "
                "source of truth for older history")

        unattributable = caller is not None and caller not in shares
        if unattributable:
            notes.append(
                "caller has no dedicated share pool; un-named callers share "
                "the public pool counter and cannot be split by X-Client-Id "
                "— counter side is not attributable to this caller")

        if caller is not None and caller in shares:
            spec = _pool_window_consumed(r, kid, win_seconds, idxs, [caller])
            counter_pools.append({"pool": "share", "caller": caller, **spec})
            counter_total = spec["total"]
        else:
            shared_spec = _pool_window_consumed(r, kid, win_seconds, idxs, None)
            counter_pools.append({"pool": "shared", **shared_spec})
            counter_total = shared_spec["total"]
            share_callers = sorted(shares.keys())
            if share_callers:
                share_spec = _pool_window_consumed(
                    r, kid, win_seconds, idxs, share_callers)
                counter_pools.append({"pool": "share",
                                      "callers": share_callers,
                                      **share_spec})
                counter_total += share_spec["total"]

        feasible = counters_retained and not unattributable
        if feasible and ledger_net_aligned != counter_total:
            late_note = ""
            if late_aligned:
                late_note = (f"; {late_aligned} late confirmation(s) were "
                             "counted in a later window bucket than reserved")
            notes.append("ledger net confirmed and window counters disagree"
                         + late_note)

        reconciliation.update({
            "feasible": feasible,
            "ledger_confirmed": ledger_net_aligned,
            "ledger_confirmed_gross": ledger_gross_aligned,
            "ledger_reversed": ledger_rev_aligned,
            "counter_consumed": counter_total,
            "aligned_start_ms": aligned_start_ms,
            "aligned_end_ms": aligned_end_ms,
            "matched": feasible and ledger_net_aligned == counter_total,
            "difference": (counter_total - ledger_net_aligned) if feasible else None,
            "note": " ".join(notes),
            "pools": counter_pools,
        })

    return {
        "key_id": kid,
        "state": ("revoked" if cfg.get(keys.F_REVOKED) == "1"
                  else "active" if cfg else "unknown"),
        "generated_at_ms": now,
        "range": {"start_ms": start_ms, "end_ms": end_ms},
        "filters": {"caller": caller or None},
        # 归并到的占用单笔数（一笔单可能对应多笔流水事件）
        "reservations_matched": sum(
            1 for u in units.values() if u["in_range"] and not u["mirror"]),
        "settle_lookahead_ms": SETTLE_LOOKAHEAD_MS,
        "totals": totals,
        # 主备顶上：备钥替主钥发生的账（镜像口径，计数实际在备钥，单列不合入
        # 本钥自身 win 对账）。净真用掉 = 顶上毛额 − 顶上冲正镜像
        "failover": {
            "reserved": totals["failover_reserved"],
            "confirmed_gross": totals["failover_confirmed_gross"],
            "confirmed": totals["failover_confirmed"],
            "reversed": totals["failover_reversed"],
            "released": totals["failover_released"],
            "held_pending": totals["failover_held_pending"],
        },
        "by_pool": by_pool,
        "reversals": reversals_out,
        "held_open": {
            "total_holds": len(held),
            "total_cost": held_cost,
            "reserved_in_range": len(held_in_range),
            "reserved_in_range_cost": held_in_range_cost,
            "carried_from_before_range": len(held_before),
            "items_in_range": held_in_range,
            "items_carried": held_before,
        },
        "reconciliation": reconciliation,
    }
