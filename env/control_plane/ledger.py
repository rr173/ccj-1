"""占用流水查询与对账单（控制面，只读）。

流水事实由数据面在 reserve.lua / settle.lua / sweep.lua 内与计数变更原子
写入：每把密钥一条只追加 Stream ``{qk:<kid>}ledger``，按调用方、业务号各挂
一个 ZSET 索引。本模块只做读，不写、不改任何流水——Stream 没有修改单条的
命令，密钥停用、服务重启都不影响已落盘的流水（AOF appendfsync always）。

两个入口：
  * list_events   ：按密钥（必选）+ 调用方 / 业务号 / 时间段翻流水；
  * build_statement：拉一段时间的对账单——这段占过多少、真用掉多少、
                    退回多少、还占着的单列，并把“流水侧真用掉”与
                    “余量计数侧真用掉（win 桶）”对账，对不上两边数都亮出。
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


def _parse_event(eid: str, fields: dict, kid: str) -> dict:
    res_full = fields.get("res", "")
    prefix = f"{{qk:{kid}}}res:"
    res_tail = res_full[len(prefix):] if res_full.startswith(prefix) else res_full
    dash = eid.find("-")
    return {
        "id": eid,
        "ts_ms": int(eid[:dash]) if dash >= 0 else None,
        "kind": fields.get("kind", ""),
        "caller": fields.get("caller", ""),
        "idem_key": fields.get("idem", ""),
        "pool": fields.get("pool", ""),
        "cost": int(fields.get("cost", "1") or "1"),
        "reason": fields.get("reason", ""),
        "late": fields.get("late", "0") == "1",
        "reserved_at_ms": int(fields.get("reserve_at", "0") or "0"),
        "reservation_id": res_tail,
    }


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
    “真用掉”的事实来源，与 quota.lua 读的是同一批计数 key。

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


def build_statement(r: redis.Redis, kid: str, cfg: dict, *,
                    start: float, end: float,
                    caller: Optional[str] = None) -> dict:
    """拉 [start, end) 的对账单并做真用掉对账。

    cfg 为控制面读到的密钥配置 dict（停用密钥照常出账）；不存在时给空 dict，
    此时只剩流水可统计，窗口计数侧对账标记为不可行。
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
    # 一笔单在流水里可能有 1~3 个事件：
    #   reserve；reserve + confirm（调成）；
    #   reserve + release（没调成）；
    #   reserve + release(timeout) + confirm(late=1)（先超时退回、之后迟到调成）。
    # 对账单以单子的【最终结局】归类：
    #   有 confirm（含迟到）  → 只算真用掉，绝不进退回；
    #   只有 release          → 只算退回；
    #   都没有                → 还占着（pending，与 held_open 分列口径一致）。
    # 这样“已调成的不会又算进退回”，三类互斥且完备。
    units: dict[str, dict] = {}

    def ingest(rid: str, fields: dict) -> None:
        u = units.get(rid)
        if u is None:
            u = {"cost": 1, "pool": "shared", "caller": "",
                 "reserved_at": 0, "confirmed": False, "late": False,
                 "released": False, "reasons": set(), "in_range": False}
            units[rid] = u
        u["cost"] = int(fields.get("cost", "1") or "1")
        u["pool"] = fields.get("pool") or u["pool"]
        u["caller"] = fields.get("caller", "")
        kind = fields.get("kind", "")
        if kind == "reserve":
            u["reserved_at"] = int(fields.get("reserve_at", "0") or 0)
        elif kind == "confirm":
            u["confirmed"] = True
            if fields.get("late") == "1":
                u["late"] = True
        elif kind == "release":
            u["released"] = True
            u["reasons"].add(fields.get("reason") or "upstream")

    # 1) 区间内的流水：归属本账单（按 reserve 落在区间内）
    for eid, f in _iter_stream(r, stream, start_ms, end_ms):
        if caller and f.get("caller", "") != caller:
            continue
        rid = f.get("res", "")
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
            u = units.get(rid)
            if u is not None and not u["confirmed"] and not u["released"]:
                ingest(rid, f)

    totals = {
        "reserved": 0,          # 这段占过的（= 确认 + 退回 + 还占着，互斥合计）
        "confirmed": 0,         # 真用掉（最终调成的，含迟到确认）
        "confirmed_late": 0,    #   其中：先超时退回、后迟到调成的
        "released": 0,          # 退回（最终没调成的；已调成的永不进这里）
        "released_upstream": 0,
        "released_timeout": 0,
        "held_pending": 0,      # 还占着没回音的（区间内占、拉账单时仍未结局）
    }
    by_pool: dict[str, dict] = {}

    def pool_bucket(pool: str) -> dict:
        return by_pool.setdefault(pool, {
            "reserved": 0, "confirmed": 0, "released": 0, "held": 0})

    for rid, u in units.items():
        if not u["in_range"]:
            continue
        b = pool_bucket(u["pool"])
        c = u["cost"]
        totals["reserved"] += c
        b["reserved"] += c
        if u["confirmed"]:
            # 最终调成：只算真用掉；即使流水里早先有 release(timeout)，
            # 也绝不再算进退回。
            totals["confirmed"] += c
            b["confirmed"] += c
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

    held = _held_open(r, kid, now, start_ms, end_ms)
    if caller:
        held = [h for h in held if h["caller"] == caller]
    held_in_range = [h for h in held if h["in_range"]]
    held_before = [h for h in held if not h["in_range"]]
    held_cost = sum(h["cost"] for h in held)
    held_in_range_cost = sum(h["cost"] for h in held_in_range)

    # ── 真用掉对账：流水侧 confirm vs 余量侧 win/swin 固定桶计数 ──
    # 计数只按固定桶存活，因此把对账区间对齐到桶边界，两边同口径。
    win_seconds = int(cfg.get(keys.F_WINDOW_SECONDS, 0) or 0)
    reconciliation = {
        "feasible": False,
        "dimension": "window",
        "window_seconds": win_seconds or None,
        "ledger_confirmed": totals["confirmed"],
        "ledger_confirmed_late": totals["confirmed_late"],
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

        # 对齐区间内的流水侧 confirm（可能与请求区间略有差异，单独算）
        ledger_aligned = 0
        late_aligned = 0
        for eid, f in _iter_stream(r, stream, aligned_start_ms, aligned_end_ms):
            if caller and f.get("caller", "") != caller:
                continue
            if f.get("kind") == "confirm":
                ledger_aligned += int(f.get("cost", "1") or "1")
                if f.get("late") == "1":
                    late_aligned += int(f.get("cost", "1") or "1")

        shares = r.hgetall(keys.shares_key(kid))
        counter_pools = []
        notes: list[str] = []

        # 计数桶只在确认时写、TTL=2 个窗口+5s（settle.lua）。只要对齐区间的
        # 最早一个桶还落在保留期内，缺失的桶一定是“从没写过(=0)”而不是
        # “写过后过期”，计数侧才可对账；更早的历史只剩流水这一份事实。
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
        if feasible and ledger_aligned != counter_total:
            late_note = ""
            if late_aligned:
                late_note = (f"; {late_aligned} late confirmation(s) were "
                             "counted in a later window bucket than reserved")
            notes.append("ledger confirmed and window counters disagree"
                         + late_note)

        reconciliation.update({
            "feasible": feasible,
            "ledger_confirmed": ledger_aligned,
            "ledger_confirmed_late": late_aligned,
            "counter_consumed": counter_total,
            "aligned_start_ms": aligned_start_ms,
            "aligned_end_ms": aligned_end_ms,
            "matched": feasible and ledger_aligned == counter_total,
            "difference": (counter_total - ledger_aligned) if feasible else None,
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
        "reservations_matched": sum(1 for u in units.values() if u["in_range"]),
        "settle_lookahead_ms": SETTLE_LOOKAHEAD_MS,
        "totals": totals,
        "by_pool": by_pool,
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
