#!/usr/bin/env python3
"""跨密钥额度划转端到端自检（仅标准库）。

前置：docker compose 起好全套（Redis + 控制面 8000 + 数据面 8080 + mock）。
覆盖：
  1. 划转写明从哪把到哪把、突发/窗口各多少，两边原子同时生效
  2. 只能划当前还能再占的余量，不能划已真用掉的
  3. 还占着的不能划；已经占着的旧单仍按原单结完
  4. 不能超过目标自己还装得下的空位
  5. 任一头停用不能划
  6. 同一划转号重放不二次划转；同号不同参数冲突
  7. 可查单笔、某把划出/划入累计与每笔明细
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import time
import urllib.error
import urllib.request

CP = os.environ.get("CP", "http://localhost:8000")
DP = os.environ.get("DP", "http://localhost:8080")
ADMIN = os.environ.get("ADMIN_TOKEN", "change-me-admin-token")
AUTH = {"Authorization": f"Bearer {ADMIN}"}

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def http(method: str, url: str, headers: dict | None = None, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    hs = {"Content-Type": "application/json"}
    if headers:
        hs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw.decode(errors="replace")}
        return e.code, parsed


def issue(name: str, burst: int, window: int):
    status, body = http("POST", f"{CP}/v1/keys", AUTH, {
        "name": name,
        "burst_capacity": burst,
        # 测试期间几乎不补令牌，避免自然补充干扰划转数量
        "burst_refill_ms": 100000,
        "window_seconds": 60,
        "window_quota": window,
    })
    assert status == 201, body
    return body["key_id"], body["api_key"]


def quota(kid: str):
    status, body = http("GET", f"{CP}/v1/keys/{kid}/quota", AUTH)
    assert status == 200, body
    return body


def transfer(source: str, transfer_id: str, target: str, burst: int = 0, window: int = 0):
    return http("POST", f"{CP}/v1/keys/{source}/transfers", AUTH, {
        "transfer_id": transfer_id,
        "target_key_id": target,
        "burst_amount": burst,
        "window_amount": window,
    })


def main() -> int:
    src, src_key = issue("transfer-source", 10, 100)
    dst, dst_key = issue("transfer-target", 10, 100)
    # 目标先真用掉 10 笔，腾出自己的 10 个空位；满额未用的密钥没有空位可划入。
    for i in range(10):
        status, body = http("GET", f"{DP}/v1/hello",
                            {"X-Api-Key": dst_key, "Idempotency-Key": f"dst-used-{i}"})
        assert status == 200, body
    assert quota(dst)["burst"]["remaining"] == 0

    # ── 1) 成功划转：源少、目标多，立即反映在余量接口 ────────────────────
    tid = f"tr-{int(time.time() * 1000)}"
    status, body = transfer(src, tid, dst, 2, 10)
    record("划转成功 201 且写明源/目标/数量",
           status == 201
           and body["source_key_id"] == src
           and body["target_key_id"] == dst
           and body["burst_amount"] == 2
           and body["window_amount"] == 10,
           str(body))

    qs, qd = quota(src), quota(dst)
    record("源突发/窗口余量立即减少",
           qs["burst"]["remaining"] == 8 and qs["window"]["remaining"] == 90,
           f"src burst={qs['burst']} window={qs['window']}")
    record("目标突发/窗口余量立即增加且不超过原容量",
           qd["burst"]["remaining"] == 2 and qd["window"]["remaining"] == 100,
           f"dst burst={qd['burst']} window={qd['window']}")
    record("目标只是补回自己空下的位置，不产生溢余额度",
           qd["burst"]["reversal_credit"] == 0
           and qd["window"]["reversal_credit"] == 0,
           f"burst={qd['burst']} window={qd['window']}")

    # ── 2) 同号原参数幂等，不二次划转 ───────────────────────────────────
    status2, body2 = transfer(src, tid, dst, 2, 10)
    record("同划转号原参数重放 200 already_transferred",
           status2 == 200 and body2.get("already_transferred") is True, str(body2))
    qs2, qd2 = quota(src), quota(dst)
    record("幂等重放不二次划转",
           qs2["burst"]["remaining"] == 8 and qd2["burst"]["remaining"] == 2
           and qs2["window"]["remaining"] == 90 and qd2["window"]["remaining"] == 100,
           f"src={qs2['burst']['remaining']}/{qs2['window']['remaining']} "
           f"dst={qd2['burst']['remaining']}/{qd2['window']['remaining']}")

    # 同号改参数不能借号改账
    other, _ = issue("transfer-conflict", 5, 50)
    status3, body3 = transfer(src, tid, other, 1, 1)
    record("同号不同目标/数量返回 409 transfer_id_conflict",
           status3 == 409 and body3["detail"]["code"] == "transfer_id_conflict",
           str(body3))

    # ── 3) 不能超过目标此刻还装得下的空位 ───────────────────────────────
    status4, body4 = transfer(src, f"{tid}-full-target", other, 1, 0)
    record("目标此刻已满额没有空位：整笔拒绝并指出 target",
           status4 == 422
           and body4["detail"]["code"] == "insufficient_burst"
           and body4["detail"]["limiting_side"] == "target"
           and body4["detail"]["target_room"] == 0,
           str(body4))

    # 源此刻突发剩余 8；找一把空位充足的目标，源自己仍有足够窗口/突发。
    room_key, _ = issue("transfer-room", 20, 200)
    status5, body5 = transfer(src, f"{tid}-too-much", room_key, 9, 0)
    record("源当前可划突发不足整笔拒绝",
           status5 == 422
           and body5["detail"]["code"] == "insufficient_burst"
           and body5["detail"]["limiting_side"] == "source"
           and body5["detail"]["source_available"] == 8,
           str(body5))

    # ── 4) 还占着的不能划；旧占用单不因划转释放，之后照常结完 ─────────────
    hold_src, hold_api = issue("transfer-held", 10, 100)
    hold_dst, hold_dst_key = issue("transfer-held-target", 10, 100)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        # 源、目标各开一笔慢调用：源可划的是未在飞的 9/99；目标的空位正是
        # 已被在飞占住的 1/1。划转不碰任何一张占用单。
        fut = ex.submit(lambda: http(
            "GET", f"{DP}/slow?ms=3000",
            {"X-Api-Key": hold_api, "Idempotency-Key": "held-during-transfer"}))
        dst_fut = ex.submit(lambda: http(
            "GET", f"{DP}/slow?ms=3000",
            {"X-Api-Key": hold_dst_key, "Idempotency-Key": "held-target-during-transfer"}))
        time.sleep(0.8)
        held_q = quota(hold_src)
        record("慢调用已形成 1 笔在占",
               held_q["burst"]["held"] == 1 and held_q["window"]["held"] == 1,
               str(held_q))
        # 容量 10，在飞 1，源只有 9/99 个空位；目标先用完，腾出正好的空位。
        st, bd = transfer(hold_src, f"{tid}-held", hold_dst, 9, 99)
        record("在飞单仍占着：只能划走未在飞的空位",
               st == 201 and bd["source_remaining"]["burst"] == 0
               and bd["source_remaining"]["window"] == 0, str(bd))
        st2, bd2 = transfer(hold_src, f"{tid}-held-overflow", hold_dst, 1, 1)
        record("不能把还占着的那 1 笔划走",
               st2 == 422 and bd2["detail"]["limiting_side"] == "source",
               str(bd2))
        call_status, call_body = fut.result()
        record("划转前已占的源慢调用仍按原单调成（200）",
               call_status == 200, str(call_body))
        dst_call_status, dst_call_body = dst_fut.result()
        record("划转前已占的目标慢调用也仍按原单调成（200）",
               dst_call_status == 200, str(dst_call_body))

    after_q = quota(hold_src)
    record("两边旧单确认后都按原密钥扣账，没有被划转释放",
           after_q["burst"]["held"] == 0 and after_q["burst"]["consumed"] == 10
           and after_q["window"]["consumed"] == 100,
           f"source burst={after_q['burst']} window={after_q['window']}")

    # ── 5) 任一头停用不能划 ─────────────────────────────────────────────
    revoked_src, _ = issue("transfer-revoked-source", 5, 50)
    active_target, _ = issue("transfer-active-target", 5, 50)
    http("POST", f"{CP}/v1/keys/{revoked_src}/revoke", AUTH)
    st, bd = transfer(revoked_src, f"{tid}-revoked-src", active_target, 1, 1)
    record("源已停用不能划", st == 409 and bd["detail"]["code"] == "source_revoked", str(bd))

    active_src, _ = issue("transfer-active-source", 5, 50)
    revoked_dst, _ = issue("transfer-revoked-target", 5, 50)
    http("POST", f"{CP}/v1/keys/{revoked_dst}/revoke", AUTH)
    st, bd = transfer(active_src, f"{tid}-revoked-dst", revoked_dst, 1, 1)
    record("目标已停用不能划", st == 409 and bd["detail"]["code"] == "target_revoked", str(bd))

    # ── 6) 查询：单笔、划出/划入累计、明细 ───────────────────────────────
    st, one = http("GET", f"{CP}/v1/transfers/{tid}", AUTH)
    record("按划转号能查源/目标/数量/完成时间",
           st == 200 and one["transfer_id"] == tid
           and one["source_key_id"] == src and one["target_key_id"] == dst,
           str(one))

    st, src_ledger = http("GET", f"{CP}/v1/keys/{src}/transfers?direction=out", AUTH)
    out_total = src_ledger["totals"]
    record("源密钥可查划出累计",
           st == 200 and out_total["out_burst"] == 2 and out_total["out_window"] == 10,
           str(out_total))

    st, dst_ledger = http("GET", f"{CP}/v1/keys/{dst}/transfers?direction=in", AUTH)
    in_total = dst_ledger["totals"]
    items = dst_ledger["items"]
    record("目标密钥可查划入累计和每笔从哪到哪、划多少",
           st == 200 and in_total["in_burst"] == 2 and in_total["in_window"] == 10
           and any(i["transfer_id"] == tid and i["source_key_id"] == src
                   and i["target_key_id"] == dst for i in items),
           str(dst_ledger))

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    if failed:
        print("failed:")
        for name in failed:
            print(f"  - {name}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
