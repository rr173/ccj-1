#!/usr/bin/env python3
"""主备顶上 端到端自检（仅标准库）。

前置：docker compose 起好全套，或本地起 Redis + 控制面(8000) + 数据面(8080)
+ mock 上游（数据面 RESERVATION_TTL_SECONDS 调小便于测超时）。

覆盖（对应需求逐条）：
  绑定 1:1（不能绑自己 / 主备都须有效 / 一主一备 / 备不能兼主）
  平时只占主钥；主钥占不住同一笔立刻改占备钥（不两头咬）
  备钥有自己的突发/窗口；备钥也满才报还要等多久
  主钥恢复后新请求回主钥
  查询：绑了谁 / 此刻备钥在顶 / 连续占不住次数 / 备钥真用掉
  解绑：在飞的按占到的走完，新来的不再占解绑备钥
  主钥停了：不自动顶上；备钥停了：只剩主钥
  没绑备钥：只占自己
  同一业务号：永远走第一次占到的那把
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

CP = os.environ.get("CP", "http://localhost:8000")
DP = os.environ.get("DP", "http://localhost:8080")
ADMIN = os.environ.get("ADMIN_TOKEN", "change-me-admin-token")

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, PASS if ok else FAIL, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def http(method: str, url: str, headers: dict | None = None, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, dict(resp.headers), json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw.decode(errors="replace")}
        return e.code, dict(e.headers or {}), parsed


def issue_key(name: str, burst: int, refill_ms: int, win_s: int, win_q: int):
    status, _, body = http("POST", f"{CP}/v1/keys",
                           {"Authorization": f"Bearer {ADMIN}"},
                           {"name": name, "burst_capacity": burst,
                            "burst_refill_ms": refill_ms,
                            "window_seconds": win_s, "window_quota": win_q})
    assert status == 201, body
    return body["key_id"], body["api_key"]


def bind(pk, bk):
    return http("PUT", f"{CP}/v1/keys/{pk}/failover/{bk}",
                {"Authorization": f"Bearer {ADMIN}"})


def unbind(pk, bk):
    return http("DELETE", f"{CP}/v1/keys/{pk}/failover/{bk}",
                {"Authorization": f"Bearer {ADMIN}"})


def fstatus(pk):
    _, _, body = http("GET", f"{CP}/v1/keys/{pk}/failover",
                      {"Authorization": f"Bearer {ADMIN}"})
    return body


def revoke(k):
    return http("POST", f"{CP}/v1/keys/{k}/revoke",
                {"Authorization": f"Bearer {ADMIN}"})


def quota(k):
    _, _, body = http("GET", f"{CP}/v1/keys/{k}/quota",
                      {"Authorization": f"Bearer {ADMIN}"})
    return body


def call(api_key, idem=None, path="/v1/hello"):
    headers = {"X-Api-Key": api_key}
    if idem:
        headers["Idempotency-Key"] = idem
    return http("GET", f"{DP}{path}", headers)


def main() -> int:
    # 主钥：突发只有 1，2 秒补一个 → 第 2 笔立刻占不住；备钥：突发 2
    pk, pa = issue_key("primary", 1, 2000, 60, 100)
    bk, ba = issue_key("backup", 2, 2000, 60, 100)
    other_k, other_a = issue_key("other", 1, 2000, 60, 100)

    # ── 绑定规则 ──────────────────────────────────────────────────────
    s, _, body = bind(pk, pk)
    record("不能拿自己当备钥", s == 409, str(body))
    s, _, body = bind(pk, bk)
    record("主备绑定成功", s == 200 and body.get("bound") is True
           and body.get("backup_key_id") == bk, str(body))
    s, _, body = bind(other_k, bk)
    record("一把备钥不能同时顶两把主钥", s == 409
           and body.get("detail", {}).get("code") == "backup_taken", str(body))
    # 备钥自己再绑备钥（链式顶上）不允许
    b2k, b2a = issue_key("backup2", 2, 2000, 60, 100)
    s, _, body = bind(bk, b2k)
    record("备钥自己不能再绑备钥(链式)", s == 409
           and body.get("detail", {}).get("code") == "failover_chain", str(body))
    s, _, body = bind(pk, b2k)
    record("一把主钥只能绑一把备钥", s == 409
           and body.get("detail", {}).get("code") == "already_bound", str(body))

    st = fstatus(pk)
    record("查得到绑的是哪一把", st.get("bound") is True
           and st.get("backup_key_id") == bk and st.get("backup_state") == "active",
           str(st))
    record("初始：没在顶/0 连占/0 真用掉",
           st["failover_active"] is False and st["on_backup_held"] == 0
           and st["master_block_streak"] == 0 and st["backup_consumed"] == 0, str(st))

    # ── 平时只占主钥 ──────────────────────────────────────────────────
    s, h, body = call(pa, "biz-1")
    record("第 1 笔走主钥成功", s == 200
           and h.get("X-Quota-Served-By") == "primary", f"status={s}")
    qm, qb = quota(pk), quota(bk)
    record("第 1 笔算在主钥头上", qm["burst"]["consumed"] == 1
           and qb["burst"]["consumed"] == 0,
           f"m={qm['burst']['consumed']} b={qb['burst']['consumed']}")

    # ── 主钥占不住，同一笔立刻改占备钥 ─────────────────────────────────
    s, h, body = call(pa, "biz-2")
    record("第 2 笔顶上备钥成功", s == 200
           and h.get("X-Quota-Served-By") == "backup"
           and h.get("X-Failover") == "1", f"status={s}")
    qm, qb = quota(pk), quota(bk)
    record("顶这笔算在备钥头上（主钥没被两头咬）",
           qm["burst"]["consumed"] == 1 and qb["burst"]["consumed"] == 1,
           f"m={qm['burst']['consumed']} b={qb['burst']['consumed']}")
    time.sleep(0.2)
    st = fstatus(pk)
    record("备钥真用掉=1、连着占不住=1、无在顶",
           st["backup_consumed"] == 1 and st["master_block_streak"] == 1
           and st["on_backup_held"] == 0 and st["failover_active"] is False, str(st))

    # ── 备钥也有突发，备钥也满了才告诉还要等多久 ───────────────────────
    s, h, body = call(pa, "biz-3")
    record("第 3 笔还能顶上备钥(备钥突发=2)", s == 200
           and h.get("X-Quota-Served-By") == "backup", f"status={s}")
    s, h, body = call(pa, "biz-4")
    record("备钥也满：429 且告知还要等多久", s == 429
           and body["error"].get("retry_after_ms", 0) > 0
           and body["error"].get("failover") is True
           and h.get("Retry-After"), str(body))
    st = fstatus(pk)
    record("连续占不住继续累计", st["master_block_streak"] >= 2, str(st))

    # ── 主钥又能占住以后，新来的回到主钥；streak 归零 ─────────────────
    time.sleep(2.3)  # 等主钥补回 1 个令牌
    s, h, body = call(pa, "biz-5")
    record("主钥恢复：新请求回主钥", s == 200
           and h.get("X-Quota-Served-By") == "primary", f"status={s}")
    st = fstatus(pk)
    record("主钥新占成后 streak 归零", st["master_block_streak"] == 0, str(st))

    # ── 同一业务号永远走第一次占到的那把 ──────────────────────────────
    # 现在主钥又空了；biz-3 第一次是在备钥上调成的，重放必须回原结论、
    # 不碰主钥
    s, _, body = call(pa, "biz-3")
    record("同业务号重放：原结论 409 confirmed", s == 409
           and body["error"]["code"] == "idempotent_replay_confirmed", str(body))
    # 用 /slow 占一笔在备钥上的在飞单（主钥此刻应已满）
    # 先把主钥占满：
    time.sleep(2.2)
    s1, _, _ = call(pa, "fill-primary")          # 占掉主钥唯一令牌并确认
    assert s1 == 200, s1
    # 用慢于检查、快于回音时限的调用占一笔在飞单（4s < lease 5s），
    # 调用还没回来时并发查“此刻备钥在顶”
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(lambda: call(pa, "route-hold", path="/uniqslow?ms=4000"))
        time.sleep(1.0)
        st = fstatus(pk)
        record("此刻查得出备钥在顶(failover_active)",
               st["failover_active"] is True and st["on_backup_held"] >= 1,
               str(st))
        sh = fut.result(timeout=20)
        record("在飞的顶上单笔占在备钥", sh[0] == 200
               and sh[1].get("X-Quota-Served-By") == "backup",
               f"status={sh[0]}")
    st = fstatus(pk)
    record("慢调用调成后在顶归零、真用掉累计",
           st["on_backup_held"] == 0 and st["backup_consumed"] >= 3, str(st))

    # ── 没绑备钥的密钥还是只占自己 ────────────────────────────────────
    s, _, _ = call(other_a, "o-1")
    record("没绑备钥：第 1 笔正常", s == 200)
    s, _, body = call(other_a, "o-2")
    record("没绑备钥：满了就是普通 429、不顶上", s == 429
           and body["error"]["code"] == "burst_limited"
           and "failover" not in body["error"], str(body))

    # ── 解绑：新来的不再占备钥；在飞/已路由的按原把走完 ───────────────
    s, _, body = unbind(pk, bk)
    record("解绑成功", s == 200 and body.get("bound") is False, str(body))
    record("解绑后查询 bound=false", fstatus(pk).get("bound") is False)
    # 主钥满（fill-primary 已确认占掉，令牌未补回时窗口内再试）——直接强测：
    # 先再占满主钥
    time.sleep(2.2)
    s, h, _ = call(pa, "after-unbind-primary")
    assert s == 200 and h.get("X-Quota-Served-By") == "primary", s
    s, _, body = call(pa, "after-unbind-new")
    record("解绑后新来的不再顶上备钥", s == 429
           and body["error"]["code"] == "burst_limited"
           and "failover" not in body["error"], str(body))
    # 已在备钥占过的同业务号，重放仍回备钥那张原单
    s, _, body = call(pa, "route-hold")
    record("解绑后老业务号仍走第一次占到的备钥(409 原结论)", s == 409
           and body["error"]["code"] == "idempotent_replay_confirmed", str(body))

    # ── 重新绑定：新关系生效 ──────────────────────────────────────────
    s, _, _ = bind(pk, bk)
    record("换绑同一把(解绑后)成功", s == 200, str(s))
    st = fstatus(pk)
    record("重新绑定后历史统计仍在(真用掉累计不归零)",
           st["backup_consumed"] >= 2, str(st))

    # ── 主钥停了：备钥不自动接新的；在飞的仍按备钥走完 ────────────────
    time.sleep(2.2)
    s, _, _ = revoke(pk)
    assert s == 200
    s, _, body = call(pa, "after-revoke")
    record("主钥停用：不顶上、直接 key_revoked 403", s == 403
           and body["error"]["code"] == "key_revoked", str(body))
    # 备钥自己仍然有效、可独立使用（它还是把正常密钥）
    s, h, _ = call(ba, "bk-direct-1")
    record("备钥作为独立密钥照常可用", s == 200)

    # ── 备钥停了：只剩主钥自己（另起一对干净的测）─────────────────────
    p2, p2a = issue_key("primary2", 1, 5000, 60, 100)
    b3, b3a = issue_key("backup3", 2, 5000, 60, 100)
    s, _, _ = bind(p2, b3)
    assert s == 200, s
    s, _, _ = call(p2a, "p2-1")
    assert s == 200  # 占掉主钥
    s, h, _ = call(p2a, "p2-2")
    record("停用前顶上正常", s == 200 and h.get("X-Quota-Served-By") == "backup",
           f"status={s}")
    s, _, _ = revoke(b3)
    assert s == 200
    s, _, body = call(p2a, "p2-3")
    record("备钥停用：不再顶上，回报主钥自己的 429", s == 429
           and "failover" not in body["error"], str(body))
    st = fstatus(p2)
    record("备钥停用状态查得到 backup_state=revoked",
           st["backup_state"] == "revoked", str(st))

    # ── 匿名调用也支持顶上；超时退回后在顶计数归零 ─────────────────────
    p4, p4a = issue_key("primary4", 1, 5000, 60, 100)
    b4, b4a = issue_key("backup4", 2, 5000, 60, 100)
    s, _, _ = bind(p4, b4)
    assert s == 200
    s, _, _ = call(p4a)               # 占掉主钥（匿名）
    assert s == 200
    # 用超过回音时限的慢调用占在备钥上，等它超时自动退回
    s, h, _ = call(p4a, path="/uniqslow?ms=8000")
    record("匿名顶上：占成后慢上游最终 502/超时前占用已登记",
           s in (200, 502), f"status={s}")
    deadline = time.time() + 9
    held = -1
    while time.time() < deadline:
        held = fstatus(p4).get("on_backup_held", -1)
        if held == 0:
            break
        time.sleep(0.5)
    record("超时/退回后 on_backup_held 归零", held == 0, f"held={held}")

    failed = [x for x in results if x[1] == FAIL]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
