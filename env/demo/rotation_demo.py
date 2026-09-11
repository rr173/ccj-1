#!/usr/bin/env python3
"""密钥换新（明文轮换）端到端自检（仅标准库）。

前置：docker compose 起好全套（或本地起 Redis + 控制面 8000 + 数据面 8080
+ mock 上游；建议数据面 RESERVATION_TTL_SECONDS=5 便于测超时/在飞）。

覆盖（对应需求逐条）：
  1. 换新返回新明文并写明旧明文还能再用多久
  2. 换新不清零已真用掉、不重填突发/窗口
  3. 宽限期没到：新旧两把都能占，吃同一份突发/窗口（不多出一份）
  4. 宽限期过了：旧明文不能再占新的，新明文照常
  5. 查得出：此刻哪些明文还能用、旧的还剩多久、新旧各自真用掉多少、余量一份
  6. 宽限没到可提前收掉旧明文；收掉后旧的不能再占；在飞的还能结完
  7. 密钥停了，新旧明文都不能再占新的
  8. 没换过新的只认签发时那一把（出示别的哈希当无效）
  9. 上一档换新的宽限没到，不能再换一次（提前收掉旧明文也不能缩短约定时间）；
     宽限自然到期后才能再换
 10. 用旧明文占过的业务号，换新明文再来不能当成另一笔（同一占用单 409）
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


AUTH = {"Authorization": f"Bearer {ADMIN}"}


def issue_key(name: str, burst: int, refill_ms: int, win_s: int, win_q: int):
    status, _, body = http("POST", f"{CP}/v1/keys", AUTH,
                           {"name": name, "burst_capacity": burst,
                            "burst_refill_ms": refill_ms,
                            "window_seconds": win_s, "window_quota": win_q})
    assert status == 201, body
    return body["key_id"], body["api_key"]


def rotate(kid: str, grace: int):
    return http("POST", f"{CP}/v1/keys/{kid}/rotate", AUTH,
                {"grace_seconds": grace})


def retire(kid: str):
    return http("POST", f"{CP}/v1/keys/{kid}/rotate/retire", AUTH)


def quota(kid: str):
    _, _, body = http("GET", f"{CP}/v1/keys/{kid}/quota", AUTH)
    return body


def revoke(kid: str):
    return http("POST", f"{CP}/v1/keys/{kid}/revoke", AUTH)


def call(api_key: str, idem: str | None = None, path: str = "/v1/hello"):
    headers = {"X-Api-Key": api_key}
    if idem:
        headers["Idempotency-Key"] = idem
    return http("GET", f"{DP}{path}", headers)


def cred_of(api_key: str) -> str:
    import hashlib
    return hashlib.sha256(api_key.encode()).hexdigest()[:32]


def wait_token_refill():
    # 测试密钥 1 令牌 / 1500ms：等补满一个
    time.sleep(1.7)


def main() -> int:
    # 突发 2，1.5s 补 1 个；窗口 60s/100（测试期间窗口不构成约束）
    kid, old_key = issue_key("rot-demo", 2, 1500, 60, 100)
    old_cred = cred_of(old_key)

    # 换新前先用旧明文真用掉 1 笔
    s, _, _ = call(old_key, "pre-1")
    record("换新前旧明文用掉 1 笔", s == 200, f"status={s}")
    q0 = quota(kid)
    record("换新前真用掉=1", q0["burst"]["consumed"] == 1, str(q0["burst"]))
    record("没换过新 rotation.rotated=false", q0["rotation"]["rotated"] is False,
           str(q0["rotation"]))

    # ── 1) 换新：返回新明文，写明旧明文还能用多久 ──────────────────────
    s, _, body = rotate(kid, 2)  # 2 秒宽限，方便测自然到期
    record("换新 201 且返回新明文", s == 201 and body.get("api_key", "").startswith("qk_"),
           str(body))
    grace_ms = body.get("grace_until_ms", 0) - body.get("rotated_at_ms", 0)
    record("写明旧明文还能再用约 2s", abs(grace_ms - 2000) < 50, f"grace={grace_ms}")
    new_key = body["api_key"]
    new_cred, prev_cred = cred_of(new_key), body["previous_key_id"]
    record("上一版明文=换新前那把", prev_cred == old_cred, f"{prev_cred} != {old_cred}")

    # ── 2) 换新不清零、不重填 ─────────────────────────────────────────
    q1 = quota(kid)
    record("换新不清零：真用掉仍是 1", q1["burst"]["consumed"] == 1, str(q1["burst"]))
    record("换新不重填：可再占仍按剩余令牌算", q1["burst"]["remaining"] <= 1,
           str(q1["burst"]))

    rot = q1["rotation"]
    record("换新后 rotated=true、两把都列为可用",
           rot["rotated"] is True and set(rot["usable_key_ids"]) == {old_cred, new_cred},
           str(rot))
    old_v = next(c for c in rot["credentials"] if c["key_id"] == old_cred)
    new_v = next(c for c in rot["credentials"] if c["key_id"] == new_cred)
    record("旧明文状态=grace 且剩余时间>0", old_v["state"] == "grace"
           and 0 < (old_v["grace_remaining_ms"] or 0) <= 2000, str(old_v))
    record("新明文状态=current", new_v["state"] == "current" and new_v["usable"] is True,
           str(new_v))
    record("旧的已真用掉=1（沿用历史）", old_v["consumed"] == 1, str(old_v))

    # ── 3) 宽限期内：新旧都能占，吃同一份，不多出一份 ───────────────────
    # 当前桶里只剩 1 个可占令牌（2 容量 - 1 已用）。
    s, _, _ = call(new_key, "grace-new-1")
    record("宽限期内新明文占得到", s == 200, f"status={s}")
    s2, _, b2 = call(old_key, "grace-old-1")
    record("旧明文再占：同一份令牌已尽 → 429（没有多出一份）",
           s2 == 429 and b2["error"]["code"] == "burst_limited",
           f"status={s2} {b2}")
    wait_token_refill()  # 补回 1 个
    s, _, _ = call(old_key, "grace-old-2")
    record("补令牌后旧明文也占得到（新旧共用同一桶）", s == 200, f"status={s}")
    q2 = quota(kid)
    record("真用掉累计=3，余量仍是这把密钥一份数",
           q2["burst"]["consumed"] == 3 and q2["burst"]["held"] == 0, str(q2["burst"]))
    rot = q2["rotation"]
    by = {c["key_id"]: c for c in rot["credentials"]}
    record("新旧各自真用掉：新=1、旧=2",
           by[new_cred]["consumed"] == 1 and by[old_cred]["consumed"] == 2, str(rot))

    # ── 10) 同一业务号跨明文不二次占 ──────────────────────────────────
    s, _, b = call(new_key, "grace-old-2")
    record("旧明文占过的业务号换新明文：409 原结论、不二次占",
           s == 409 and b["error"]["code"] == "idempotent_replay_confirmed", str(b))

    # ── 9a) 上一档宽限没到，不能再换 ─────────────────────────────────
    s, _, b = rotate(kid, 10)
    record("宽限没到再换被拒 409 rotation_in_grace",
           s == 409 and b["detail"]["code"] == "rotation_in_grace", str(b))

    # ── 4) 宽限到期：旧明文不能再占新的，新明文照常 ────────────────────
    wait_token_refill()
    deadline = time.time() + 4
    retired_code = None
    while time.time() < deadline:
        sc, _, bc = call(old_key, "after-grace-old")
        if sc == 403 and bc["error"]["code"] == "api_key_retired":
            retired_code = sc
            break
        time.sleep(0.3)
    record("说好的时间过了旧明文 403 api_key_retired", retired_code == 403,
           f"last={retired_code}")
    s, _, b = call(new_key, "after-grace-new")
    record("过点后新明文照常可用", s == 200, f"status={s} {b}")
    q3 = quota(kid)
    old_v = next(c for c in q3["rotation"]["credentials"] if c["key_id"] == old_cred)
    record("查询里旧明文 state=expired、剩余=0、移出 usable",
           old_v["state"] == "expired" and old_v["usable"] is False
           and old_v["grace_remaining_ms"] == 0
           and q3["rotation"]["usable_key_ids"] == [new_cred], str(old_v))

    # ── 9b) 宽限过了（且第二把密钥用于提前收掉测试），可以再换 ──────────
    s, _, body2 = rotate(kid, 3)
    record("上一档结束后可以再换", s == 201 and body2["api_key"] != new_key, str(s))
    newest_key = body2["api_key"]
    newest_cred = cred_of(newest_key)
    s, _, b = call(new_key, "chain-prev")
    record("再换后上一把（当时的新明文）进入宽限仍可用", s == 200, f"status={s}")
    s, _, b = call(old_key, "chain-grandpa")
    record("再换后更早那把旧明文仍不认（403 已收掉 / 401 不再认得）",
           s in (401, 403), f"status={s}")

    # ── 6) 提前收掉：旧的不能再占，在飞的能结完 ────────────────────────
    # 另起一把干净密钥做“在飞”验证：用 /slow 让调用占着额度不收尾
    k2, k2_old = issue_key("rot-retire", 2, 1500, 60, 100)
    s, _, rb = rotate(k2, 60)
    assert s == 201, rb
    k2_new = rb["api_key"]
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        # 旧明文开一笔慢调用（4s < 回音时限），先占住
        fut = ex.submit(lambda: call(k2_old, "inflight", path="/slow?ms=4000"))
        time.sleep(1.0)
        qh = quota(k2)
        held = qh["burst"]["held"]
        record("旧明文的在飞单占着额度（held=1）", held == 1, f"held={held}")
        s, _, b = retire(k2)
        record("宽限没到提前收掉旧明文成功", s == 200 and b.get("retired") is True, str(b))
        sc, _, bc = call(k2_old, "after-retire-old")
        record("收掉后旧明文不能再占新的 403", sc == 403
               and bc["error"]["code"] == "api_key_retired", f"status={sc}")
        sh = fut.result(timeout=20)
        record("旧明文开了头还没结的照样结完（200，不放掉额度）",
               sh[0] == 200, f"status={sh[0]}")
    time.sleep(0.3)
    q4 = quota(k2)
    record("在飞结完后真用掉照实计入、held 归零",
           q4["burst"]["consumed"] >= 1 and q4["burst"]["held"] == 0, str(q4["burst"]))
    # 收掉是幂等的
    s, _, b = retire(k2)
    record("重复收掉幂等 200 already_inactive", s == 200
           and b.get("already_inactive") is True, str(b))
    # 提前收掉不缩短约定时间：说好的宽限没走完，仍不能再换（即便旧的已收）
    s, _, b = rotate(k2, 10)
    record("收掉旧的以后宽限没走完仍不能再换 409", s == 409
           and b["detail"]["code"] == "rotation_in_grace"
           and b["detail"]["previous_state"] == "retired", str(b))
    # 收掉只是不能占新的：当前明文不受影响，照常可用
    s, _, _ = call(k2_new, "retired-then-current-ok")
    record("收掉上一版后当前明文照常可用", s == 200, f"status={s}")

    # ── 7) 密钥停了：新旧都不能再占 ──────────────────────────────────
    s, _, _ = revoke(k2)
    assert s == 200
    cur = rotate(k2, 1)  # 不能再换新（已停）
    record("停用后换新被拒", cur[0] == 409, str(cur[2]))
    sc, _, bc = call(k2_new, "revoked-prev")
    record("停用后旧明文 key_revoked 403", sc == 403
           and bc["error"]["code"] == "key_revoked", f"status={sc}")
    sc, _, bc = call(k2_old, "revoked-older")
    record("停用后更早的明文也 key_revoked 403", sc == 403
           and bc["error"]["code"] == "key_revoked", f"status={sc}")

    # ── 8) 没换过新的只认签发时那一把 ────────────────────────────────
    k3, k3_key = issue_key("rot-none", 2, 1500, 60, 100)
    import secrets as _secrets
    forged = "qk_" + _secrets.token_urlsafe(32)
    # 极小概率与真实哈希碰撞可忽略；forged 没有 cfg 也没有别名 → 401
    sc, _, bc = call(forged, "forged")
    record("陌生明文：invalid_api_key 401", sc == 401
           and bc["error"]["code"] == "invalid_api_key", f"status={sc}")
    sc, _, _ = call(k3_key, "genuine")
    record("没换过新的密钥签发明文照常可用", sc == 200, f"status={sc}")
    # 对没换过新的密钥执行收掉：404
    sc, _, b = retire(k3)
    record("没换过新的密钥没有旧明文可收 404", sc == 404, str(b))

    failed = [x for x in results if x[1] == FAIL]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
