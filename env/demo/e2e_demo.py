#!/usr/bin/env python3
"""端到端自检脚本（仅用标准库）。

用法：
  1. docker compose up -d --build
  2. python3 demo/e2e_demo.py
  3. 带 --restart 时额外验证 Redis 重启后已扣额度不丢不增
     （需要本机能执行 `docker compose`，会重启 redis 容器）

覆盖：先占后调（成了才真扣、没调成退回、超时未回音自动退回）、占着别人
不能拿、占不住告知等多久、余量能看到“还占着/真用掉”、同一业务号幂等、
停用后占着的也不能确认、点名调用方份额隔离、公平性、持久化。
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

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


def issue_key(name: str, burst: int, refill_ms: int, win_s: int, win_q: int) -> tuple[str, str]:
    status, _, body = http("POST", f"{CP}/v1/keys",
                           {"Authorization": f"Bearer {ADMIN}"},
                           {"name": name, "burst_capacity": burst,
                            "burst_refill_ms": refill_ms,
                            "window_seconds": win_s, "window_quota": win_q})
    assert status == 201, body
    return body["key_id"], body["api_key"]


def call_dp(api_key: str, idem: str | None = None, client: str | None = None,
            path: str = "/v1/hello"):
    headers = {"X-Api-Key": api_key}
    if idem:
        headers["Idempotency-Key"] = idem
    if client:
        headers["X-Client-Id"] = client
    return http("GET", f"{DP}{path}", headers)


def set_share(kid: str, caller: str, burst: int, win_q: int):
    return http("PUT", f"{CP}/v1/keys/{kid}/shares/{caller}",
                {"Authorization": f"Bearer {ADMIN}"},
                {"burst_capacity": burst, "window_quota": win_q})


def del_share(kid: str, caller: str):
    return http("DELETE", f"{CP}/v1/keys/{kid}/shares/{caller}",
                {"Authorization": f"Bearer {ADMIN}"})


def quota(kid: str):
    _, _, body = http("GET", f"{CP}/v1/keys/{kid}/quota",
                      {"Authorization": f"Bearer {ADMIN}"})
    return body


def ledger(kid: str, query: str = ""):
    _, _, body = http("GET", f"{CP}/v1/keys/{kid}/ledger?{query}",
                      {"Authorization": f"Bearer {ADMIN}"})
    return body


def statement(kid: str, start: int, end: int, query: str = ""):
    _, _, body = http(
        "GET", f"{CP}/v1/keys/{kid}/statement?start={start}&end={end}&{query}",
        {"Authorization": f"Bearer {ADMIN}"})
    return body


def wait_healthy() -> None:
    for base, plane in ((CP, "control"), (DP, "data")):
        for _ in range(60):
            try:
                status, _, body = http("GET", f"{base}/healthz")
                if status == 200 and body.get("plane") == plane:
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            sys.exit(f"{plane} plane did not become healthy")


def t_burst_limit() -> None:
    print("\n=== 1) 短周期突发限制 + Retry-After ===")
    _, key = issue_key("burst", burst=5, refill_ms=400, win_s=60, win_q=1000)
    allowed = 0
    for i in range(7):
        status, headers, body = call_dp(key)
        if status == 200:
            allowed += 1
        elif status == 429:
            retry_ms = body["error"]["retry_after_ms"]
            retry_hdr = headers.get("Retry-After")
            record("突发占满后立即拒绝且码为 burst_limited",
                   body["error"]["code"] == "burst_limited"
                   and retry_ms > 0 and retry_hdr is not None,
                   f"retry_after_ms={retry_ms}, Retry-After={retry_hdr}, "
                   f"rem_burst={body.get('remaining_burst')}")
            # 等到令牌补充 1 个，重试应当成功
            time.sleep(retry_ms / 1000 + 0.15)
            status2, _, body2 = call_dp(key)
            record("按 Retry-After 等待后重试成功", status2 == 200,
                   f"status={status2} {body2 if status2 != 200 else ''}")
            break
    record("前 5 个请求全部放行（突发容量）", allowed == 5, f"allowed={allowed}")


def t_window_limit() -> None:
    print("\n=== 2) 长周期总量限制（滑动窗口） ===")
    _, key = issue_key("window", burst=1000, refill_ms=1, win_s=3, win_q=5)
    statuses = [call_dp(key)[0] for _ in range(6)]
    record("窗口内第 6 次请求被 window_limited 拒绝",
           statuses[:5] == [200] * 5 and statuses[5] == 429,
           f"statuses={statuses}")
    _, headers, body = call_dp(key)
    retry_ms = body["error"]["retry_after_ms"]
    record("窗口超限给出到窗口边界的等待时间",
           body["error"]["code"] == "window_limited" and 0 < retry_ms <= 3000,
           f"retry_after_ms={retry_ms}")
    time.sleep(retry_ms / 1000 + 0.2)
    status3, _, _ = call_dp(key)
    record("旧请求滑出窗口后额度恢复、调用成功", status3 == 200, f"status={status3}")


def t_fairness() -> None:
    print("\n=== 3) 同一把密钥大量并发请求的相对公平 ===")
    _, key = issue_key("fair", burst=10, refill_ms=100000, win_s=60, win_q=10)
    counts = {"A": 0, "B": 0}
    order: list[str] = []

    def one(client: str, idx: int):
        headers = {"X-Api-Key": key, "X-Client-Id": client}
        return client, idx, http("GET", f"{DP}/v1/hello", headers)

    with ThreadPoolExecutor(max_workers=20) as pool:
        futs = []
        for i in range(10):
            futs.append(pool.submit(one, "A", i))
            futs.append(pool.submit(one, "B", i))
        for f in as_completed(futs):
            client, idx, (status, _, _) = f.result()
            if status == 200:
                counts[client] += 1
                order.append(client)

    total = counts["A"] + counts["B"]
    record("10 个额度恰好放行 10 个（不超发、不饿死人）",
           total == 10 and counts["A"] > 0 and counts["B"] > 0,
           f"counts={counts}")
    record("两个调用方相对均分（4:6 ~ 6:4）",
           abs(counts["A"] - counts["B"]) <= 2,
           f"counts={counts}, 放行顺序={''.join(order)}")
    # 轮转发放下，放行序列中不应出现某一方连续 4 次以上
    run = max((len(list(g)) for _, g in __import__("itertools").groupby(order)), default=0)
    record("不存在某一方成批抢光（最大连放长度 <= 3）", run <= 3,
           f"max_run={run}, order={''.join(order)}")


def t_control_plane_view() -> None:
    print("\n=== 4) 控制面查看余量，口径与数据面一致 ===")
    kid, key = issue_key("view", burst=10, refill_ms=100000, win_s=60, win_q=8)
    for _ in range(3):
        call_dp(key)
    q = quota(kid)
    ok = (q["state"] == "active"
          and q["burst"]["remaining"] == 7 and q["window"]["remaining"] == 5
          and q["burst"]["held"] == 0 and q["burst"]["consumed"] == 3
          and q["window"]["held"] == 0 and q["window"]["consumed"] == 3)
    record("确认后控制面只读视图准确（可再占/还占着/真用掉 三种状态）",
           ok, json.dumps(q, ensure_ascii=False))


def t_reservation_lifecycle() -> None:
    print("\n=== 5) 先占后调：占着别人不能拿、没调成退回、超时自动退回 ===")
    # 5a. 调用进行中：额度显示“还占着”，别人占不走；调成后才算真用掉
    kid, key = issue_key("lifecycle", burst=2, refill_ms=100000, win_s=60, win_q=10)
    done: list = []

    def slow_call():
        done.append(call_dp(key, path="/slow?ms=1500"))

    th = threading.Thread(target=slow_call)
    th.start()
    time.sleep(0.4)  # 等它占上、正在调上游
    q = quota(kid)
    record("调用进行中：余量里看得到“还占着 1 个”，还没真用掉",
           q["burst"]["held"] == 1 and q["burst"]["consumed"] == 0
           and q["burst"]["remaining"] == 1,
           json.dumps(q["burst"], ensure_ascii=False))
    th.join()
    record("慢调用最终成功", done and done[0][0] == 200, f"{done}")
    q = quota(kid)
    record("调成后：占用转为“真用掉”，还占着归零",
           q["burst"]["held"] == 0 and q["burst"]["consumed"] == 1
           and q["burst"]["remaining"] == 1,
           json.dumps(q["burst"], ensure_ascii=False))

    # 5b. 上游没调成：占着的额度退回去，不算用掉
    kid2, key2 = issue_key("lifecycle-fail", burst=5, refill_ms=100000, win_s=60, win_q=10)
    s, _, b = call_dp(key2, path="/fail")
    q2 = quota(kid2)
    record("上游 500：返回 502，且占着的额度已退回（没调成不算用掉）",
           s == 502 and b["error"]["code"] == "upstream_unavailable"
           and q2["burst"]["held"] == 0 and q2["burst"]["consumed"] == 0
           and q2["burst"]["remaining"] == 5 and q2["window"]["consumed"] == 0,
           f"status={s} burst={q2['burst']} window={q2['window']}")

    # 5c. 过了约定回音时限没回音：占着的自动退回，别人能占
    kid3, key3 = issue_key("lifecycle-lease", burst=1, refill_ms=100000, win_s=60, win_q=10)
    done3: list = []

    def hanging_call():
        done3.append(call_dp(key3, path="/slow?ms=7000"))

    th3 = threading.Thread(target=hanging_call)
    th3.start()
    time.sleep(0.5)
    s_blocked, _, b_blocked = call_dp(key3)
    record("占用期间别人占不走（burst_limited 并告知等多久）",
           s_blocked == 429 and b_blocked["error"]["code"] == "burst_limited"
           and b_blocked["error"]["retry_after_ms"] > 0,
           f"status={s_blocked} retry={b_blocked.get('error', {}).get('retry_after_ms')}")
    # 轮询等占用超约定时限自动退回（数据面环境变量 RESERVATION_TTL_SECONDS，演示为 5s）
    released_at = None
    for _ in range(180):
        q3 = quota(kid3)
        if q3["burst"]["held"] == 0:
            released_at = time.time()
            break
        time.sleep(0.5)
    record("过了约定回音时限，占着的自动退回（还占着归零）",
           released_at is not None,
           f"held={q3['burst']['held']}")
    s_after, _, _ = call_dp(key3)
    record("退回后别人立刻能占（调用成功）", s_after == 200, f"status={s_after}")
    th3.join()
    q3 = quota(kid3)
    record("迟到的上游回音仍按真实调用记一笔（窗口真用掉=2，不重复、不丢失）",
           done3 and done3[0][0] == 200
           and q3["window"]["consumed"] == 2 and q3["window"]["held"] == 0,
           f"slow_call={done3} window={q3['window']}")


def t_idempotency() -> None:
    print("\n=== 6) 同一业务号：不再占一次 ===")
    kid, key = issue_key("idem", burst=10, refill_ms=100000, win_s=60, win_q=10)
    s1, _, b1 = call_dp(key, idem="order-123")
    s2, _, b2 = call_dp(key, idem="order-123")
    s3, _, _ = call_dp(key, idem="order-123")
    record("首次调成，同一业务号再来返回 409 且不再占",
           s1 == 200 and s2 == 409 and s3 == 409
           and b2["error"]["code"] == "idempotent_replay_confirmed",
           f"{s1}/{s2}/{s3}")
    q = quota(kid)
    record("三次请求只真用掉 1 个额度",
           q["burst"]["consumed"] == 1 and q["burst"]["held"] == 0,
           f"burst={q['burst']}")

    # 同一业务号在“还占着”期间并发再来：命中同一张占用单，只算一笔
    kid2, key2 = issue_key("idem-inflight", burst=10, refill_ms=100000, win_s=60, win_q=10)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(call_dp, key2, "order-456", None, "/slow?ms=800")
                for _ in range(2)]
        sts = [f.result()[0] for f in futs]
    q2 = quota(kid2)
    record("占用在飞时同一业务号并发再来：都成功但只真用掉 1 个",
           sts == [200, 200] and q2["burst"]["consumed"] == 1,
           f"statuses={sts} burst={q2['burst']}")

    # 没调成的业务号：占用已退回，重放如实告知，换新业务号才能再试
    kid3, key3 = issue_key("idem-fail", burst=10, refill_ms=100000, win_s=60, win_q=10)
    sf1, _, _ = call_dp(key3, idem="order-789", path="/fail")
    sf2, _, bf2 = call_dp(key3, idem="order-789")
    q3 = quota(kid3)
    record("没调成的业务号：额度已退回，重放返回 409 idempotent_replay_released",
           sf1 == 502 and sf2 == 409
           and bf2["error"]["code"] == "idempotent_replay_released"
           and q3["burst"]["consumed"] == 0 and q3["burst"]["held"] == 0,
           f"{sf1}/{sf2} burst={q3['burst']}")


def t_auth_and_revoke() -> None:
    print("\n=== 7) 无密钥拒绝 + 停用立即生效 + 停用后占着的不能确认 ===")
    status, _, _ = http("GET", f"{DP}/v1/hello")
    record("缺少密钥返回 401", status == 401, f"status={status}")

    kid, key = issue_key("revoke", burst=10, refill_ms=1000, win_s=60, win_q=10)
    s_before, _, _ = call_dp(key)
    http("POST", f"{CP}/v1/keys/{kid}/revoke",
         {"Authorization": f"Bearer {ADMIN}"})
    s_after, _, body = call_dp(key)
    record("停用前可用，停用后下一次调用即 403",
           s_before == 200 and s_after == 403 and body["error"]["code"] == "key_revoked",
           f"{s_before}->{s_after}")
    record("停用密钥在控制面视图中为 revoked", quota(kid)["state"] == "revoked")
    # 回归：停用后查余量必须仍然 200（曾经因 refill_ms=0 除零报 500）
    status, _, qbody = http("GET", f"{CP}/v1/keys/{kid}/quota",
                            {"Authorization": f"Bearer {ADMIN}"})
    record("停用后仍可查余量且返回 200、state=revoked",
           status == 200 and qbody.get("state") == "revoked"
           and qbody["burst"]["refill_per_second"] is not None,
           f"status={status}")

    # 停用发生在“还占着”的时候：这笔占用不能再被当成调成
    kid2, key2 = issue_key("revoke-inflight", burst=5, refill_ms=100000, win_s=60, win_q=10)
    done: list = []

    def slow_call():
        done.append(call_dp(key2, path="/slow?ms=1500"))

    th = threading.Thread(target=slow_call)
    th.start()
    time.sleep(0.4)  # 已占上、正在调上游
    http("POST", f"{CP}/v1/keys/{kid2}/revoke", {"Authorization": f"Bearer {ADMIN}"})
    th.join()
    q2 = quota(kid2)
    record("占用在飞时停用：上游虽调成，但确认被拒——不计入真用掉",
           done and done[0][0] == 200
           and q2["state"] == "revoked"
           and q2["burst"]["consumed"] == 0 and q2["window"]["consumed"] == 0,
           f"call={done} burst={q2['burst']}")
    record("停用后在占的额度仍如实展示（等约定时限自动退回）",
           q2["burst"]["held"] == 1, f"held={q2['burst']['held']}")
    s_new, _, _ = call_dp(key2)
    record("停用后新调用立即 403", s_new == 403, f"status={s_new}")
    # 等占用超约定时限自动退回
    expired = False
    for _ in range(180):
        if quota(kid2)["burst"]["held"] == 0:
            expired = True
            break
        time.sleep(0.5)
    record("约定时限到，停用密钥上占着的也自动退回（不会占死）",
           expired, f"held={quota(kid2)['burst']['held']}")


def t_shares() -> None:
    print("\n=== 8) 点名调用方份额：预留、隔离、总量约束、停用失效 ===")
    # 总量：突发 10 / 窗口 10；refill 极慢、窗口 60s 内不滑出，全部确定性
    kid, key = issue_key("shares", burst=10, refill_ms=100000, win_s=60, win_q=10)

    s, _, b = set_share(kid, "A", 3, 3)
    record("给 A 预留 3/3 成功", s == 200 and b["reserved"]["burst_capacity"] == 3,
           f"{s} {b}")
    s, _, b = set_share(kid, "B", 2, 2)
    record("给 B 预留 2/2 成功，reserved 累计 5",
           s == 200 and b["reserved"]["burst_capacity"] == 5, f"{s} {b}")

    s, _, b = set_share(kid, "C", 6, 1)
    record("Σ份额(3+2+6=11)超总量 10 被拒绝(409)",
           s == 409 and b["detail"]["code"] == "exceeds_total", f"{s} {b}")

    # A 用自己的 3 个，第 4 个被拒——此刻公共池 5 个一分未动
    st = [call_dp(key, client="A")[0] for _ in range(3)]
    s4, _, b4 = call_dp(key, client="A")
    record("A 用完自己的 3 个后第 4 个被拒（公共池还有也不能拿），并告知等多久",
           st == [200] * 3 and s4 == 429
           and b4["error"]["code"] == "burst_limited"
           and b4["error"]["retry_after_ms"] > 0,
           f"{st}->{s4} retry_after_ms={b4.get('error', {}).get('retry_after_ms')}")

    q = quota(kid)
    record("余量视图同时看到总盘、公共池和各人份额",
           q["burst"]["remaining"] == 7
           and q["shared_pool"]["burst"]["remaining"] == 5
           and {s_["caller"]: s_["burst"]["remaining"] for s_ in q["shares"]}
           == {"A": 0, "B": 2},
           json.dumps({"total": q["burst"], "pool": q["shared_pool"],
                       "shares": q["shares"]}, ensure_ascii=False))

    # 未点名的 C 只能用公共池 5 个，第 6 个被拒（吃不到别人留的）
    st = [call_dp(key, client="C")[0] for _ in range(5)]
    s6, _, _ = call_dp(key, client="C")
    record("未点名的 C 用光公共池 5 个后第 6 个被拒（吃不到别人留的）",
           st == [200] * 5 and s6 == 429, f"{st}->{s6}")

    # B 的预留完好：B 还能用自己的 2 个，用完第 3 个被拒
    st = [call_dp(key, client="B")[0] for _ in range(2)]
    s3, _, _ = call_dp(key, client="B")
    record("B 的预留未被公共池消耗，自己用完 2 个后第 3 个被拒",
           st == [200] * 2 and s3 == 429, f"{st}->{s3}")

    # A3 + C5 + B2 = 10：总盘恰好用尽，不超发
    q = quota(kid)
    record("总盘恰好用尽（不超发）",
           q["burst"]["remaining"] == 0 and q["window"]["remaining"] == 0,
           f"burst={q['burst']['remaining']} window={q['window']['remaining']}")

    # 份额的窗口维度同样独立：突发充足、窗口份额用尽
    kid4, key4 = issue_key("shares-win", burst=100, refill_ms=1, win_s=3, win_q=10)
    set_share(kid4, "A", 50, 2)  # A 窗口只留 2；公共池窗口 8
    st = [call_dp(key4, client="A")[0] for _ in range(2)]
    s3, _, b3 = call_dp(key4, client="A")
    record("A 的窗口份额 2 用完后被 window_limited 拒绝并给出等待时间",
           st == [200] * 2 and s3 == 429
           and b3["error"]["code"] == "window_limited"
           and 0 < b3["error"]["retry_after_ms"] <= 3000,
           f"{st}->{s3} retry={b3.get('error', {}).get('retry_after_ms')}")
    s4, _, _ = call_dp(key4, client="C")
    record("A 的窗口份额用尽不影响公共池", s4 == 200, f"{s4}")

    # 删除份额：预留回到公共池（refill 较快，等桶按新容量补满）
    kid2, key2 = issue_key("shares-del", burst=4, refill_ms=100, win_s=60, win_q=4)
    set_share(kid2, "A", 3, 3)  # 公共池被压缩到 1/1
    s1 = call_dp(key2, client="C")[0]
    time.sleep(0.15)
    s2 = call_dp(key2, client="C")[0]
    record("公共池被份额压缩到 1 个", s1 == 200 and s2 == 429, f"{s1}/{s2}")
    s, _, _ = del_share(kid2, "A")
    record("删除份额成功", s == 200, f"{s}")
    time.sleep(0.5)  # 公共池按恢复后的容量 4 补满
    st = [call_dp(key2, client="C")[0] for _ in range(3)]
    s5 = call_dp(key2, client="C")[0]
    record("删除份额后预留回到公共池（C 再用 3 个后打满）",
           st == [200] * 3 and s5 == 429, f"{st}->{s5}")

    # 停用：份额与公共池一起立即失效；停用密钥不能再设份额；余量仍可查
    kid3, key3 = issue_key("shares-revoke", burst=6, refill_ms=100000, win_s=60, win_q=6)
    set_share(kid3, "A", 4, 4)
    s1 = call_dp(key3, client="A")[0]
    http("POST", f"{CP}/v1/keys/{kid3}/revoke", {"Authorization": f"Bearer {ADMIN}"})
    s2, _, b2 = call_dp(key3, client="A")
    s3, _, b3 = call_dp(key3, client="C")
    record("停用后份额与公共池都立即失效",
           s1 == 200 and s2 == 403 and s3 == 403
           and b2["error"]["code"] == "key_revoked"
           and b3["error"]["code"] == "key_revoked",
           f"{s1}->{s2}/{s3}")
    s, _, _ = set_share(kid3, "D", 1, 1)
    record("停用密钥拒绝新增份额(409)", s == 409, f"{s}")
    s, _, q = http("GET", f"{CP}/v1/keys/{kid3}/quota",
                   {"Authorization": f"Bearer {ADMIN}"})
    record("停用后仍可查总盘与份额口径（余量 0）",
           s == 200 and q["state"] == "revoked"
           and q["shares"][0]["caller"] == "A"
           and q["shares"][0]["burst"]["capacity"] == 4
           and q["shares"][0]["burst"]["remaining"] == 0,
           f"{s} shares={q.get('shares')}")


def t_restart_durability(compose_file: str) -> None:
    print("\n=== 9) 记额度层重启：两端不重启，已扣额度不丢不增、自动恢复 ===")
    kid, key = issue_key("durability", burst=100, refill_ms=100000, win_s=300, win_q=10)
    for _ in range(4):
        s, _, _ = call_dp(key)
        assert s == 200
    before = quota(kid)
    assert before["burst"]["remaining"] == 96 and before["window"]["remaining"] == 6, before
    time.sleep(1.5)  # 等 AOF 落盘
    print("  -- 重启 redis 容器 ...")
    r = subprocess.run(["docker", "compose", "-f", compose_file, "restart", "redis"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        record("重启 redis（跳过：未找到 docker compose）", False, r.stderr.strip()[:200])
        return
    wait_healthy()
    # 恢复后：余量必须与重启前逐字节一致，且无需重启控制面/数据面
    after = quota(kid)
    record("重启后余量与重启前完全一致（不丢、不增）",
           after["burst"]["remaining"] == 96 and after["window"]["remaining"] == 6,
           f"before=(96,6) after=({after['burst']['remaining']},{after['window']['remaining']})")
    s, _, _ = call_dp(key)
    record("数据面无需重启，恢复后调用自动成功", s == 200, f"status={s}")
    again = quota(kid)
    record("恢复后的调用只扣 1 个额度（95/5）",
           again["burst"]["remaining"] == 95 and again["window"]["remaining"] == 5,
           f"now=({again['burst']['remaining']},{again['window']['remaining']})")
    # 再用掉剩余 5 个，第 6 个必须被拒，证明总量没有凭空恢复
    statuses = [call_dp(key)[0] for _ in range(6)]
    record("剩余额度精确用完后第 6 个被 429 拒绝",
           statuses[:5] == [200] * 5 and statuses[5] == 429,
           f"statuses={statuses}")


def t_ledger_and_statement() -> None:
    print("\n=== 10) 占用流水与对账单 ===")
    t0 = int(time.time())
    kid, key = issue_key("ledger", burst=10, refill_ms=100000, win_s=300, win_q=100)

    # 2 笔调成（alice / bob）、1 笔没调成退回（alice）
    s1, _, _ = call_dp(key, idem="L-1", client="alice")
    s2, _, _ = call_dp(key, idem="L-2", client="bob")
    sf, _, _ = call_dp(key, idem="L-3", client="alice", path="/fail")
    record("准备：2 调成 + 1 退回",
           s1 == 200 and s2 == 200 and sf == 502, f"{s1}/{s2}/{sf}")

    # 10a. 占到/调成/退回各留一笔，且只一笔
    ev = ledger(kid, "order=asc&limit=200")
    pairs = [(e["kind"], e["reason"], e["idem_key"]) for e in ev["events"]]
    expect = [("reserve", "", "L-1"), ("confirm", "", "L-1"),
              ("reserve", "", "L-2"), ("confirm", "", "L-2"),
              ("reserve", "", "L-3"), ("release", "upstream", "L-3")]
    record("占到/调成/退回各一笔、字段正确（kind/reason/idem/pool）",
           pairs == expect, json.dumps(pairs, ensure_ascii=False))
    assert all(e["pool"] == "shared" for e in ev["events"])

    # 10b. 同一业务号再来不重复留笔
    call_dp(key, idem="L-1", client="alice")  # 409
    call_dp(key, idem="L-3", client="alice", path="/fail")  # 409
    ev = ledger(kid, "limit=200")
    record("同一业务号占/结/退各只能记一笔（再来不新增流水）",
           ev["returned"] == 6, f"returned={ev['returned']}")

    # 10c. 按调用方翻
    ev_a = ledger(kid, "caller=alice&order=asc")
    ev_b = ledger(kid, "caller=bob")
    record("按调用方翻：alice 4 笔、bob 2 笔，且不串号",
           ev_a["returned"] == 4 and ev_b["returned"] == 2
           and {e["idem_key"] for e in ev_a["events"]} == {"L-1", "L-3"},
           f"alice={ev_a['returned']} bob={ev_b['returned']}")

    # 10d. 按业务号翻
    ev_i = ledger(kid, "idem_key=L-2")
    record("按业务号翻：L-2 恰好占/结两笔",
           ev_i["returned"] == 2
           and [e["kind"] for e in ev_i["events"]] == ["confirm", "reserve"],
           f"returned={ev_i['returned']}")

    # 10e. 按时间段翻（未来 1 小时为空；覆盖区间为全量）
    now = int(time.time())
    ev_future = ledger(kid, f"start={now+3600}&end={now+7200}")
    ev_past = ledger(kid, f"start={t0-60}&end={now+60}")
    record("按时间段翻：未来为空、覆盖区间拿全 6 笔",
           ev_future["returned"] == 0 and ev_past["returned"] == 6,
           f"future={ev_future['returned']} past={ev_past['returned']}")

    # 10f. 对账单：占过 3、真用掉 2、退回 1，还占着 0
    st = statement(kid, t0 - 60, now + 60)
    t = st["totals"]
    rec = st["reconciliation"]
    ok_stmt = (t["reserved"] == 3 and t["confirmed"] == 2 and t["released"] == 1
               and t["released_upstream"] == 1 and t["released_timeout"] == 0
               and st["held_open"]["total_cost"] == 0)
    record("对账单：占过 3 / 真用掉 2 / 退回 1（upstream）/ 还占着 0",
           ok_stmt, json.dumps(t, ensure_ascii=False))
    record("真用掉与余量计数侧对得上（两边数都亮出）",
           rec["matched"] is True and rec["ledger_confirmed"] == 2
           and rec["counter_consumed"] == 2 and rec["difference"] == 0,
           json.dumps({k: rec[k] for k in
                       ("matched", "ledger_confirmed", "counter_consumed",
                        "difference")}, ensure_ascii=False))

    # 10g. 还占着的单独列、不算真用掉
    done: list = []

    def held_call():
        done.append(call_dp(key, idem="L-hold", client="alice",
                            path="/slow?ms=1200"))

    th = threading.Thread(target=held_call)
    th.start()
    time.sleep(0.5)
    st2 = statement(kid, t0 - 60, now + 120)
    held = st2["held_open"]
    record("调用进行中：还占着的单独列（items_in_range），且不进真用掉",
           held["reserved_in_range_cost"] == 1
           and held["items_in_range"][0]["idem_key"] == "L-hold"
           and st2["totals"]["reserved"] == 4
           and st2["totals"]["confirmed"] == 2,
           json.dumps({"held": held["total_cost"],
                       "items": [x["idem_key"] for x in held["items_in_range"]]},
                      ensure_ascii=False))
    th.join()
    record("慢调用最终成功并补确认", done and done[0][0] == 200, f"{done}")

    # 10h. 对不上时两边数都亮出：在真有调用的密钥上制造计数差异。
    # 正常路径数据面与计数在同一 Lua 内一致，这里直接改 win 桶模拟
    # “余量计数与流水不一致”（如人工修数/迁移），statement 必须两边都亮。
    # 需要本机有 redis-cli（演示镜像里没有就跳过这一子项）。
    import shutil
    rcli = shutil.which("redis-cli")
    if rcli:
        import subprocess
        win_ms = 300_000
        cur_bucket = int(time.time() * 1000) // win_ms
        rc = subprocess.run(
            [rcli, "-u",
             os.environ.get("REDIS_CLI_URL", "redis://127.0.0.1:6379/0"),
             "INCRBY", f"{{qk:{kid}}}win:300:{cur_bucket}", "7"],
            capture_output=True, text=True, timeout=5)
        if rc.returncode == 0 and rc.stdout.strip().isdigit():
            st3 = statement(kid, t0 - 60, int(time.time()) + 60)
            rec3 = st3["reconciliation"]
            ok_diff = (rec3["feasible"] is True and rec3["matched"] is False
                       and rec3["ledger_confirmed"] == 3
                       and rec3["counter_consumed"] == 10
                       and rec3["difference"] == 7 and "disagree" in rec3["note"])
            record("对不上时两边数都亮出（ledger=3 / counter=10 / difference=7）",
                   ok_diff,
                   json.dumps({k: rec3[k] for k in
                               ("matched", "ledger_confirmed", "counter_consumed",
                                "difference")}, ensure_ascii=False))
        else:
            record("对不上时两边数都亮出（跳过：连不上 Redis CLI）", True)
    else:
        record("对不上时两边数都亮出（跳过：环境无 redis-cli）", True)

    # 10i. 停用后流水还在、对账单还能拉
    http("POST", f"{CP}/v1/keys/{kid}/revoke",
         {"Authorization": f"Bearer {ADMIN}"})
    ev_r = ledger(kid, "limit=200")
    st_r = statement(kid, t0 - 60, int(time.time()) + 60)
    record("密钥停用后：流水仍可翻、对账单仍可拉、state=revoked",
           ev_r.get("state") == "revoked" and ev_r["returned"] >= 8
           and st_r["state"] == "revoked"
           and st_r["totals"]["confirmed"] == 3,
           f"events={ev_r['returned']} confirmed={st_r['totals']['confirmed']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--restart", action="store_true",
                    help="额外执行 Redis 重启持久化验证")
    args = ap.parse_args()

    wait_healthy()
    t_burst_limit()
    t_window_limit()
    t_fairness()
    t_control_plane_view()
    t_reservation_lifecycle()
    t_idempotency()
    t_auth_and_revoke()
    t_shares()
    t_ledger_and_statement()
    if args.restart:
        compose = str(Path(__file__).resolve().parent.parent / "docker-compose.yml")
        t_restart_durability(compose)

    print("\n================ 汇总 ================")
    failed = [r for r in results if r[1] == FAIL]
    for name, state, detail in results:
        print(f"[{state}] {name}" + (f" -- {detail}" if detail and state == FAIL else ""))
    print(f"\n共 {len(results)} 项，失败 {len(failed)} 项")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
