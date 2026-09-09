#!/usr/bin/env python3
"""端到端自检脚本（仅用标准库）。

用法：
  1. docker compose up -d --build
  2. python3 demo/e2e_demo.py
  3. 带 --restart 时额外验证 Redis 重启后已扣额度不丢不增
     （需要本机能执行 `docker compose`，会重启 redis 容器）
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
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
        with urllib.request.urlopen(req, timeout=15) as resp:
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


def call_dp(api_key: str, idem: str | None = None, client: str | None = None):
    headers = {"X-Api-Key": api_key}
    if idem:
        headers["Idempotency-Key"] = idem
    if client:
        headers["X-Client-Id"] = client
    return http("GET", f"{DP}/v1/hello", headers)


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
            record("突发打满后立即拒绝且码为 burst_limited",
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
    ok = q["state"] == "active" and q["burst"]["remaining"] == 7 and q["window"]["remaining"] == 5
    record("扣减后控制面只读视图准确", ok, json.dumps(q, ensure_ascii=False))


def t_idempotency() -> None:
    print("\n=== 5) 幂等键：重试不重复扣额度 ===")
    kid, key = issue_key("idem", burst=10, refill_ms=100000, win_s=60, win_q=10)
    s1, _, b1 = call_dp(key, idem="order-123")
    s2, _, b2 = call_dp(key, idem="order-123")
    s3, _, _ = call_dp(key, idem="order-123")
    record("首次放行，重放返回 409 且不再扣减",
           s1 == 200 and s2 == 409 and s3 == 409
           and b2["error"]["code"] == "idempotent_replay_allowed",
           f"{s1}/{s2}/{s3}")
    q = quota(kid)
    record("三次请求只扣 1 个额度", q["burst"]["remaining"] == 9,
           f"remaining={q['burst']['remaining']}")


def t_auth_and_revoke() -> None:
    print("\n=== 6) 无密钥拒绝 + 停用立即生效 ===")
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


def t_shares() -> None:
    print("\n=== 7) 点名调用方份额：预留、隔离、总量约束、停用失效 ===")
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

    # 未点名的 C 只能用公共池 5 个，第 6 个被拒（吃不到 B 留的 2 个）
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
    print("\n=== 8) 记额度层重启：两端不重启，已扣额度不丢不增、自动恢复 ===")
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
    t_idempotency()
    t_auth_and_revoke()
    t_shares()
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
