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
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

CP = "http://localhost:8000"
DP = "http://localhost:8080"
ADMIN = "change-me-admin-token"

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


def call_dp(api_key: str, idem: str | None = None):
    headers = {"X-Api-Key": api_key}
    if idem:
        headers["Idempotency-Key"] = idem
    return http("GET", f"{DP}/v1/hello", headers)


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


def t_restart_durability(compose_file: str) -> None:
    print("\n=== 7) Redis 重启：已扣额度不丢、不增 ===")
    kid, key = issue_key("durability", burst=100, refill_ms=100000, win_s=300, win_q=10)
    for _ in range(5):
        s, _, _ = call_dp(key)
        assert s == 200
    before = quota(kid)
    time.sleep(1.5)  # 等 AOF everysec 落盘
    print("  -- 重启 redis 容器 ...")
    r = subprocess.run(["docker", "compose", "-f", compose_file, "restart", "redis"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        record("重启 redis（跳过：未找到 docker compose）", False, r.stderr.strip()[:200])
        return
    wait_healthy()
    time.sleep(1)
    after = quota(kid)
    record("重启后窗口余量保持 5（扣减未丢、未凭空增加）",
           after["window"]["remaining"] == before["window"]["remaining"] == 5
           and after["burst"]["remaining"] == before["burst"]["remaining"],
           f"before={before['window']['remaining']} after={after['window']['remaining']}")
    statuses = [call_dp(key)[0] for _ in range(6)]
    record("重启后仍只能再用 5 个，第 6 个被拒",
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
