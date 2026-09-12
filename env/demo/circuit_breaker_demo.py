#!/usr/bin/env python3
"""按调用方熔断 端到端自检（仅标准库）。

前置：docker compose 起好全套，或本地起 Redis + 控制面(8000) + 数据面(8080)。
为了快速验证，熔断策略使用 1s 冷静；数据面默认 RESERVATION_TTL_SECONDS=5。
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

results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def http(method: str, url: str, headers: dict | None = None,
         body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read() or b"{}"
            return resp.status, dict(resp.headers), json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw.decode(errors="replace")}
        return exc.code, dict(exc.headers or {}), parsed


def call(api_key: str, caller: str, idem: str | None = None,
         path: str = "/v1/hello"):
    headers = {"X-Api-Key": api_key, "X-Client-Id": caller}
    if idem:
        headers["Idempotency-Key"] = idem
    return http("GET", f"{DP}{path}", headers)


def state(kid: str, caller: str) -> dict:
    _, _, body = http("GET", f"{CP}/v1/keys/{kid}/circuit-breaker",
                      {"Authorization": f"Bearer {ADMIN}"})
    for row in body.get("callers", []):
        if row["caller"] == caller:
            return row
    return {}


def main() -> int:
    _, _, body = http("POST", f"{CP}/v1/keys",
                      {"Authorization": f"Bearer {ADMIN}"},
                      {"name": "circuit-demo", "burst_capacity": 20,
                       "burst_refill_ms": 100, "window_seconds": 60,
                       "window_quota": 1000})
    assert body.get("api_key"), body
    kid, api_key = body["key_id"], body["api_key"]

    s, _, body = http("PUT", f"{CP}/v1/keys/{kid}/circuit-breaker",
                      {"Authorization": f"Bearer {ADMIN}"},
                      {"failure_threshold": 2, "cooldown_seconds": 1,
                       "enabled": True})
    record("配置连续 2 次失败、冷静 1s", s == 200 and body["circuit_breaker"]["enabled"], str(body))

    s, _, body = call(api_key, "bad", "bad-1", "/fail")
    record("第 1 次没调成：502", s == 502, f"status={s}")
    st = state(kid, "bad")
    record("第 1 次后连续没成=1、仍未断",
           st.get("consecutive_failures") == 1 and st.get("open") is False, str(st))

    s, h, body = call(api_key, "bad", "bad-2", "/fail")
    record("第 2 次没调成：502 后此人已断", s == 502, f"status={s}")
    st = state(kid, "bad")
    record("查询得到 open 和剩余冷静",
           st.get("state") == "open" and st.get("open") is True
           and (st.get("cooldown_remaining_ms") or 0) > 0, str(st))

    s, h, body = call(api_key, "bad", "bad-3", "/hello")
    retry_ms = body.get("error", {}).get("retry_after_ms", 0)
    record("断着再来不占额度：503 caller_circuit_open 且告知时间",
           s == 503 and body.get("error", {}).get("code") == "caller_circuit_open"
           and retry_ms > 0 and h.get("Retry-After"), str(body))

    s, _, _ = call(api_key, "good", "good-1", "/hello")
    record("同一把密钥的其他调用方照常占", s == 200, f"status={s}")

    s, _, body = call(api_key, "bad", "bad-1", "/hello")
    record("同一业务号重放只回第一次失败结论，不再占/不再记账",
           s == 409 and body.get("error", {}).get("code") == "idempotent_replay_released",
           str(body))
    st2 = state(kid, "bad")
    record("幂等重放没有多记一次连续失败",
           st2.get("consecutive_failures") == st.get("consecutive_failures"),
           f"{st.get('consecutive_failures')} -> {st2.get('consecutive_failures')}")

    time.sleep(1.05)
    s, h, _ = call(api_key, "good", "good-2", "/hello")
    record("等待期间其他调用方仍正常", s == 200, f"status={s}")

    s, h, body = call(api_key, "bad", "bad-probe-fail", "/fail")
    record("冷静后第一笔被放行试探；试探失败返回 502", s == 502, f"status={s}")
    st = state(kid, "bad")
    record("试探没成，马上再断且冷静重新算",
           st.get("state") == "open" and st.get("open") is True
           and (st.get("cooldown_remaining_ms") or 0) >= 900, str(st))

    time.sleep(1.05)
    st_before = state(kid, "bad")
    s, h, _ = call(api_key, "bad", "bad-probe-success", "/hello")
    record("下一轮冷静后唯一试探调成", s == 200, f"status={s}")
    st = state(kid, "bad")
    record("试探成了才解开并清零",
           st.get("state") == "closed" and st.get("open") is False
           and st.get("consecutive_failures") == 0
           and st.get("probe_in_flight") is False, str(st))
    record("试那一笔之前不是已解开状态", st_before.get("open") is not False, str(st_before))

    for i in range(2):
        s, _, _ = call(api_key, "manual", f"manual-{i}", "/fail")
    s, _, body = http("POST",
                      f"{CP}/v1/keys/{kid}/circuit-breaker/manual/reset",
                      {"Authorization": f"Bearer {ADMIN}"})
    record("可提前手动解开", s == 200 and body.get("reset") is True, str(body))
    st = state(kid, "manual")
    record("手动解开后连续没成清零、不再 open",
           st.get("state") == "closed" and st.get("consecutive_failures") == 0
           and st.get("open") is False, str(st))
    s, _, _ = call(api_key, "manual", "manual-after-reset", "/hello")
    record("手动解开后立刻能再占", s == 200, f"status={s}")

    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
