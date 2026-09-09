"""数据面：每真实调用一次就执行一次原子扣减，再放行到上游。

公平性（见 data_plane/fairness.py）：
  同一把密钥的请求在本进程内进入 per-key 调度器：同一调用方严格 FIFO，
  不同调用方（X-Client-Id，缺省对端 IP）轮转服务，谁都不能成批抢光额度。
  最终“放不放行、还剩多少”集中在 Redis 单线程 Lua 中原子判定，因此多副本
  部署也绝不超发；跨副本的严格轮转需要按密钥一致性路由（见 README）。

数据面不持有任何额度状态：进程重启不丢、不增任何额度。
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import aiohttp
import redis.asyncio as aioredis
from aiohttp import web

from common import keys
from common.script_runner import run_script_async
from data_plane.fairness import FairRegistry

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://upstream:8000")
SERVER_PORT = int(os.environ.get("PORT", "8080"))
# per-key FIFO 锁的最大数量，防止海量不同密钥撑爆内存；超出后退化为无锁直连
MAX_KEY_LOCKS = int(os.environ.get("MAX_KEY_LOCKS", "100000"))

COMMON_DIR = Path(__file__).resolve().parent.parent / "common"

# Hop-by-hop 以及由本服务重新计算的头，不向上游透传 / 不向下游透传
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length",
    "host",
}


class KeyLockRegistry:
    """有界的 per-key 公平轮转队列（见 data_plane/fairness.py）。"""

    def __init__(self, max_keys: int) -> None:
        self._registry = FairRegistry(max_keys)

    def get(self, kid: str):
        return self._registry.get(kid)

    def cleanup(self, kid: str, q) -> None:
        self._registry.cleanup(kid, q)


registry = KeyLockRegistry(MAX_KEY_LOCKS)
redis_client: aioredis.Redis = None  # type: ignore
_CONSUME_SRC = (COMMON_DIR / "consume.lua").read_text(encoding="utf-8")
# SHA 放在可变 dict 里：Redis 重启清空脚本缓存后由 script_runner 重载刷新，
# 无需重启数据面进程
_consume_sha: dict = {"sha": None}


async def init_redis(app: web.Application) -> None:
    global redis_client
    redis_client = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    app["upstream"] = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    # 短暂等待 Redis 就绪（正常容器编排下很快）；等不到也不退出，
    # 进入降级态：healthz 报 503、调用返回 503，待 Redis 恢复后自愈。
    for attempt in range(30):
        try:
            if await redis_client.ping():
                break
        except (aioredis.ConnectionError, OSError):
            pass
        await asyncio.sleep(1)


async def close_redis(app: web.Application) -> None:
    await app["upstream"].close()
    if redis_client is not None:
        await redis_client.aclose()


def _error(status: int, code: str, retry_after_ms: int | None = None,
           remaining_burst: int | None = None, remaining_window: int | None = None,
           detail: str | None = None) -> web.Response:
    body = {
        "error": {
            "code": code,
            "message": detail or code,
        }
    }
    if retry_after_ms is not None:
        body["error"]["retry_after_ms"] = retry_after_ms
        body["error"]["retry_after"] = max(1, round(retry_after_ms / 1000))
    if remaining_burst is not None:
        body["remaining_burst"] = remaining_burst
    if remaining_window is not None:
        body["remaining_window"] = remaining_window
    headers = {}
    if retry_after_ms is not None:
        # Retry-After 取整秒并向上取整，宁可让调用方多等也不引导其过早重试
        headers["Retry-After"] = str(max(1, (retry_after_ms + 999) // 1000))
        headers["X-Retry-After-Ms"] = str(retry_after_ms)
    return web.json_response(body, status=status, headers=headers)


class StoreUnavailableError(Exception):
    """记额度那层不可用（连接不上 / 重启中 / 只读故障切换）。"""


async def consume(kid: str, idem: str) -> list:
    try:
        cfg = await redis_client.hgetall(keys.cfg_key(kid))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc
    if not cfg:
        return [3, "key not found"]

    args = [
        kid,
        cfg[keys.F_CAPACITY],
        cfg[keys.F_REFILL_MS],
        cfg[keys.F_WINDOW_SECONDS],
        cfg[keys.F_WINDOW_QUOTA],
        1,
        idem,
        keys.DEFAULT_IDEM_TTL_SECONDS,
    ]
    num_keys = 3 if idem else 2
    lua_keys = [keys.cfg_key(kid), keys.tb_key(kid)]
    if idem:
        lua_keys.append(keys.dedup_key(kid, idem))
    try:
        return await run_script_async(
            redis_client, _CONSUME_SRC, _consume_sha, num_keys,
            [*lua_keys, *args])
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc


async def handle_proxy(request: web.Request) -> web.Response:
    api_key = request.headers.get("X-Api-Key")
    if not api_key:
        return _error(401, "missing_api_key", detail="missing X-Api-Key header")

    idem = request.headers.get("Idempotency-Key", "")
    kid = keys.key_id(api_key)
    # 调用方身份：用于同一把密钥内多个租户之间的轮转公平
    client = request.headers.get("X-Client-Id") or (
        request.remote.split(":")[0] if request.remote else "anon")

    # 同密钥、按调用方轮转：先到先判，但每个请求只扣自己的 1 个额度，
    # 任何调用方都不能成批抢光；最终额度由 Redis Lua 原子仲裁，绝不超发。
    queue = registry.get(kid)
    if queue is not None:
        await queue.acquire(client)
    try:
        try:
            result = await consume(kid, idem)
        except StoreUnavailableError:
            # 记额度层重启/不可用：明确告知稍后重试，绝不“放行”也不裸 500
            return _error(503, "quota_store_unavailable", retry_after_ms=2000,
                          detail="quota store is restarting or unreachable; retry shortly")
    finally:
        if queue is not None:
            queue.release(client)
            registry.cleanup(kid, queue)

    code = int(result[0])

    if code == 3:
        return _error(401, "invalid_api_key", detail=str(result[1]))

    if code == 2:  # 幂等重放：返回第一次的结论，不再接触上游、不再扣减
        status = result[1]
        rem_burst, rem_window = int(result[2]), int(result[3])
        if status == "allow":
            return _error(409, "idempotent_replay_allowed",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail="request with this Idempotency-Key was already allowed; quota was not deducted again")
        return _error(429, f"idempotent_replay_{status}",
                      remaining_burst=rem_burst, remaining_window=rem_window,
                      detail="request with this Idempotency-Key was already rejected")

    if code == 0:
        _, reason, retry_ms, rem_burst, rem_window = result
        if reason == "revoked":
            return _error(403, "key_revoked")
        return _error(429, reason, retry_after_ms=int(retry_ms),
                      remaining_burst=int(rem_burst),
                      remaining_window=int(rem_window))

    # code == 1，放行
    _, rem_burst, rem_window, _ = result
    return await forward_to_upstream(request, int(rem_burst), int(rem_window))


async def forward_to_upstream(request: web.Request,
                              rem_burst: int, rem_window: int) -> web.Response:
    body = await request.read()
    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
        and k.lower() not in {"x-api-key", "idempotency-key"}
    }
    url = UPSTREAM_URL.rstrip("/") + request.path_qs
    session = request.app["upstream"]
    try:
        async with session.request(request.method, url, headers=fwd_headers,
                                   data=body, allow_redirects=False) as up:
            payload = await up.read()
            resp_headers = {
                k: v for k, v in up.headers.items()
                if k.lower() not in HOP_BY_HOP
            }
            resp_headers["X-Quota-Remaining-Burst"] = str(rem_burst)
            resp_headers["X-Quota-Remaining-Window"] = str(rem_window)
            return web.Response(status=up.status, body=payload,
                                headers=resp_headers)
    except aiohttp.ClientError as exc:
        # 额度已在 Redis 原子扣减并落盘。上游故障不退款，避免同一笔调用被
        # 重试后重复计数；调用方应携带 Idempotency-Key 实现安全重试。
        return _error(502, "upstream_unavailable",
                      remaining_burst=rem_burst, remaining_window=rem_window,
                      detail=f"quota was consumed, upstream call failed: {exc}")


async def healthz(request: web.Request) -> web.Response:
    try:
        await redis_client.ping()
    except (aioredis.ConnectionError, aioredis.TimeoutError, AttributeError):
        return web.json_response(
            {"ok": False, "plane": "data", "store": "unavailable"}, status=503)
    return web.json_response({"ok": True, "plane": "data", "store": "ok"})


app = web.Application()
app.on_startup.append(init_redis)
app.on_cleanup.append(close_redis)
app.router.add_get("/healthz", healthz)
app.router.add_route("*", "/{path_info:.*}", handle_proxy)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=SERVER_PORT)
