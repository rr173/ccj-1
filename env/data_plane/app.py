"""数据面：先占额度、再调上游、最后按结果了结占用（确认或退回）。

三段式（对应 reserve.lua / settle.lua，各为 Redis 单线程内的一段原子 Lua）：
  1. 占：判定 + 预扣一次完成。占成才放行；占不住按 429 告知还要等多久。
  2. 调：透传到上游真实服务。
  3. 了结：上游给了正常业务应答 → 确认，占用转为真用掉；
           上游连不上 / 5xx / 调用方中途断连 → 退回，额度立刻能被别人再占；
           数据面自己崩了没来得及了结 → 占用单带约定回音时限，过期后任何人
           都能把它释放，占着的额度自动回到池里。

公平性（见 data_plane/fairness.py）：
  同一把密钥的请求在本进程内进入 per-key 调度器：同一调用方严格 FIFO，
  不同调用方（X-Client-Id，缺省对端 IP）轮转服务，谁都不能成批抢光额度。
  最终“占不占得到、还剩多少”集中在 Redis 单线程 Lua 中原子判定，因此多副本
  部署也绝不超发；跨副本的严格轮转需要按密钥一致性路由（见 README）。

份额隔离（reserve.lua 内原子判定）：
  控制面可为密钥指定若干调用方各留一份突发/窗口额度（shares 表）。
  被点名的调用方只占用自己的份额，未点名的只占用公共池（总量 − 全部预留），
  两边互不相通；密钥停用后份额与公共池一并立即失效。

幂等（同一业务号）：
  调用方带 Idempotency-Key 时，占用单以该业务号命名。同一业务号再来，
  reserve.lua 直接返回同一张占用单，绝不二次占额；数据面凭单子上已记录的
  上游结论补做确认/退回（进程崩溃后重试也能把账结平），不重复调上游。

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
# 占用约定的回音时限（秒）：占用后超过这个时间还没确认/退回，
# 占用单即视为“叫号未到”，占着的额度自动退回池里给别人用。
# 必须大于上游调用的总超时（30s），否则慢上游会被误判为失联
RESERVATION_TTL_SECONDS = int(os.environ.get(
    "RESERVATION_TTL_SECONDS", str(keys.DEFAULT_RESERVATION_TTL_SECONDS)))

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
_RESERVE_SRC = (COMMON_DIR / "reserve.lua").read_text(encoding="utf-8")
_SETTLE_SRC = (COMMON_DIR / "settle.lua").read_text(encoding="utf-8")
# SHA 放在可变 dict 里：Redis 重启清空脚本缓存后由 script_runner 重载刷新，
# 无需重启数据面进程
_reserve_sha: dict = {"sha": None}
_settle_sha: dict = {"sha": None}


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


async def reserve(kid: str, idem: str, client: str, res_key: str) -> list:
    """先占额度：原子判定 + 登记占用。返回 reserve.lua 的结果数组。"""
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
        RESERVATION_TTL_SECONDS,
        client,
    ]
    lua_keys = [keys.cfg_key(kid), keys.tb_key(kid), keys.shares_key(kid), res_key]
    try:
        return await run_script_async(
            redis_client, _RESERVE_SRC, _reserve_sha, 4,
            [*lua_keys, *args])
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc


async def settle(kid: str, res_key: str, action: str, force: bool) -> list | None:
    """了结一笔占用：confirm（调成了）/ release（没调成，退回）。

    记额度层不可用时返回 None：占用单还在，到期后会被超时释放兜底，
    绝不会因为这次了结失败而把额度弄丢或弄多。
    """
    try:
        return await run_script_async(
            redis_client, _SETTLE_SRC, _settle_sha, 2,
            [keys.cfg_key(kid), res_key, action, "1" if force else "0"])
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError, aioredis.ResponseError):
        return None


async def handle_proxy(request: web.Request) -> web.Response:
    api_key = request.headers.get("X-Api-Key")
    if not api_key:
        return _error(401, "missing_api_key", detail="missing X-Api-Key header")

    idem = request.headers.get("Idempotency-Key", "")
    kid = keys.key_id(api_key)
    # 占用单号：带业务号（Idempotency-Key）时由业务号决定——同一业务号再来，
    # 命中同一张占用单，绝不二次占额；匿名调用每次一张新单子
    res_key = keys.reservation_key(kid, idem) if idem else keys.reservation_key(kid)
    # 调用方身份：用于同一把密钥内多个租户之间的轮转公平与份额隔离
    client = request.headers.get("X-Client-Id") or (
        request.remote.split(":")[0] if request.remote else "anon")

    # 同密钥、按调用方轮转：先到先占，但每个请求只占自己的 1 个额度，
    # 任何调用方都不能成批抢光；最终额度由 Redis Lua 原子仲裁，绝不超发。
    # 份额隔离同样在 Lua 内完成：client 命中 shares 表时只占自己的预留池。
    queue = registry.get(kid)
    if queue is not None:
        await queue.acquire(client)
    try:
        try:
            result = await reserve(kid, idem, client, res_key)
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

    if code == 2:
        # 同一业务号再来：返回同一张占用单，绝不二次占额。
        # outcome 非空说明这笔业务此前已了结（数据面可能崩溃过，调用方在重试），
        # 直接把当时的结论还给它，不再接触上游。
        _, lease_exp, outcome, rem_burst, rem_window = result
        rem_burst, rem_window = int(rem_burst), int(rem_window)
        if outcome == "confirmed":
            return _error(409, "idempotent_replay_confirmed",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail="request with this Idempotency-Key was already confirmed; quota was not reserved again")
        if outcome == "released":
            return _error(409, "idempotent_replay_released",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail="request with this Idempotency-Key failed upstream and its reservation was released; use a new Idempotency-Key to retry")
        # outcome == ""：占用还在约定回音时限内（上次可能调到一半数据面崩了，
        # 调用方在重试）。直接带着这张占用单调上游，调完照常了结——
        # 同一笔业务只算一次。（若占用已超时限，reserve.lua 会按同一业务号
        # 重新占好并返回 code=1，走不到这里。）
        return await forward_and_settle(request, kid, res_key,
                                        rem_burst, rem_window)

    if code == 0:
        _, reason, retry_ms, rem_burst, rem_window = result
        if reason == "revoked":
            return _error(403, "key_revoked")
        return _error(429, reason, retry_after_ms=int(retry_ms),
                      remaining_burst=int(rem_burst),
                      remaining_window=int(rem_window))

    # code == 1，占成功：放行调上游，调完按结果了结
    _, rem_burst, rem_window, lease_exp = result
    return await forward_and_settle(request, kid, res_key,
                                    int(rem_burst), int(rem_window))


async def forward_and_settle(request: web.Request, kid: str, res_key: str,
                             rem_burst: int, rem_window: int) -> web.Response:
    """带着占用单调上游，并按上游结果了结：成了确认，没成退回。"""
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
            if up.status < 500:
                # 上游给了业务应答（含 4xx：调用方自己的问题，额度照算）→ 确认
                await settle(kid, res_key, "confirm", force=False)
                resp_headers = {
                    k: v for k, v in up.headers.items()
                    if k.lower() not in HOP_BY_HOP
                }
                resp_headers["X-Quota-Remaining-Burst"] = str(rem_burst)
                resp_headers["X-Quota-Remaining-Window"] = str(rem_window)
                return web.Response(status=up.status, body=payload,
                                    headers=resp_headers)
            # 上游 5xx：这次没调成，占着的额度退回去
            await settle(kid, res_key, "release", force=True)
            return _error(502, "upstream_unavailable",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail=f"upstream returned {up.status}; reservation was released")
    except aiohttp.ClientError as exc:
        # 上游连不上：没调成，占着的额度退回去，别人立刻能再占
        await settle(kid, res_key, "release", force=True)
        return _error(502, "upstream_unavailable",
                      remaining_burst=rem_burst, remaining_window=rem_window,
                      detail=f"upstream call failed: {exc}; reservation was released")
    except (asyncio.CancelledError, ConnectionResetError):
        # 调用方中途断连：这笔不算调成，退回额度，让断连不白扣
        await settle(kid, res_key, "release", force=True)
        raise


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
