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

跨密钥共享池（reserve.lua + pool_reserve.lua）：
  控制面可把多把有效密钥放入同一个池。成员请求必须先占住密钥自己的额度，
  再占住池的聚合额度；池满即拒绝，即使该密钥自己还有额度。密钥出池只影响
  新请求，出池前已占着的配对单仍按原池确认/退回；池停后新占用和在飞确认都拒绝。

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
# 后台兜底扫描周期（秒）：把各池里“过了约定回音时限还没回音”的占用单
# 终结为超时退回并留流水。热池在 reserve.lua 判定时顺手清理，这个任务只
# 兜住之后再没人来占的冷池。
SWEEP_INTERVAL_SECONDS = float(os.environ.get("SWEEP_INTERVAL_SECONDS", "2"))

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
_POOL_RESERVE_SRC = (COMMON_DIR / "pool_reserve.lua").read_text(encoding="utf-8")
_SETTLE_SRC = (COMMON_DIR / "settle.lua").read_text(encoding="utf-8")
_SWEEP_SRC = (COMMON_DIR / "sweep.lua").read_text(encoding="utf-8")
# SHA 放在可变 dict 里：Redis 重启清空脚本缓存后由 script_runner 重载刷新，
# 无需重启数据面进程
_reserve_sha: dict = {"sha": None}
_pool_reserve_sha: dict = {"sha": None}
_settle_sha: dict = {"sha": None}
_sweep_sha: dict = {"sha": None}
_sweep_task: asyncio.Task | None = None


async def init_redis(app: web.Application) -> None:
    global redis_client, _sweep_task
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
    _sweep_task = asyncio.create_task(sweep_loop())


async def close_redis(app: web.Application) -> None:
    if _sweep_task is not None:
        _sweep_task.cancel()
        try:
            await _sweep_task
        except asyncio.CancelledError:
            pass
    await app["upstream"].close()
    if redis_client is not None:
        await redis_client.aclose()


def _error(status: int, code: str, retry_after_ms: int | None = None,
           remaining_burst: int | None = None, remaining_window: int | None = None,
           detail: str | None = None, served_by: str | None = None,
           requested_key: str | None = None) -> web.Response:
    body = {
        "error": {
            "code": code,
            "message": detail or code,
        }
    }
    if served_by is not None and requested_key is not None and served_by != requested_key:
        # 主钥占不住、这笔此刻是备钥在顶（备钥也满了的 429 同样标出）
        body["error"]["failover"] = True
        body["error"]["served_by"] = "backup"
        body["served_key_id"] = served_by
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


async def resolve_identity(api_key: str) -> tuple[str | None, str | None]:
    """把调用出示的明文解析成 (逻辑配额身份 kid, 出示明文哈希 cred32)。

    kid = sha256(明文) 前 32 位。密钥换新后新/旧明文哈希下只有一条
    {qk:<cred32>}rcr 别名指向这把密钥的逻辑 kid（配额/计数/流水都在它名下）；
    没换过新的密钥没有别名，cred32 本身就是逻辑 kid。
    返回 (None, cred) 表示配置存在但别名指向的密钥配置已不在（按无效密钥处理）。
    """
    cred = keys.key_id(api_key)
    try:
        target = await redis_client.get(keys.cred_alias_key(cred))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc
    return (target or cred, cred)


async def reserve_one(effective_kid: str, idem: str, client: str, res_key: str,
                      pid: str = "", pool_res_key: str = "",
                      fb_role: str = "", fb_master_kid: str = "",
                      fb_route_key: str = "", fb_stats_key: str = "",
                      presented_cred: str = "",
                      identity_cred: str = "",
                      cb_owner: str = "") -> list:
    """在一把指定的密钥上完成“密钥自身 + 可选共享池”的占用。

    对调用方来说进来的永远是主钥（X-Api-Key），但主备顶上时真正占到的
    可能是备钥——所有后续了结/记账都以实际占到的 effective_kid 为准。
    fb_role/fb_master_kid/fb_route_key/fb_stats_key 是主备顶上才有的参数。
    presented_cred 是这次请求出示的明文哈希：空串表示系统内部路径
    （主备顶上/业务号路由），跳过换新门但仍按 identity_cred 归因真用掉。
    """
    try:
        cfg = await redis_client.hgetall(keys.cfg_key(effective_kid))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc
    if not cfg:
        return [3, "key not found"]

    try:
        current_pid = await redis_client.get(keys.pool_membership_key(effective_kid))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc
    current_pid = current_pid or pid
    paired_pool_res = keys.pool_reservation_key(current_pid, effective_kid, idem) if current_pid else ""

    args = [
        effective_kid,
        cfg[keys.F_CAPACITY],
        cfg[keys.F_REFILL_MS],
        cfg[keys.F_WINDOW_SECONDS],
        cfg[keys.F_WINDOW_QUOTA],
        1,
        idem,
        RESERVATION_TTL_SECONDS,
        client,
        current_pid,
        paired_pool_res,
        fb_role,
        fb_master_kid,
        fb_route_key,
        fb_stats_key,
        presented_cred,
        identity_cred,
        fb_master_kid if fb_role == "backup" else kid,
    ]
    num_keys = 7 if fb_route_key else 6
    lua_keys = [keys.cfg_key(effective_kid), keys.tb_key(effective_kid),
                keys.shares_key(effective_kid), res_key,
                keys.ledger_stream_key(effective_kid),
                keys.rotation_status_key(effective_kid)]
    if fb_route_key:
        lua_keys.append(fb_route_key)
    try:
        key_result = await run_script_async(
            redis_client, _RESERVE_SRC, _reserve_sha, num_keys,
            [*lua_keys, *args])
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc

    # 非成功且非“同一张在飞单子”的结果无需再占池；只有真的占住/继续这张单子时，
    # 才把密钥与池两侧配成一对。
    if int(key_result[0]) not in (1, 2):
        return key_result
    if not current_pid:
        return key_result
    if int(key_result[0]) == 2 and key_result[2] != "":
        # 已终结/超时的同业务号由现有幂等响应处理，不能再占池。
        return key_result
    if int(key_result[0]) == 2 and len(key_result) >= 7 and key_result[5]:
        # 在飞重放必须沿用第一次占的原池/原池单，即使密钥刚刚被移动到别的池。
        current_pid = key_result[5]
        paired_pool_res = key_result[6]

    pool_res_key = paired_pool_res
    try:
        pcfg = await redis_client.hgetall(keys.pool_cfg_key(current_pid))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc
    if not pcfg:
        # 池侧没占住：密钥侧这笔根本没进上游，不算上游失败；若是唯一试探，
        # settle.lua 只把试探位让回 open，不重新计算冷静时间。
        await settle(effective_kid, res_key, "cancel", force=True)
        return [0, "pool_stopped", 0, 0, 0]

    pool_keys = [
        keys.pool_cfg_key(current_pid),
        keys.pool_tb_key(current_pid),
        keys.pool_members_key(current_pid),
        keys.pool_membership_key(effective_kid),
        pool_res_key,
    ]
    pool_args = [
        current_pid, effective_kid,
        pcfg[keys.F_CAPACITY], pcfg[keys.F_REFILL_MS],
        pcfg[keys.F_WINDOW_SECONDS], pcfg[keys.F_WINDOW_QUOTA],
        1, idem, RESERVATION_TTL_SECONDS, client, res_key,
    ]
    try:
        pool_result = await run_script_async(
            redis_client, _POOL_RESERVE_SRC, _pool_reserve_sha, 5,
            [*pool_keys, *pool_args])
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc

    pc = int(pool_result[0])
    if pc == 1:
        # 只有“新占成密钥侧”时才可能走到这里；code=2 在飞由下面处理。
        return [1, pool_result[1], pool_result[2], key_result[3], pool_res_key]
    if pc == 2 and pool_result[2] == "":
        return [2, key_result[1], "", pool_result[3], pool_result[4], pool_res_key]
    if pc == 0 and pool_result[1] in {"burst_limited", "window_limited"}:
        # 密钥自己还有但池满：立即释放刚占的密钥侧；响应只报池还需等多久。
        await settle(effective_kid, res_key, "cancel", force=True)
        return pool_result
    if pc == 0 and pool_result[1] == "pool_stopped":
        await settle(effective_kid, res_key, "cancel", force=True)
        return pool_result
    if pc == 3 and pool_result[1] == "not pool member":
        # 并发移出池：这次请求不再受池限制，沿用密钥侧已占住的额度。
        return key_result
    return pool_result


async def reserve_with_failover(kid: str, idem: str, client: str,
                                res_key: str, cred: str = "") -> tuple[str, str, list]:
    """主备顶上的占用编排，返回 (实际占到的密钥 kid, 实际占用单 key, 结果)。

    顺序是硬的：
      1. 同一业务号带路由再来：永远走第一次占到的那把（路由只在顶上占成时
         原子写下），不看当前绑定是否已解绑/换绑；
      2. 平时只占主钥；
      3. 主钥【额度】这一笔占不住（突发/窗口满）且绑着有效的备钥，同一笔
         立刻改占备钥——主钥那次 reserve 没占成、什么都没咬住，不存在两头
         同时被占；
      4. 备钥也满了，直接把备钥给出的“还要等多久”告诉调用方；
      5. 主钥又能占住以后，新来的自然回到主钥（主钥路径新占成即 streak=0）。
    主钥停用（key_revoked）、备钥停用/解绑、管理性失败都不触发顶上。

    cred 是调用方对外出示的明文哈希（逻辑 kid 已由别名解析得到）：它永远
    是这一笔记账/换新门的身份；顶上到备钥是系统内部路径，presented 置空
    （不拿主钥明文去开备钥的换新门），但 identity 仍是调用方出示的那把。
    """
    # 1) 业务号路由：同一笔永远回到第一次占到的那把
    if idem:
        try:
            routed = await redis_client.get(keys.failover_route_key(kid, idem))
        except (aioredis.ConnectionError, aioredis.TimeoutError,
                aioredis.BusyLoadingError) as exc:
            raise StoreUnavailableError() from exc
        if routed:
            route_res_key = keys.reservation_key(routed, idem)
            return routed, route_res_key, await reserve_one(
                routed, idem, client, route_res_key,
                presented_cred="", identity_cred=cred,
                fb_role="backup", fb_master_kid=kid)

    # 2) 先只占主钥。没绑备钥时 fb_role 为空，行为与旧版完全一致。
    try:
        bkid = await redis_client.get(keys.failover_backup_key(kid))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc

    role = "master" if bkid else ""
    stats_key = keys.failover_stats_key(kid) if bkid else ""
    result = await reserve_one(kid, idem, client, res_key,
                               fb_role=role, fb_stats_key=stats_key,
                               presented_cred=cred, identity_cred=cred)

    # 3) 只有“这一笔”在主钥额度上占不住才顶上；停用/池停/已了结都不顶
    if int(result[0]) != 0 or result[1] not in {"burst_limited", "window_limited"} \
       or not bkid:
        return kid, res_key, result

    # 顶上前再认一次绑定：解绑与这一笔并发时，以 Redis 里此刻的关系为准，
    # 新来的不能去占已经解绑的备钥。
    try:
        bound_now = await redis_client.get(keys.failover_backup_key(kid))
        backup_revoked = await redis_client.hget(keys.cfg_key(bkid), keys.F_REVOKED)
        backup_exists = await redis_client.exists(keys.cfg_key(bkid))
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError) as exc:
        raise StoreUnavailableError() from exc
    if bound_now != bkid or not backup_exists or backup_revoked == "1":
        # 备钥已经不是这把主钥的备钥：老老实回报主钥的占不住，不顶上
        return kid, res_key, result

    # 4) 同一笔立刻改占备钥（主钥那边没占成，没有需要释放的登记）。
    #    占用单必须以【实际那把密钥】命名：备钥的 holds ZSET、流水与占用单
    #    同 slot，跨 tag 的 member 在超时清理/了结时根本定位不到单子。
    #    业务号相同的幂等性由 (kid, 业务号) 语义 + 主钥侧路由键保证。
    bres_key = keys.reservation_key(bkid, idem) if idem else keys.reservation_key(bkid)
    route_key = keys.failover_route_key(kid, idem) if idem else ""
    bresult = await reserve_one(
        bkid, idem, client, bres_key,
        fb_role="backup", fb_master_kid=kid,
        fb_route_key=route_key, fb_stats_key=keys.failover_stats_key(kid),
        presented_cred="", identity_cred=cred)
    # 顶上占成 / 在飞重放 / 备钥也满（报备钥的等待）——都以备钥结果为准
    return bkid, bres_key, bresult


async def settle(kid: str, res_key: str, action: str, force: bool) -> list | None:
    """了结一笔占用：confirm（调成了）/ release（没调成，退回）。

    记额度层不可用时返回 None：占用单还在，到期后会被超时释放兜底，
    绝不会因为这次了结失败而把额度弄丢或弄多。
    """
    try:
        return await run_script_async(
            redis_client, _SETTLE_SRC, _settle_sha, 3,
            [keys.cfg_key(kid), res_key, keys.ledger_stream_key(kid),
             action, "1" if force else "0"])
    except (aioredis.ConnectionError, aioredis.TimeoutError,
            aioredis.BusyLoadingError, aioredis.ResponseError):
        return None


async def sweep_once() -> int:
    """把所有池里过期未回音的占用单终结为超时退回（sweep.lua）。

    公共池突发占用登记 {qk:*}holds；份额池 {qk:*}sholds:<sha1>；
    跨密钥共享池 {qp:*}holds。
    窗口占用登记（wholds/swholds）里的单子必然也在对应的突发登记里，
    扫一套就够。SCAN 分批枚举池，每池单脚本最多处理 100 笔，避免长尾拖太久。
    """
    finalized = 0
    try:
        async for hkey in _scan_holds():
            try:
                res = await run_script_async(
                    redis_client, _SWEEP_SRC, _sweep_sha, 1,
                    [hkey, "100"])
                finalized += int(res[0])
            except (aioredis.ConnectionError, aioredis.TimeoutError,
                    aioredis.BusyLoadingError, aioredis.ResponseError):
                break  # 存储不可用：下一轮再来
    except asyncio.CancelledError:
        raise
    except Exception:
        # 兜底任务不能因为一次意外把整个循环带崩
        return finalized
    return finalized


async def _scan_holds():
    """枚举所有突发占用登记 key（公共池 + 各份额池）。"""
    seen = set()
    for pattern in ("{qk:*}holds", "{qk:*}sholds:*", "{qp:*}holds"):
        async for k in redis_client.scan_iter(match=pattern, count=200):
            if k not in seen:
                seen.add(k)
                yield k


async def sweep_loop() -> None:
    """周期性兜底：冷池（之后再没人来占）的过期占用也要被终结、留流水。"""
    while True:
        try:
            await asyncio.sleep(SWEEP_INTERVAL_SECONDS)
            await sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(1)


async def handle_proxy(request: web.Request) -> web.Response:
    api_key = request.headers.get("X-Api-Key")
    if not api_key:
        return _error(401, "missing_api_key", detail="missing X-Api-Key header")

    idem = request.headers.get("Idempotency-Key", "")
    # 先把出示明文解析成逻辑配额身份：没换过新时 cred==kid；换过新后
    # {qk:<cred>}rcr 别名把新旧明文都解析到同一逻辑 kid——配额、计数、
    # 占用单、流水全部以逻辑 kid 为准，新旧明文吃的是同一份突发/窗口。
    try:
        kid, cred = await resolve_identity(api_key)
    except StoreUnavailableError:
        return _error(503, "quota_store_unavailable", retry_after_ms=2000,
                      detail="quota store is restarting or unreachable; retry shortly")
    # 占用单号：带业务号（Idempotency-Key）时由 (逻辑 kid, 业务号) 决定——
    # 用旧明文占过的同一业务号，换新明文再来命中的还是同一张占用单，
    # 绝不会被当成另一笔业务再占一次。
    res_key = keys.reservation_key(kid, idem) if idem else keys.reservation_key(kid)
    # 调用方身份：用于同一把密钥内多个租户之间的轮转公平与份额隔离
    client = request.headers.get("X-Client-Id") or (
        request.remote.split(":")[0] if request.remote else "anon")

    # 同密钥、按调用方轮转：先到先占，但每个请求只占自己的 1 个额度，
    # 任何调用方都不能成批抢光；最终额度由 Redis Lua 原子仲裁，绝不超发。
    # 顶上备钥的请求仍排在主钥的队里（对外只有一把主钥），不在备钥侧另开队列。
    # 换新后新旧明文属于同一逻辑密钥，仍排在同一把锁的同一队里。
    queue = registry.get(kid)
    if queue is not None:
        await queue.acquire(client)
    try:
        try:
            served_kid, served_res_key, result = await reserve_with_failover(
                kid, idem, client, res_key, cred=cred or "")
        except StoreUnavailableError:
            # 记额度层重启/不可用：明确告知稍后重试，绝不“放行”也不裸 500
            return _error(503, "quota_store_unavailable", retry_after_ms=2000,
                          detail="quota store is restarting or unreachable; retry shortly")
    finally:
        if queue is not None:
            queue.release(client)
            registry.cleanup(kid, queue)

    code = int(result[0])
    on_backup = served_kid != kid

    if code == 3:
        # 换新门：出示的明文已被收掉/超过说好的宽限时间 → 403；
        # 出示的明文根本不属于这把密钥（且密钥本体存在）→ 401。
        reason = str(result[1])
        if reason == "credential_retired":
            return _error(403, "api_key_retired",
                          detail="this API key plaintext was retired (rotation grace "
                                 "expired or revoked early); use the current plaintext")
        return _error(401, "invalid_api_key", detail=reason)

    if code == 2:
        # 同一业务号再来：返回同一张占用单，绝不二次占额。
        # outcome 非空说明这笔业务此前已了结（数据面可能崩溃过，调用方在重试），
        # 直接把当时的结论还给它，不再接触上游。
        _, lease_exp, outcome, rem_burst, rem_window = result[:5]
        rem_burst, rem_window = int(rem_burst), int(rem_window)
        if outcome == "confirmed":
            return _error(409, "idempotent_replay_confirmed",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail="request with this Idempotency-Key was already confirmed; quota was not reserved again")
        if outcome == "released":
            return _error(409, "idempotent_replay_released",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail="request with this Idempotency-Key failed upstream and its reservation was released; use a new Idempotency-Key to retry")
        if outcome == "timeout":
            # 约定回音时限过了没回音，占用已按超时退回（流水里有 release/
            # timeout 一笔）。同一业务号不能再占——换新业务号再试。
            # 迟到的真实回音若已赶到，reserve.lua 会返回 confirmed 而不是这里。
            return _error(409, "idempotent_replay_timeout",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail="request with this Idempotency-Key timed out before settlement and its reservation was released; use a new Idempotency-Key to retry")
        # outcome == ""：占用还在约定回音时限内（上次可能调到一半数据面崩了，
        # 调用方在重试）。直接带着这张占用单调上游，调完照常了结——
        # 同一笔业务只算一次。（若占用已超时限，reserve.lua 会终结旧占用、
        # 返回 timeout，走不到这里。）
        return await forward_and_settle(request, served_kid, kid, served_res_key,
                                        rem_burst, rem_window, on_backup)

    if code == 0:
        _, reason, retry_ms, rem_burst, rem_window = result
        if reason == "revoked":
            return _error(403, "key_revoked")
        if reason == "pool_stopped":
            return _error(403, "pool_stopped",
                          detail="quota pool is stopped; it cannot accept new reservations")
        if reason == "circuit_open":
            return _error(503, "caller_circuit_open",
                          retry_after_ms=int(retry_ms),
                          detail=f"caller '{client}' is circuit-open on this API key; "
                                 "one probe request will be admitted after the cooldown")
        # 备钥也满了：等待时间以备钥这次判定为准（“备钥也满了才告诉还要等多久”）
        return _error(429, reason, retry_after_ms=int(retry_ms),
                      remaining_burst=int(rem_burst),
                      remaining_window=int(rem_window),
                      detail=None, served_by=(served_kid if on_backup else None),
                      requested_key=kid)

    # code == 1，占成功：放行调上游，调完按结果了结
    _, rem_burst, rem_window, _lease_exp = result[:4]
    return await forward_and_settle(request, served_kid, kid, served_res_key,
                                    int(rem_burst), int(rem_window), on_backup)


async def forward_and_settle(request: web.Request, served_kid: str, kid: str,
                             res_key: str, rem_burst: int, rem_window: int,
                             on_backup: bool) -> web.Response:
    """带着占用单调上游，并按上游结果了结：成了确认，没成退回。

    served_kid 是这笔实际占到的密钥（主或备）；确认/退回都记在它的计数与
    流水上，备钥顶上的单子由 settle.lua 同步回写主钥视角的主备统计。
    """
    body = await request.read()
    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
        and k.lower() not in {"x-api-key", "idempotency-key"}
    }
    url = UPSTREAM_URL.rstrip("/") + request.path_qs
    session = request.app["upstream"]
    # 备钥在顶的标记：成功响应头始终带出，调用方/排障一眼看得出这笔走的是备钥
    served_headers = {
        "X-Quota-Served-By": "backup" if on_backup else "primary",
    }
    if on_backup:
        served_headers["X-Failover"] = "1"
    try:
        async with session.request(request.method, url, headers=fwd_headers,
                                   data=body, allow_redirects=False) as up:
            payload = await up.read()
            if up.status < 500:
                # 上游给了业务应答（含 4xx：调用方自己的问题，额度照算）→ 确认
                settled = await settle(served_kid, res_key, "confirm", force=False)
                if settled and str(settled[0]) == "0" and settled[1] == "pool_stopped":
                    return _error(403, "pool_stopped",
                                  remaining_burst=rem_burst,
                                  remaining_window=rem_window,
                                  detail="quota pool stopped before settlement; this call is not counted")
                resp_headers = {
                    k: v for k, v in up.headers.items()
                    if k.lower() not in HOP_BY_HOP
                }
                resp_headers["X-Quota-Remaining-Burst"] = str(rem_burst)
                resp_headers["X-Quota-Remaining-Window"] = str(rem_window)
                resp_headers.update(served_headers)
                return web.Response(status=up.status, body=payload,
                                    headers=resp_headers)
            # 上游 5xx：这次没调成，占着的额度退回去
            await settle(served_kid, res_key, "release", force=True)
            return _error(502, "upstream_unavailable",
                          remaining_burst=rem_burst, remaining_window=rem_window,
                          detail=f"upstream returned {up.status}; reservation was released",
                          served_by=(served_kid if on_backup else None),
                          requested_key=kid)
    except aiohttp.ClientError as exc:
        # 上游连不上：没调成，占着的额度退回去，别人立刻能再占
        await settle(served_kid, res_key, "release", force=True)
        return _error(502, "upstream_unavailable",
                      remaining_burst=rem_burst, remaining_window=rem_window,
                      detail=f"upstream call failed: {exc}; reservation was released",
                      served_by=(served_kid if on_backup else None),
                      requested_key=kid)
    except (asyncio.CancelledError, ConnectionResetError):
        # 调用方中途断连：这笔不算调成，退回额度，让断连不白扣
        await settle(served_kid, res_key, "release", force=True)
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
