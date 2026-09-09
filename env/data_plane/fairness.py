"""同一把密钥内、不同调用方之间的相对公平调度。

场景：密钥 K 同时被多个下游（租户/客户端）高并发使用。仅靠一把全局 FIFO
锁时，调度顺序完全取决于谁先抢到锁；极端情况下某个连接的请求可能被成批
放行。本模块在“同一密钥”维度内再按调用方身份分桶，用轮转（round-robin）
仲裁申请顺序：

  * 同一密钥、同一调用方：严格 FIFO；
  * 同一密钥、不同调用方：轮流服务，任何一方都不能连续批量拿走额度；
  * 最终“还剩多少额度、放不放行”仍由 Redis Lua 原子判定，本地调度只
    决定申请顺序，因此多副本下也绝不超发。跨副本的严格轮转需要在数据面
    之上按密钥做一致性路由（见 README）。

调用方身份取 X-Client-Id 头，缺省用对端 IP，再缺省为 "anon"。

状态机（asyncio 单线程事件循环，无需加锁）：
  acquire -> 请求进入所属调用方队列；若调度器空闲且轮到自己，事件被 set，
             同时该调用方记为“在飞”，事件仍保留在队列中直到 release。
  release -> 弹出本次在飞事件、清空在飞标记，再从下一个调用方继续轮转。
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict, defaultdict, deque


class FairKeyQueue:
    def __init__(self) -> None:
        # client -> 等待中 + 可能含一个在飞事件（队首），FIFO
        self._waiting: dict[str, deque[asyncio.Event]] = defaultdict(deque)
        self._clients: "OrderedDict[str, None]" = OrderedDict()
        self._busy = False
        self._inflight_client: str | None = None

    async def acquire(self, client: str) -> None:
        ev = asyncio.Event()
        self._waiting[client].append(ev)
        self._clients.setdefault(client, None)
        self._pump(first_hint=client)
        await ev.wait()

    def release(self, client: str) -> None:
        # 弹出本次在飞请求（其事件是该调用方队列队首）
        q = self._waiting.get(client)
        if q:
            q.popleft()
        self._inflight_client = None
        self._busy = False
        self._prune(client)
        self._pump(first_hint=client)

    def _prune(self, client: str) -> None:
        if not self._waiting.get(client):
            self._waiting.pop(client, None)
            self._clients.pop(client, None)

    def _pump(self, first_hint: str) -> None:
        if self._busy:
            return
        order = list(self._clients.keys())
        if not order:
            return
        # 从 first_hint 的下一家开始轮转；它不在列表中（已被清理）时从头找
        start = 0
        if first_hint in self._clients:
            start = (order.index(first_hint) + 1) % len(order)
        for off in range(len(order)):
            cand = order[(start + off) % len(order)]
            q = self._waiting.get(cand)
            if q:
                self._busy = True
                self._inflight_client = cand
                self._clients.move_to_end(cand)
                q[0].set()
                return

    def is_empty(self) -> bool:
        return not self._busy and not self._waiting


class FairRegistry:
    """per-key 队列注册表；超上限退化为不排队（Redis 原子判定兜底不超发）。"""

    def __init__(self, max_keys: int) -> None:
        self._queues: dict[str, FairKeyQueue] = {}
        self._max = max_keys

    def get(self, kid: str) -> FairKeyQueue | None:
        q = self._queues.get(kid)
        if q is None:
            if len(self._queues) >= self._max:
                return None
            q = FairKeyQueue()
            self._queues[kid] = q
        return q

    def cleanup(self, kid: str, q: FairKeyQueue) -> None:
        if self._queues.get(kid) is q and q.is_empty():
            self._queues.pop(kid, None)
