"""共享的健壮 Lua 脚本执行器。

解决“记额度那层（Redis）重启后，两端持续内部错误、必须重启进程”的问题：

1. Redis 重启会清空脚本缓存，EVALSHA 返回 NOSCRIPT（redis-py 抛
   NoScriptError，其文案是 "No matching script. Please use EVAL."，
   并不包含字符串 "NOSCRIPT"）。这里按异常类型识别，重新 SCRIPT LOAD
   并把新 SHA 回写到两端的缓存，后续请求自动恢复，无需重启进程。
2. Redis 进程刚拉起 / 主从切换后的短窗口内会返回连接拒绝、LOADING、
   READONLY 等，按小退避重试若干次，而不是直接 5xx。
3. 重试只作用于幂等的只读脚本与“由 Lua 自身保证幂等”的扣减脚本
   （consume.lua 内部对已落库的结果不重复写），因此连接在服务端已执行、
   客户端却超时这种不确定情况下重试是安全的；是否被真正扣减以 Redis
   中已落盘的状态/幂等键为准。
"""
from __future__ import annotations

import asyncio
import time

import redis
import redis.asyncio as aioredis

# 异常类统一定义在 redis.exceptions，同步/异步客户端都会抛它们
NoScriptError = redis.exceptions.NoScriptError
ResponseError = redis.exceptions.ResponseError
ConnectionError = redis.exceptions.ConnectionError
TimeoutError_ = redis.exceptions.TimeoutError
BusyLoadingError = redis.exceptions.BusyLoadingError

# 连接类瞬时错误：重试期间 Redis 仍在拉起 / 故障切换
RETRYABLE = (ConnectionError, TimeoutError_, BusyLoadingError)


def _is_noscript(exc: Exception) -> bool:
    if isinstance(exc, NoScriptError):
        return True
    msg = str(exc).upper()
    return "NOSCRIPT" in msg or "NO MATCHING SCRIPT" in msg


def _is_readonly(exc: Exception) -> bool:
    return "READONLY" in str(exc).upper()  # 主从切换后打到只读副本


def run_script_sync(client: redis.Redis, src: str, sha_holder: dict,
                    numkeys: int, keys_and_args: list, *,
                    attempts: int = 6) -> list:
    """同步执行 Lua（控制面用）。sha_holder={'sha': ...} 由调用方持有。"""
    delay = 0.1
    for _ in range(attempts):
        if sha_holder.get("sha") is None:
            sha_holder["sha"] = client.script_load(src)
        try:
            return client.evalsha(sha_holder["sha"], numkeys, *keys_and_args)
        except NoScriptError:
            # 脚本缓存被清空（Redis 重启/切换）：重载并刷新 SHA，下一轮重试
            sha_holder["sha"] = client.script_load(src)
        except ResponseError as exc:
            if not _is_readonly(exc):
                raise
        except RETRYABLE:
            pass
        # 指数退避：0.1,0.2,0.4,...，总等待约 6.3s
        time.sleep(delay)
        delay = min(delay * 2, 2.0)
    # 最后一次不再吞错误，交给上层转成 503
    if sha_holder.get("sha") is None:
        sha_holder["sha"] = client.script_load(src)
    return client.evalsha(sha_holder["sha"], numkeys, *keys_and_args)


async def run_script_async(client: aioredis.Redis, src: str, sha_holder: dict,
                           numkeys: int, keys_and_args: list, *,
                           attempts: int = 6) -> list:
    """异步执行 Lua（数据面用）。语义同 run_script_sync。"""
    delay = 0.1
    for _ in range(attempts):
        if sha_holder.get("sha") is None:
            sha_holder["sha"] = await client.script_load(src)
        try:
            return await client.evalsha(sha_holder["sha"], numkeys, *keys_and_args)
        except NoScriptError:
            sha_holder["sha"] = await client.script_load(src)
        except ResponseError as exc:
            if not _is_readonly(exc):
                raise
        except RETRYABLE:
            pass
        await asyncio.sleep(delay)
        delay = min(delay * 2, 2.0)
    if sha_holder.get("sha") is None:
        sha_holder["sha"] = await client.script_load(src)
    return await client.evalsha(sha_holder["sha"], numkeys, *keys_and_args)
