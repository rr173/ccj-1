"""控制面：签发、停用调用密钥，查看当前窗口剩余额度。

控制面只写“配置 hash”，从不写令牌桶 / 窗口计数——已发生的扣减只由数据面
通过 Lua 原子写入。两边对不上时，以这些已经对外生效的扣减计数为准。
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import redis
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from common import keys
from common.script_runner import run_script_sync

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
# 管理面访问令牌：生产环境应走正式的管理员鉴权，这里用静态 Bearer 演示边界
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "change-me-admin-token")

COMMON_DIR = Path(__file__).resolve().parent.parent / "common"

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_QUOTA_SRC = (COMMON_DIR / "quota.lua").read_text(encoding="utf-8")
# SHA 放在可变 dict 里：Redis 重启清空脚本缓存后由 script_runner 刷新，
# 无需重启本进程
_quota_sha: dict = {"sha": None}


def quota_eval(cfg_key_: str, tb_key_: str, *argv) -> tuple:
    return run_script_sync(r, _QUOTA_SRC, _quota_sha, 2,
                           [cfg_key_, tb_key_, *argv])


app = FastAPI(title="quota-control-plane", version="1.0.0")


class QuotaStoreUnavailable(HTTPException):
    """记额度那层不可用：返回 503 并提示重试，而不是裸 500。"""

    def __init__(self) -> None:
        super().__init__(
            status_code=503,
            detail={"code": "quota_store_unavailable",
                    "message": "quota store (Redis) is temporarily unavailable; retry shortly"},
            headers={"Retry-After": "2"},
        )


def _retry_ping() -> None:
    """写/读前确认存储可达（脚本执行器内部对瞬时错误已有退避重试）。"""
    try:
        r.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc


class IssueRequest(BaseModel):
    name: str = Field(default="", description="备注名，便于检索")
    burst_capacity: int = Field(gt=0, description="短周期突发上限（令牌桶容量）")
    burst_refill_ms: int = Field(gt=0, description="每补充 1 个令牌的毫秒数")
    window_seconds: int = Field(gt=0, le=86400, description="长周期窗口长度（秒）")
    window_quota: int = Field(gt=0, description="每个长周期窗口的总量")


def require_admin(authorization: Optional[str] = Header(default=None)) -> None:
    expected = f"Bearer {ADMIN_TOKEN}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="admin token required")


@app.get("/healthz")
def healthz() -> JSONResponse:
    try:
        r.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        return JSONResponse(status_code=503,
                            content={"ok": False, "plane": "control",
                                     "store": "unavailable"})
    return JSONResponse(content={"ok": True, "plane": "control", "store": "ok"})


@app.post("/v1/keys", dependencies=[Depends(require_admin)])
def issue_key(body: IssueRequest) -> JSONResponse:
    """签发密钥：明文只返回这一次，Redis 中只保存其哈希与配置。"""
    _retry_ping()
    api_key = keys.new_api_key()
    kid = keys.key_id(api_key)
    try:
        r.hset(
            keys.cfg_key(kid),
            mapping={
                keys.F_NAME: body.name,
                keys.F_REVOKED: "0",
                keys.F_CAPACITY: body.burst_capacity,
                keys.F_REFILL_MS: body.burst_refill_ms,
                keys.F_WINDOW_SECONDS: body.window_seconds,
                keys.F_WINDOW_QUOTA: body.window_quota,
                keys.F_CREATED_AT: str(int(time.time())),
            },
        )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    return JSONResponse(
        status_code=201,
        content={
            "key_id": kid,
            "api_key": api_key,  # 仅此一次
            "limits": body.model_dump(),
        },
    )


def _load_config(kid: str) -> Optional[dict]:
    try:
        cfg = r.hgetall(keys.cfg_key(kid))
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    if not cfg:
        return None
    return cfg


@app.post("/v1/keys/{kid}/revoke", dependencies=[Depends(require_admin)])
def revoke_key(kid: str) -> dict:
    """停用：置 revoked=1。数据面每次调用都实时读配置，不缓存放行结论，
    因此停用在下一次调用即生效；已扣减计数原样保留作为对账依据。"""
    cfg = _load_config(kid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="key not found")
    try:
        r.hset(keys.cfg_key(kid), keys.F_REVOKED, "1")
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    return {"key_id": kid, "revoked": True}


@app.get("/v1/keys/{kid}/quota", dependencies=[Depends(require_admin)])
def get_quota(kid: str) -> dict:
    """只读查看当前剩余：突发余量 + 长周期窗口余量。

    停用后的密钥仍可查询：返回 state=revoked 与其配置/余量口径，不报错。
    """
    cfg = _load_config(kid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="key not found")

    try:
        state, rem_burst, rem_window, win_s, refill_ms, capacity, win_quota = quota_eval(
            keys.cfg_key(kid),
            keys.tb_key(kid),
            kid,
            cfg[keys.F_CAPACITY],
            cfg[keys.F_REFILL_MS],
            cfg[keys.F_WINDOW_SECONDS],
            cfg[keys.F_WINDOW_QUOTA],
        )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    refill_ms = int(refill_ms)
    return {
        "key_id": kid,
        "state": state,
        "burst": {
            "capacity": int(capacity),
            "remaining": int(rem_burst),
            "refill_per_second": round(1000.0 / refill_ms, 3) if refill_ms > 0 else None,
        },
        "window": {
            "seconds": int(win_s),
            "quota": int(win_quota),
            "remaining": max(0, int(rem_window)),
        },
    }


@app.get("/v1/keys", dependencies=[Depends(require_admin)])
def list_keys() -> dict:
    """枚举密钥（SCAN 配置 key，无需额外索引；大规模可换独立元数据库）。"""
    out = []
    try:
        iterator = r.scan_iter(match="{qk:*}cfg", count=200)
        for k in iterator:
            # key 形如 {qk:<kid>}cfg
            skid = k[4:-4]
            scfg = r.hgetall(k)
            out.append(
                {
                    "key_id": skid,
                    "name": scfg.get(keys.F_NAME, ""),
                    "revoked": scfg.get(keys.F_REVOKED) == "1",
                    "burst_capacity": int(scfg.get(keys.F_CAPACITY, 0)),
                    "window_seconds": int(scfg.get(keys.F_WINDOW_SECONDS, 0)),
                    "window_quota": int(scfg.get(keys.F_WINDOW_QUOTA, 0)),
                }
            )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    return {"keys": out}
