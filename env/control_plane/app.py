"""控制面：签发、停用调用密钥，查看当前额度（可再占 / 还占着 / 真用掉）。

控制面只写“配置 hash”，从不写令牌桶 / 窗口计数 / 占用登记——已发生的占用
与扣减只由数据面通过 Lua 原子写入。两边对不上时，以这些已经对外生效的
计数为准。
"""
from __future__ import annotations

import json
import os
import secrets
import time
from pathlib import Path
from typing import Optional

import redis
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from common import keys
from common.script_runner import run_script_sync
from control_plane import ledger as ledger_mod

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
# 管理面访问令牌：生产环境应走正式的管理员鉴权，这里用静态 Bearer 演示边界
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "change-me-admin-token")

COMMON_DIR = Path(__file__).resolve().parent.parent / "common"

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_QUOTA_SRC = (COMMON_DIR / "quota.lua").read_text(encoding="utf-8")
_SET_SHARE_SRC = (COMMON_DIR / "set_share.lua").read_text(encoding="utf-8")
_REVERSE_SRC = (COMMON_DIR / "reverse.lua").read_text(encoding="utf-8")
_POOL_QUOTA_SRC = (COMMON_DIR / "pool_quota.lua").read_text(encoding="utf-8")
_POOL_MEMBER_SRC = (COMMON_DIR / "pool_member.lua").read_text(encoding="utf-8")
# SHA 放在可变 dict 里：Redis 重启清空脚本缓存后由 script_runner 刷新，
# 无需重启本进程
_quota_sha: dict = {"sha": None}
_set_share_sha: dict = {"sha": None}
_reverse_sha: dict = {"sha": None}
_pool_quota_sha: dict = {"sha": None}
_pool_member_sha: dict = {"sha": None}


def quota_eval(cfg_key_: str, tb_key_: str, shares_key_: str, *argv) -> tuple:
    return run_script_sync(r, _QUOTA_SRC, _quota_sha, 3,
                           [cfg_key_, tb_key_, shares_key_, *argv])


def set_share_eval(cfg_key_: str, shares_key_: str, caller: str, share_json: str) -> list:
    return run_script_sync(r, _SET_SHARE_SRC, _set_share_sha, 2,
                           [cfg_key_, shares_key_, caller, share_json])


def reverse_eval(kid: str, idem: str, amount: int, note: str) -> list:
    """冲正一笔已调成的真用掉（reverse.lua 原子完成校验/退回/留笔）。"""
    return run_script_sync(
        r, _REVERSE_SRC, _reverse_sha, 3,
        [keys.cfg_key(kid), keys.reservation_key(kid, idem),
         keys.ledger_stream_key(kid), kid, idem, amount, note])


def pool_quota_eval(pid: str, cfg: dict) -> list:
    return run_script_sync(
        r, _POOL_QUOTA_SRC, _pool_quota_sha, 3,
        [keys.pool_cfg_key(pid), keys.pool_tb_key(pid), keys.pool_members_key(pid),
         pid, cfg[keys.F_CAPACITY], cfg[keys.F_REFILL_MS],
         cfg[keys.F_WINDOW_SECONDS], cfg[keys.F_WINDOW_QUOTA]])


def pool_member_eval(pid: str, kid: str, action: str) -> list:
    return run_script_sync(
        r, _POOL_MEMBER_SRC, _pool_member_sha, 4,
        [keys.pool_cfg_key(pid), keys.pool_members_key(pid),
         keys.pool_membership_key(kid), keys.cfg_key(kid),
         action, pid, kid])


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


class PoolRequest(BaseModel):
    """跨密钥共享池开池参数。池有自己独立的突发/窗口上限；成员密钥仍各算各的。"""
    pool_id: Optional[str] = Field(default=None, min_length=1, max_length=64,
                                   pattern=r"^[A-Za-z0-9_-]+$",
                                   description="可选外部指定 ID；不传则随机生成")
    name: str = Field(default="", max_length=256)
    burst_capacity: int = Field(gt=0)
    burst_refill_ms: int = Field(gt=0)
    window_seconds: int = Field(gt=0, le=86400)
    window_quota: int = Field(gt=0)


class ShareRequest(BaseModel):
    """给某调用方预留的一份额度。突发/窗口都从密钥总量里切出，
    补充速率与窗口长度沿用密钥配置（份额只是总量里的一截）。"""
    burst_capacity: int = Field(gt=0, description="该调用方预留的突发容量")
    window_quota: int = Field(gt=0, description="该调用方预留的窗口总量")


class ReverseRequest(BaseModel):
    """对一笔已经调成（真用掉）的业务做冲正。

    - idem_key 写明冲哪一笔（调用时带的 Idempotency-Key，业务号）；
    - amount 冲多少，不能超过那笔当时真用掉的数量；
    - 一笔业务号只能冲一次，重复提交返回上一次的冲正结果（幂等）；
    - 只能冲有效密钥上已确认的业务，密钥停用后拒绝新冲正。
    """
    idem_key: str = Field(min_length=1, max_length=256, description="被冲正的业务号")
    amount: int = Field(gt=0, description="冲回数量，不得超过该笔当时真用掉的")
    note: str = Field(default="", max_length=512, description="备注，随流水留档")


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


def _load_pool(pid: str) -> Optional[dict]:
    try:
        cfg = r.hgetall(keys.pool_cfg_key(pid))
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    return cfg or None


def _pool_limits_view(cfg: dict, res: list) -> dict:
    (state, rem_b, rem_w, held_b, held_w, win_s, refill_ms,
     capacity, win_quota, n_members) = res
    capacity, win_quota = int(capacity), int(win_quota)
    rem_b, rem_w = max(0, int(rem_b)), max(0, int(rem_w))
    held_b, held_w = max(0, int(held_b)), max(0, int(held_w))
    stopped = state == "stopped"
    credit_b = max(0, rem_b + held_b - capacity)
    credit_w = max(0, rem_w + held_w - win_quota)
    consumed_b = max(0, capacity - rem_b - held_b)
    consumed_w = max(0, win_quota - rem_w - held_w)
    if stopped:
        # 停池只让 remaining（还能不能再占新的）归零，不能因此改写真用掉/冲正溢余。
        rem_b = rem_w = 0
    return {
        "state": state,
        "burst": {
            "capacity": capacity,
            "remaining": rem_b,
            "held": held_b,
            "consumed": consumed_b,
            "reversal_credit": credit_b,
            "refill_per_second": round(1000.0 / int(refill_ms), 3),
        },
        "window": {
            "seconds": int(win_s),
            "quota": win_quota,
            "remaining": rem_w,
            "held": held_w,
            "consumed": consumed_w,
            "reversal_credit": credit_w,
        },
        "member_count": int(n_members),
    }


@app.post("/v1/pools", status_code=201, dependencies=[Depends(require_admin)])
def create_pool(body: PoolRequest) -> dict:
    """开跨密钥共享额度池：写明突发上限、补充速率与窗口总量。"""
    _retry_ping()
    pid = body.pool_id or secrets.token_hex(16)
    pcfg_key = keys.pool_cfg_key(pid)
    try:
        already = r.exists(pcfg_key)
        if already:
            raise HTTPException(status_code=409, detail={
                "code": "pool_id_exists", "pool_id": pid,
            })
        r.hset(
            pcfg_key,
            mapping={
                keys.F_NAME: body.name,
                keys.F_STOPPED: "0",
                keys.F_CAPACITY: body.burst_capacity,
                keys.F_REFILL_MS: body.burst_refill_ms,
                keys.F_WINDOW_SECONDS: body.window_seconds,
                keys.F_WINDOW_QUOTA: body.window_quota,
                keys.F_CREATED_AT: str(int(time.time())),
            },
        )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    return {
        "pool_id": pid,
        "state": "active",
        "limits": body.model_dump(exclude={"pool_id"}),
    }


@app.post("/v1/pools/{pid}/stop", dependencies=[Depends(require_admin)])
def stop_pool(pid: str) -> dict:
    """停池：成员密钥立刻不能占新的池额度；在飞单回来也不算池真用掉。"""
    cfg = _load_pool(pid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="pool not found")
    try:
        r.hset(keys.pool_cfg_key(pid), keys.F_STOPPED, "1")
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
        raise QuotaStoreUnavailable() from exc
    return {"pool_id": pid, "stopped": True}


@app.get("/v1/pools/{pid}", dependencies=[Depends(require_admin)])
def get_pool(pid: str) -> dict:
    """查池：剩余/在占/真用掉，以及当前在池里的密钥。"""
    cfg = _load_pool(pid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="pool not found")
    try:
        res = pool_quota_eval(pid, cfg)
        members_raw = r.hgetall(keys.pool_members_key(pid))
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    view = _pool_limits_view(cfg, res)
    members = [
        {"key_id": kid, "joined_at_ms": int(joined)}
        for kid, joined in sorted(members_raw.items(), key=lambda x: int(x[1]))
    ]
    return {
        "pool_id": pid,
        "name": cfg.get(keys.F_NAME, ""),
        **view,
        "keys": members,
    }


@app.put("/v1/pools/{pid}/keys/{kid}", dependencies=[Depends(require_admin)])
def add_pool_key(pid: str, kid: str) -> dict:
    """把一把有效密钥放进池。密钥已有归属且不是本池时返回 409。"""
    if _load_pool(pid) is None:
        raise HTTPException(status_code=404, detail="pool not found")
    try:
        res = pool_member_eval(pid, kid, "add")
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    if int(res[0]) != 1:
        reason = res[1]
        if reason == "key not found":
            raise HTTPException(status_code=404, detail="key not found")
        if reason == "key_revoked":
            raise HTTPException(status_code=409, detail="key is revoked")
        if reason == "pool_stopped":
            raise HTTPException(status_code=409, detail="pool is stopped")
        if reason == "already_in_pool":
            raise HTTPException(status_code=409, detail={
                "code": "already_in_pool",
                "message": "key is already a member of another pool",
                "pool_id": res[2],
            })
    return {"pool_id": pid, "key_id": kid, "in_pool": True,
            "member_count": int(res[1])}


@app.delete("/v1/pools/{pid}/keys/{kid}", dependencies=[Depends(require_admin)])
def remove_pool_key(pid: str, kid: str) -> dict:
    """从池中拿出密钥。新调用立即不受池限制；旧在飞单仍按原池走完。"""
    if _load_pool(pid) is None:
        raise HTTPException(status_code=404, detail="pool not found")
    try:
        res = pool_member_eval(pid, kid, "remove")
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    if int(res[0]) != 1:
        reason = res[1]
        if reason == "key not found":
            raise HTTPException(status_code=404, detail="key not found")
        if reason == "not_member":
            raise HTTPException(status_code=404, detail="key is not in this pool")
    return {"pool_id": pid, "key_id": kid, "in_pool": False,
            "member_count": int(res[1])}


@app.get("/v1/keys/{kid}/quota", dependencies=[Depends(require_admin)])
def get_quota(kid: str) -> dict:
    """只读查看当前额度：总盘 + 公共池 + 各点名调用方份额，一次看齐。

    每个视角都给出三种状态的数量，占着的和真用掉的一眼分清：
      * remaining 可再占：现在还能被占走的部分；
      * held      还占着：已预占、还没回音的——别人现在拿不走，没调成或
                  过了约定回音时限会退回来；
      * consumed  真用掉：已确认调成、沉在计数里的（= 配额 − 可再占 − 还占着）。

    顶层 burst/window 是整把密钥的总盘（公共池 + 各份额之和，上限为密钥
    总量）；shared_pool 是未点名调用方可用的部分；shares 逐人列出。
    停用后的密钥仍可查询：返回 state=revoked 与其配置/在占口径，余量为 0。
    """
    cfg = _load_config(kid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="key not found")

    try:
        res = quota_eval(
            keys.cfg_key(kid),
            keys.tb_key(kid),
            keys.shares_key(kid),
            kid,
            cfg[keys.F_CAPACITY],
            cfg[keys.F_REFILL_MS],
            cfg[keys.F_WINDOW_SECONDS],
            cfg[keys.F_WINDOW_QUOTA],
        )
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc

    (state, rem_b_total, rem_w_total, held_b_total, held_w_total,
     win_s, refill_ms, capacity, win_quota,
     reserved_b, reserved_w,
     pool_rem_b, pool_rem_w, pool_held_b, pool_held_w, n_shares) = res[:16]
    refill_ms = int(refill_ms)
    capacity, win_quota = int(capacity), int(win_quota)
    reserved_b, reserved_w = int(reserved_b), int(reserved_w)
    revoked = state == "revoked"

    def view(cap_: int, quota_: int, rem_b: int, rem_w: int,
             held_b: int, held_w: int) -> dict:
        # quota.lua 给的是自然口径（停用也照算）。展示层约定：
        #   * 可再占（remaining）：正常即自然余量；冲正把已确认的量退回后，
        #     桶/窗口里可能出现“超出容量的溢余”，余量可以大于配额，这部分就是
        #     冲回去、立刻又能再占的额度——照常亮出，不按容量截断；
        #   * 还占着（held）     ：照实——占用单还在，等约定时限自动退回；
        #   * 真用掉（consumed） ：配额 − 可再占 − 还占着，冲正后立即跟着少；
        #     余量超过配额（净冲正 > 自然用量）时为 0，不显示成负数；
        #   * reversal_credit   ：冲正净退回、超出当前配额的那部分额度
        #     （= 已冲正 − 当前自然口径仍占用的量），余量里的真用掉为何变少，
        #     从这个字段一眼能对上流水里的 reversal。
        rem_b, rem_w = max(0, int(rem_b)), max(0, int(rem_w))
        held_b, held_w = max(0, int(held_b)), max(0, int(held_w))
        consumed_b = max(0, cap_ - rem_b - held_b)
        consumed_w = max(0, quota_ - rem_w - held_w)
        credit_b = max(0, rem_b + held_b - cap_)
        credit_w = max(0, rem_w + held_w - quota_)
        if revoked:
            rem_b, rem_w = 0, 0
        return {
            "burst": {"capacity": cap_, "remaining": rem_b, "held": held_b,
                      "consumed": consumed_b, "reversal_credit": credit_b},
            "window": {"quota": quota_, "remaining": rem_w, "held": held_w,
                       "consumed": consumed_w, "reversal_credit": credit_w},
        }

    shares = []
    for i in range(int(n_shares)):
        base = 16 + i * 7
        caller, s_cap, s_win_q, s_rem_b, s_rem_w, s_held_b, s_held_w = res[base:base + 7]
        shares.append({
            "caller": caller,
            **view(int(s_cap), int(s_win_q), s_rem_b, s_rem_w, s_held_b, s_held_w),
        })

    pool = view(capacity - reserved_b, win_quota - reserved_w,
                pool_rem_b, pool_rem_w, pool_held_b, pool_held_w)
    total = view(capacity, win_quota, rem_b_total, rem_w_total,
                 held_b_total, held_w_total)

    def _get_quota_pool_view(kid_: str) -> Optional[dict]:
        try:
            pool_id = r.get(keys.pool_membership_key(kid_))
        except redis.exceptions.RedisError:
            return None
        if not pool_id:
            return None
        pcfg = _load_pool(pool_id)
        if not pcfg:
            return None
        pres = pool_quota_eval(pool_id, pcfg)
        return {"pool_id": pool_id, **_pool_limits_view(pcfg, pres)}

    pool_view = _get_quota_pool_view(kid)

    return {
        "key_id": kid,
        "state": state,
        "burst": {**total["burst"],
                  "refill_per_second": round(1000.0 / refill_ms, 3) if refill_ms > 0 else None},
        "window": {"seconds": int(win_s), **total["window"]},
        "reserved": {
            "burst_capacity": reserved_b,
            "window_quota": reserved_w,
        },
        "shared_pool": pool,
        "quota_pool": pool_view,
        "shares": shares,
    }


@app.get("/v1/keys/{kid}/ledger", dependencies=[Depends(require_admin)])
def get_ledger(
    kid: str,
    start: Optional[float] = Query(default=None, description="起（含），unix 秒/毫秒"),
    end: Optional[float] = Query(default=None, description="止（不含），unix 秒/毫秒"),
    caller: Optional[str] = Query(default=None, description="按调用方过滤（X-Client-Id）"),
    idem_key: Optional[str] = Query(default=None, alias="idem_key",
                                    description="按业务号过滤（Idempotency-Key）"),
    order: str = Query(default="desc", pattern="^(asc|desc)$"),
    limit: int = Query(default=ledger_mod.PAGE_DEFAULT, ge=1, le=ledger_mod.PAGE_MAX),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """翻占用流水：占到 / 调成 / 退回每一步一笔，只追加、不可改。

    按密钥查（路径里的 kid）；可叠加按调用方、按业务号、按时间段过滤。
    停用密钥、服务重启后流水都还在。时间参数给秒或毫秒都行（>1e11 按毫秒）。
    """
    cfg = _load_config(kid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="key not found")
    try:
        out = ledger_mod.list_events(
            r, kid, start=start, end=end, caller=caller, idem=idem_key,
            order=order, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    out["state"] = "revoked" if cfg.get(keys.F_REVOKED) == "1" else "active"
    return out


@app.get("/v1/keys/{kid}/statement", dependencies=[Depends(require_admin)])
def get_statement(
    kid: str,
    start: float = Query(..., description="账单起点（含），unix 秒/毫秒"),
    end: float = Query(..., description="账单终点（不含），unix 秒/毫秒"),
    caller: Optional[str] = Query(default=None, description="只出某调用方的账"),
) -> dict:
    """拉 [start, end) 的对账单：

    - totals.reserved：这段占过多少（每笔 reserve 流水）
    - totals.confirmed：真用掉多少（每笔 confirm 流水；confirmed_late 是
      约定时限过后才赶到的迟到确认，单独列出）
    - totals.released：退回多少（released_upstream=没调成即时退，
      released_timeout=过了约定时限没回音自动退）
    - held_open：还占着的单独列（in_range 的与更早挂过来的分开），
      这些不算进真用掉
    - reconciliation：流水侧真用掉（confirm）与余量计数侧真用掉
      （win/swin 固定桶，和 GET /quota 同一批 key）对账；对不上时 matched=false
      且 ledger_confirmed / counter_consumed 两边的数都亮出，并给 difference
      与可能原因（迟到确认 / 计数桶已过 TTL / 不可归因）。
    """
    cfg = _load_config(kid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="key not found")
    try:
        return ledger_mod.build_statement(
            r, kid, cfg, start=start, end=end, caller=caller)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc


@app.post("/v1/keys/{kid}/reversals", status_code=201,
          dependencies=[Depends(require_admin)])
def reverse_consumption(kid: str, body: ReverseRequest) -> dict:
    """对一笔已经调成（真用掉）的业务做冲正。

    冲正规则（由 reverse.lua 在 Redis 单线程内原子保证）：
      * 只能冲有效密钥上、已经调成的那一笔——密钥停用、业务号不存在、
        还占着/已退回的一律拒绝；
      * 写明冲哪个业务号（idem_key）、冲多少（amount），冲的数量不能超过
        那笔当时真用掉的；
      * 一笔业务号只能冲一次：重复提交返回上一次的冲正结果（200，幂等），
        不二次退额度、不写第二笔流水；
      * 冲过的额度立刻回到可再占：突发令牌加回原池令牌桶、窗口量从当初确认
        落的那个固定桶扣掉（已自然老化的维度无账可退，如实标 0）；
      * 冲正自己在只追加流水上留一笔 reversal，之后不能改。
    """
    cfg = _load_config(kid)
    if cfg is None:
        raise HTTPException(status_code=404, detail="key not found")
    try:
        res = reverse_eval(kid, body.idem_key, body.amount, body.note)
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc

    code = int(res[0])
    if code == 1:
        (_, eid, amount_done, orig_cost, tb_ref, win_ref, pool, caller,
         pool_tb_ref, pool_win_ref, pool_id) = res
        return {
            "key_id": kid,
            "reversal_id": eid,
            "idem_key": body.idem_key,
            "amount": int(amount_done),
            "confirmed_cost": int(orig_cost),
            "refunded": {
                "burst": bool(int(tb_ref)),
                "window": bool(int(win_ref)),
            },
            "pool": pool,
            "pool_id": pool_id or None,
            "pool_refunded": {
                "burst": bool(int(pool_tb_ref or 0)),
                "window": bool(int(pool_win_ref or 0)),
            },
            "caller": caller or None,
            "note": body.note,
        }
    if code == 2:
        # 这笔业务已经冲过：把原冲正结论还给调用方（幂等，不再动账、不留笔）
        _, eid, amount_done, orig_cost = res
        return JSONResponse(status_code=200, content={
            "key_id": kid,
            "reversal_id": eid,
            "idem_key": body.idem_key,
            "amount": int(amount_done),
            "confirmed_cost": int(orig_cost),
            "already_reversed": True,
        })

    reason = res[1]
    if reason == "key not found":
        raise HTTPException(status_code=404, detail="key not found")
    if reason == "revoked":
        raise HTTPException(status_code=409, detail={
            "code": "key_revoked",
            "message": "key is revoked; already-used quota on a revoked key cannot be reversed",
        })
    if reason == "idem_required":
        raise HTTPException(status_code=422, detail="idem_key is required")
    if reason == "bad_amount":
        raise HTTPException(status_code=422, detail="amount must be a positive integer")
    if reason == "not_confirmed":
        raise HTTPException(status_code=409, detail={
            "code": "not_confirmed",
            "message": "the reservation identified by this idem_key was never confirmed "
                       "(missing, still held, or released); only confirmed consumption can be reversed",
        })
    if reason == "amount_exceeds":
        raise HTTPException(status_code=422, detail={
            "code": "amount_exceeds",
            "message": "reversal amount cannot exceed what that reservation actually consumed",
            "confirmed_cost": int(res[2]),
            "requested_amount": body.amount,
        })
    raise HTTPException(status_code=400, detail=str(reason))


@app.put("/v1/keys/{kid}/shares/{caller}", dependencies=[Depends(require_admin)])
def put_share(kid: str, caller: str, body: ShareRequest) -> JSONResponse:
    """给有效密钥的某调用方预留一截突发与窗口额度（幂等，可重复设置/调额）。

    约束（由 set_share.lua 在 Redis 内原子校验，多控制面并发也不会超分）：
      Σ各份额 burst_capacity ≤ 密钥 burst_capacity，
      Σ各份额 window_quota  ≤ 密钥 window_quota；
    剩余部分即公共池，留给未点名调用方。份额的窗口长度与补充速率沿用密钥配置。
    已停用密钥不能再指定份额（409）；份额随密钥停用立即失效，无需逐个清理。
    """
    share_json = json.dumps(
        {"burst_capacity": body.burst_capacity, "window_quota": body.window_quota},
        separators=(",", ":"))
    try:
        res = set_share_eval(keys.cfg_key(kid), keys.shares_key(kid),
                             caller, share_json)
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    return _share_result(kid, caller, res, created_body={
        "share": {"burst_capacity": body.burst_capacity,
                  "window_quota": body.window_quota},
    })


@app.delete("/v1/keys/{kid}/shares/{caller}", dependencies=[Depends(require_admin)])
def delete_share(kid: str, caller: str) -> JSONResponse:
    """取消某调用方的预留：其额度立即回到公共池，该调用方此后按未点名处理。
    该调用方已发生的扣减计数原样保留（有 TTL 自然过期），不回写、不补偿。"""
    try:
        res = set_share_eval(keys.cfg_key(kid), keys.shares_key(kid), caller, "")
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.BusyLoadingError) as exc:
        raise QuotaStoreUnavailable() from exc
    return _share_result(kid, caller, res, created_body={"deleted": True})


def _share_result(kid: str, caller: str, res: list,
                  created_body: dict) -> JSONResponse:
    """把 set_share.lua 的返回翻译成 HTTP 响应。"""
    if int(res[0]) == 1:
        reserved_b, reserved_w = int(res[1]), int(res[2])
        return JSONResponse(content={
            "key_id": kid,
            "caller": caller,
            **created_body,
            "reserved": {"burst_capacity": reserved_b, "window_quota": reserved_w},
        })
    reason = res[1]
    if reason == "key not found":
        raise HTTPException(status_code=404, detail="key not found")
    if reason == "share not found":
        raise HTTPException(status_code=404, detail="share not found")
    if reason == "revoked":
        raise HTTPException(status_code=409,
                            detail="key is revoked; shares can only be set on active keys")
    if reason == "exceeds_total":
        # Σ份额超总量：明确告知冲突后的合计与密钥总量，便于调用方调小再试
        sum_b, sum_w, cap, win_q = (int(x) for x in res[2:6])
        raise HTTPException(status_code=409, detail={
            "code": "exceeds_total",
            "message": "sum of shares would exceed the key's total quota",
            "sum_burst_capacity": sum_b, "sum_window_quota": sum_w,
            "burst_capacity": cap, "window_quota": win_q,
        })
    raise HTTPException(status_code=400, detail=str(reason))


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
