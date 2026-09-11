"""控制面与数据面共享的 key 规则、配置口径。

两端是独立进程、独立镜像，只通过 Redis 中的“配置 hash + 计数 key + Lua 脚本”
这一协议耦合；修改本文件需要同时重建两个镜像（见 Dockerfile）。
"""
from __future__ import annotations

import hashlib
import secrets

# 配置 hash 字段
F_REVOKED = "revoked"
F_CAPACITY = "burst_capacity"        # 令牌桶容量：短周期突发上限
F_REFILL_MS = "burst_refill_ms"      # 每补充 1 个令牌的毫秒数
F_WINDOW_SECONDS = "window_seconds"  # 长周期窗口长度（秒）
F_WINDOW_QUOTA = "window_quota"      # 长周期总量
F_NAME = "name"
F_CREATED_AT = "created_at"
# 已预留给点名调用方的合计（由控制面 set_share.lua 原子维护，数据面只读）。
# 公共池配额 = 总量 - reserved；缺省 0（未设任何份额时行为与旧版一致）。
F_RESERVED_BURST = "reserved_burst"
F_RESERVED_WINDOW = "reserved_window"
F_STOPPED = "stopped"

DEFAULT_RESERVATION_TTL_SECONDS = 60  # 占用约定的回音时限：超时未了结，
                                      # 占着的额度自动退回池里给别人用


def new_api_key() -> str:
    """生成一把新密钥。只在签发时返回明文一次。"""
    return "qk_" + secrets.token_urlsafe(32)


def key_id(api_key: str) -> str:
    """密钥 ID = 明文的 SHA-256 前 32 位 hex。

    Redis 中只存哈希，不落明文；泄露配置存储不等于泄露可用密钥。
    同一个明文密钥稳定映射到同一个 kid，因此可做幂等与停用。
    """
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:32]


# 同一密钥的 key 都带 {qk:<kid>} hash tag：Lua 访问的 key 同 slot，
# 而不同密钥自然分散到 Cluster 的不同 slot。
def cfg_key(kid: str) -> str:
    return f"{{qk:{kid}}}cfg"


def tb_key(kid: str) -> str:
    return f"{{qk:{kid}}}tb"


def reservation_key(kid: str, idempotency_key: str = "") -> str:
    """占用单 key：一笔占用（先占额度、回音后了结）的凭据。

    带业务号（Idempotency-Key）时以业务号命名：同一业务号再来，命中同一张
    占用单，绝不二次占额；没带业务号时用随机串，一次调用一张单子。
    """
    if idempotency_key:
        h = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        return f"{{qk:{kid}}}res:{h}"
    return f"{{qk:{kid}}}res:{secrets.token_hex(16)}"


def win_key(kid: str, window_seconds: int, bucket_index: int) -> str:
    return f"{{qk:{kid}}}win:{window_seconds}:{bucket_index}"


def shares_key(kid: str) -> str:
    """某把密钥的“点名调用方份额表”：hash，field=调用方名，
    value=JSON {"burst_capacity":N,"window_quota":K}。
    控制面写（set_share.lua 原子校验“Σ份额 ≤ 总量”），数据面 Lua 只读。"""
    return f"{{qk:{kid}}}shares"


def caller_hash(caller: str) -> str:
    """份额计数 key 的调用方后缀。与 reserve.lua / quota.lua 中的
    redis.sha1hex(caller) 完全一致，保证两端拼出同一个 key。"""
    return hashlib.sha1(caller.encode("utf-8")).hexdigest()


def share_tb_key(kid: str, caller: str) -> str:
    """点名调用方自己的令牌桶（与公共池 {qk:<kid>}tb 完全隔离）。"""
    return f"{{qk:{kid}}}stb:{caller_hash(caller)}"


def share_win_key(kid: str, caller: str, window_seconds: int, bucket_index: int) -> str:
    """点名调用方自己的窗口计数桶（与公共池 {qk:<kid>}win:* 完全隔离）。"""
    return f"{{qk:{kid}}}swin:{caller_hash(caller)}:{window_seconds}:{bucket_index}"


# ── 占用流水（只追加账本）──────────────────────────────────────────────────
# 每把密钥一条 Redis Stream：占（reserve）/ 调成（confirm）/ 退回（release）
# 各 XADD 一笔，与计数变更在同一段 Lua 内原子落盘。Stream 没有修改单条的
# 命令，代码里也从不 XDEL/XTRIM——留下之后不能改；停用密钥不删流水，
# AOF(appendfsync always) 保证重启后流水还在。
def ledger_stream_key(kid: str) -> str:
    """占用流水主表：Redis Stream，entry id 即 Redis 服务器毫秒时间戳。"""
    return f"{{qk:{kid}}}ledger"


def ledger_caller_key(kid: str, caller: str) -> str:
    """按调用方的流水索引：ZSET，member=stream entry id，score=毫秒时间戳。
    与 Lua 内 redis.sha1hex(caller) 的拼法一致。"""
    return f"{{qk:{kid}}}lci:{caller_hash(caller)}"


def ledger_idem_key(kid: str, idem: str) -> str:
    """按业务号（Idempotency-Key）的流水索引：ZSET，同上。
    一个业务号至多占/结/退三笔，用 sha1 与 Lua 侧 redis.sha1hex 对齐。"""
    return f"{{qk:{kid}}}lii:{hashlib.sha1(idem.encode('utf-8')).hexdigest()}"


# ── 跨密钥共享额度池 ───────────────────────────────────────────────────────
# 池配置与计数使用独立 hash tag（{qp:<pid>}）；成员资格放在密钥 hash tag 下，
# 由管理脚本跨 slot 原子校验/写入。单机 Redis 可直接执行跨 key Lua；若使用
# Redis Cluster，需要为这些管理/占用脚本改用外部事务协调或同 slot 键设计。
def pool_cfg_key(pid: str) -> str:
    return f"{{qp:{pid}}}cfg"


def pool_tb_key(pid: str) -> str:
    return f"{{qp:{pid}}}tb"


def pool_members_key(pid: str) -> str:
    """池成员 hash：field=kid，value=加入时间（毫秒）。"""
    return f"{{qp:{pid}}}members"


def pool_membership_key(kid: str) -> str:
    """密钥当前归属池：string，内容为 pid；不存在表示未进池。"""
    return f"{{qk:{kid}}}pool"


def pool_reservation_key(pid: str, kid: str, idempotency_key: str = "") -> str:
    """池侧占用单。带业务号时由 (pid,kid,业务号) 决定，避免不同密钥但业务号
    相同发生碰撞；匿名调用随机一张。"""
    if idempotency_key:
        raw = f"{pid}\0{kid}\0{idempotency_key}".encode("utf-8")
        h = hashlib.sha256(raw).hexdigest()
        return f"{{qp:{pid}}}pres:{h}"
    return f"{{qp:{pid}}}pres:{secrets.token_hex(16)}"


def pool_win_prefix(pid: str) -> str:
    return f"{{qp:{pid}}}win:"

