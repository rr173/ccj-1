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

DEFAULT_RESERVATION_TTL_SECONDS = 60  # 占用约定的回音时限：超时未了结，
                                      # 占着的额度自动退回池里给别人用

# 临时通行证令牌前缀（qk pass），与密钥 qk_ 区分
PASS_PREFIX = "qkp_"
# 提前吊销的通行证在通行证表里保留多久（等在飞占用按回音时限退回后再清表）。
# 需覆盖数据面 RESERVATION_TTL_SECONDS 的最大值（默认 60s）。
PASS_REVOKED_RETENTION_MS = 120_000


def new_api_key() -> str:
    """生成一把新密钥。只在签发时返回明文一次。"""
    return "qk_" + secrets.token_urlsafe(32)


def new_pass_token(kid: str) -> str:
    """生成一张临时通行证的明文令牌（只在签发时返回一次）。

    令牌里自带密钥 ID：qkp_<kid>_<secret>。数据面凭令牌即可定位密钥，
    不必维护跨密钥的反查索引（Cluster 下也不会跨 slot）；存储只留哈希。
    """
    return PASS_PREFIX + kid + "_" + secrets.token_urlsafe(24)


def parse_pass_token(token: str) -> tuple[str, str] | None:
    """解析通行证令牌 -> (kid, secret)；非法令牌返回 None。"""
    if not token or not token.startswith(PASS_PREFIX):
        return None
    rest = token[len(PASS_PREFIX):]
    parts = rest.split("_", 1)
    if len(parts) != 2:
        return None
    kid, secret = parts
    # kid 固定为 32 位十六进制（见 key_id）
    if len(kid) != 32 or any(c not in "0123456789abcdef" for c in kid) or not secret:
        return None
    return kid, secret


def pass_id(token: str) -> str:
    """通行证 ID = 明文令牌的 SHA-256 前 32 位 hex（与 key_id 同口径）。

    Redis 中只存哈希，泄露配置存储不等于泄露可用通行证。
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]


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


def passes_key(kid: str) -> str:
    """某把密钥的“临时通行证表”：hash，field=通行证ID(pass_id)，
    value=JSON（grantee/cap/wq/exp/created/revoked）。
    控制面写（issue_pass.lua 原子校验并切额），数据面 Lua 只读。
    已作废（到期/停用/吊销）的条目仍短期保留，用于把在飞占用等回。"""
    return f"{{qk:{kid}}}passes"


def pass_cfg_key(kid: str, pid: str) -> str:
    """单张通行证的配置 hash（{qk:<kid>}pcfg:<pid>）。
    数据面每次占额前实时读它——到点/吊销后下一次占额立即失效。"""
    return f"{{qk:{kid}}}pcfg:{pid}"


def pass_tb_key(kid: str, pid: str) -> str:
    """通行证自己的令牌桶（与密钥公共池/份额桶完全隔离）。"""
    return f"{{qk:{kid}}}ptb:{pid}"


def pass_win_prefix(kid: str, pid: str) -> str:
    """通行证自己的滑动窗口计数前缀。"""
    return f"{{qk:{kid}}}pwin:{pid}:"


def pass_holds_key(kid: str, pid: str) -> str:
    """通行证突发占用登记（ZSET，与份额 sholds 同构）。"""
    return f"{{qk:{kid}}}pholds:{pid}"


def pass_wholds_key(kid: str, pid: str) -> str:
    """通行证窗口占用登记（ZSET）。"""
    return f"{{qk:{kid}}}pwholds:{pid}"


def pass_reservation_key(kid: str, pid: str, idempotency_key: str = "") -> str:
    """通行证占用单 key。

    与密钥占用单（res:）分命名空间（pres:），且把通行证 ID 编进 key：
    同一张通行证带同一业务号再来命中同一张单子、绝不二次占额；
    两张通行证各带同一个业务号则是两笔不同业务，各占各的。
    """
    if idempotency_key:
        h = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        return f"{{qk:{kid}}}pres:{pid}:{h}"
    return f"{{qk:{kid}}}pres:{pid}:{secrets.token_hex(16)}"


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
