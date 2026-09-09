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

DEFAULT_IDEM_TTL_SECONDS = 86400  # 幂等记录保留 24h


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


def dedup_key(kid: str, idempotency_key: str) -> str:
    h = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    return f"{{qk:{kid}}}dedup:{h}"


def win_key(kid: str, window_seconds: int, bucket_index: int) -> str:
    return f"{{qk:{kid}}}win:{window_seconds}:{bucket_index}"
