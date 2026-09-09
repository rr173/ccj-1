# 开放接口配额系统（控制面 / 数据面分离）

一套给开放接口用的配额能力。**控制面与数据面是两个独立进程、独立镜像，
通过 Redis 中的配置与计数耦合，绝不是同一进程里的两个函数。**

## 需求对照

| 需求 | 实现方式 |
|---|---|
| 控制面/数据面分开部署 | `control_plane/`（签发/停用/查余量，端口 8000）与 `data_plane/`（每次调用扣额度，端口 8080），独立 Dockerfile、独立进程，分别水平扩缩 |
| 签发、停用密钥 | `POST /v1/keys`、`POST /v1/keys/{kid}/revoke`。明文密钥只在签发时返回一次，存储只存 SHA-256 哈希 |
| 查看当前窗口剩余额度 | `GET /v1/keys/{kid}/quota`，只读 Lua，口径与数据面完全一致 |
| 限制短时间突发 | 令牌桶（容量=突发上限，按毫秒匀速补令牌） |
| 限制长周期总量 | 相邻两个固定桶加权的**滑动窗口**，避免固定窗口边界 2x 突发 |
| 同密钥并发相对公平 | ① 数据面 per-key FIFO 锁，同密钥按到达顺序向 Redis 申请；② 判定+扣减在 Redis 单线程的一段 Lua 内原子完成，每次请求只扣自己的 1 个额度，不存在“先到的一批把额度抢光” |
| 超限告诉调用方等多久 | 429 响应体 `error.retry_after_ms` / `error.retry_after`，并带标准 `Retry-After` 头；窗口超限等到桶边界，突发超限按令牌补充速率精确计算 |
| 重启后已扣额度不丢、不增 | 判定与扣减是同一次原子落盘；Redis 开 **AOF `appendfsync always`**（每笔扣减 fsync）、关闭 RDB 回退、`noeviction`；两端自身无状态，随便重启 |
| Redis 重启后无需重启两端 | 两端对 `NOSCRIPT`（按异常类型识别并重新 `SCRIPT LOAD` 刷新 SHA）、连接拒绝、`LOADING/READONLY` 做自动重载+退避重试；Redis 恢复后下一次请求即自愈。宕机窗口返回明确 **503 + Retry-After**，不是裸 500；数据面连不上 Redis 启动时进入降级态而不是退出 |
| 停用后仍可查余量 | 停用密钥的余量查询返回 200、`state=revoked` 与其真实配置；Lua 不再返回会导致除零的占位值 |
| 两边对不上时以已对外发生的扣减为准 | **数据面只写计数，控制面只写配置**。控制面查余量时实时读计数；控制面从不回写/重置计数，历史扣减永远是事实来源 |
| Docker 部署 | `docker compose up -d --build`，含 Redis（持久化卷）、控制面、数据面、模拟上游 |

## 架构

```
                管理员
                  │ Bearer ADMIN_TOKEN
                  ▼
          ┌────────────────┐   写配置 hash（签发/停用）
          │  control-plane │────────────────┐
          │  (FastAPI)     │◀─只读查余量────┤
          └────────────────┘                │
                                            ▼
   调用方 X-Api-Key              ┌─────────────────────┐
   ────────────────────────────▶│   Redis 7（AOF）     │
                                │  {qk:id}cfg   配置    │
                                │  {qk:id}tb    令牌桶  │
                                │  {qk:id}win:* 窗口计数│
   ┌────────────────┐           │  {qk:id}dedup:* 幂等 │
   │  data-plane    │ EVAL Lua  └─────────────────────┘
   │ (aiohttp 代理) │───────────────▲ 原子判定+扣减
   │ 每密钥轮转公平  │               │
   └───────┬────────┘               │
           │ 放行后透传             │
           ▼                        │
       上游真实服务（mock_upstream）
```

关键边界：

- **控制面**写 `{qk:<kid>}cfg`；**数据面**对 cfg 只读，只写
  `{qk:<kid>}tb` / `{qk:<kid>}win:*` / `{qk:<kid>}dedup:*`。生产环境建议用
  两套 Redis ACL 用户分别授权（ACL 对 Lua 内访问的 key 同样生效）。
- 数据面**不缓存“是否放行/剩余多少”的结论**，每请求实时执行 Lua；
  因此停用在下一次调用立即生效。
- 判定时间一律取 Redis `TIME`，不相信调用方或本机时钟。
- 令牌以千分之一为单位存整数，杜绝浮点漂移凭空产生额度。

## 快速开始

```bash
docker compose up -d --build

# 签发一把密钥：突发 5 个、每 400ms 补 1 个；每 60s 总量 100
curl -s -X POST localhost:8000/v1/keys \
  -H "Authorization: Bearer change-me-admin-token" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","burst_capacity":5,"burst_refill_ms":400,
       "window_seconds":60,"window_quota":100}'
# -> {"key_id":"...","api_key":"qk_xxx", ...}  明文只出现这一次

# 调用开放接口（数据面 -> 上游）
curl -i localhost:8080/v1/hello -H 'X-Api-Key: qk_xxx'

# 查余量
curl -s localhost:8000/v1/keys/<kid>/quota \
  -H "Authorization: Bearer change-me-admin-token"

# 停用
curl -s -X POST localhost:8000/v1/keys/<kid>/revoke \
  -H "Authorization: Bearer change-me-admin-token"
```

一键端到端自检（突发、滑动窗口、公平性、Retry-After、幂等、停用、重启持久化）：

```bash
pip install httpx  # 不需要，自检脚本只用标准库
python3 demo/e2e_demo.py            # 基础项
python3 demo/e2e_demo.py --restart  # 额外重启 Redis 验证 AOF 持久化
```

## 限流返回示例

```json
HTTP/1.1 429 Too Many Requests
Retry-After: 1
X-Retry-After-Ms: 400

{
  "error": {
    "code": "burst_limited",
    "message": "burst_limited",
    "retry_after_ms": 400,
    "retry_after": 1
  },
  "remaining_burst": 0,
  "remaining_window": 96
}
```

`code` 取值：`burst_limited`（等令牌补充）、`window_limited`（等旧请求滑出
长窗口）、`key_revoked`（403）、`invalid_api_key`（401）、
`missing_api_key`（401）、`quota_store_unavailable`（503，记额度层
重启/不可用，带 `Retry-After: 2`，Redis 恢复后自动自愈、无需重启两端）。

> 停用后的密钥仍可查余量：`GET /v1/keys/{kid}/quota` 返回 200 且
> `state="revoked"`，余量显示为 0、配置口径照常返回。

## 幂等重试（重要）

调用方在请求头带 `Idempotency-Key: <业务唯一号>`：

- 同一 key 的重复请求返回首次结论（允许重放为 409
  `idempotent_replay_allowed`，拒绝重放为 429），**额度只扣一次**；
- 数据面已扣额度但上游调用失败时返回 502 **不退款**（否则超时重试会把
  同一笔业务算多次）。调用方用相同幂等键安全重试即可；
- 幂等记录默认保留 24h（`common/keys.py:DEFAULT_IDEM_TTL_SECONDS`）。

## 公平性的边界（如实说明）

- 单数据面副本内：同一密钥严格按票号仲裁；同一密钥下不同调用方
  （`X-Client-Id`，缺省取对端 IP）采用轮转调度——A、B 两个租户各发 10 个
  突发请求而只剩 10 个额度时，双方约各得一半，任一方都不会被连续成批
  放行；同一调用方自身仍是 FIFO。
- 多副本时：轮转只在副本内成立；跨副本由 Redis 单线程 Lua 保证
  **总量精确、不超发**（线性化），但“哪一方先拿到”取决于网络与副本调度，
  概率上均匀，非严格全局轮转。若业务要求跨副本也严格公平，应在数据面
  之上按密钥哈希做一致性路由（同一把密钥固定打到同一副本）。
- 当前每请求 cost=1，不存在单个大请求一次拿走多额度的场景；如需
  cost=N（如按报文大小计费），调度语义保持不变。

## 生产化清单

1. **Redis 高可用**：主从 + Sentinel 或托管版；写路径可用 `WAIT 1 <ms>`
   等待一个副本确认（牺牲一点延迟换跨机耐久）；需要自动故障转移且能接受
   Redis 语义时可用 Cluster——同一密钥的 key 都带 `{qk:<kid>}` hash tag，
   Lua 访问的 cfg/tb/win/dedup 一定同 slot，而不同密钥自然分散到不同 slot
   实现水平分片）。
2. **AOF 策略**：默认 `appendfsync always`（每笔扣减 fsync，崩溃不丢已
   对外的扣减，代价是每次约 0.1~1ms 磁盘同步）；吞吐优先可改 `everysec`，
   崩溃窗口 ≤1s 且只可能丢“尚未返回给调用方”的扣减。已关闭 RDB 快照，
   避免旧 dump 与 AOF 混用造成计数回退。
3. **脚本缓存与重连**：两端启动时 `SCRIPT LOAD`，运行期 Redis 重启导致
   `NOSCRIPT` 时由 `common/script_runner.py` 自动重载并刷新 SHA；连接
   拒绝/`LOADING`/`READONLY` 指数退避重试。因此升级或重启 Redis 后无需
   重启任何一面。
4. **管理员鉴权**：`ADMIN_TOKEN` 换成正式 IAM/JWT；管理口走内网或 mTLS。
5. **密钥元数据**：当前密钥列表用 `SCAN cfg:*`。密钥量级很大或需要
   审计/多版本配置时，控制面加 PostgreSQL 作为元数据 SoR，但**计数事实
   仍以 Redis 为准**（对账时以 `win/tb` 计数覆盖元数据中的派生值）。
6. **数据面扩缩**：无状态，直接加多副本；上游地址用 `UPSTREAM_URL`
   配置，代理默认透传 query/path/header（剔除 hop-by-hop 与密钥头）。
7. **可观测**：建议数据面对 `burst_limited/window_limited/revoked/
   quota_store_unavailable` 打点上报 Prometheus（本仓库为聚焦核心未包含）。

## 目录

```
common/               两端共享的协议（构建时各自打进镜像）
  consume.lua         数据面：原子 去重→校验→令牌桶→滑动窗口→判定→落盘
  quota.lua           控制面：只读余量查询，口径与 consume 一致
  keys.py             key 规则、hash、配置字段
  script_runner.py    NOSCRIPT 自动重载 + 连接/LOADING/READONLY 退避重试
control_plane/        FastAPI：签发/停用/列表/查余量
data_plane/          aiohttp 反向代理：per-key 轮转公平 + EVAL 扣减 + 透传上游
  fairness.py         同一密钥内按调用方轮转的 FIFO 调度器
mock_upstream/        被保护的真实服务示例（标准库）
redis/redis.conf      AOF(always) + 关闭 RDB + noeviction
demo/e2e_demo.py      端到端自检
```
