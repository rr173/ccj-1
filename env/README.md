# 开放接口配额系统（控制面 / 数据面分离）

一套给开放接口用的配额能力。**控制面与数据面是两个独立进程、独立镜像，
通过 Redis 中的配置与计数耦合，绝不是同一进程里的两个函数。**

额度采用**占用式**：调用先占额度，调成了才算真用掉；没调成、或过了约定
回音时限没回音，占着的自动退回给别人用。

## 需求对照

| 需求 | 实现方式 |
|---|---|
| 控制面/数据面分开部署 | `control_plane/`（签发/停用/查余量，端口 8000）与 `data_plane/`（占用→调上游→了结，端口 8080），独立 Dockerfile、独立进程，分别水平扩缩 |
| 签发、停用密钥 | `POST /v1/keys`、`POST /v1/keys/{kid}/revoke`。明文密钥只在签发时返回一次，存储只存 SHA-256 哈希 |
| 先占额度再调，调成才真扣 | 数据面三段式：`reserve.lua` 原子判定+登记占用（只登记、不真扣）→ 透传上游 → `settle.lua` 了结：上游给业务应答（含 4xx）→ **confirm** 此刻才真扣；上游 5xx/连不上/调用方断连 → **release** 立即退回 |
| 过了约定时间没回音自动退回 | 占用登记在 ZSET 里按“约定回音时限”（`RESERVATION_TTL_SECONDS`，默认 60s，须大于上游总超时）记分，每次判定先清掉过期登记——数据面崩溃、调用方失联都不会把额度占死 |
| 占着的这段别人不能拿 | 判定余量 = 令牌桶/窗口余量 − 还在占用中的；占用期间这笔额度对其他人不可见 |
| 占不住时告诉还要等多久 | 429 响应体 `error.retry_after_ms` / `error.retry_after`，并带标准 `Retry-After` 头；突发超限按令牌补充速率精确计算，窗口超限等到桶边界 |
| 查余量看得出“还占着 / 真用掉” | `GET /v1/keys/{kid}/quota` 每个视角都返回 `remaining`（可再占）、`held`（还占着）、`consumed`（真用掉）三个数；只读 Lua，口径与数据面完全一致 |
| 同一业务号不再占一次 | 调用方带 `Idempotency-Key: <业务号>`：占用单以业务号命名，同一业务号再来命中同一张单子，绝不二次占额；已了结的重放返回原结论（409） |
| 停用后占着的不能当成调成 | `settle.lua` 确认前再查一次 `revoked`：密钥一停用，在飞的占用确认被拒（409 语义），占用保持原状等超时自动退回；新占用立即 403 |
| 点名的占自己那份，没点名的占公共池 | `PUT/DELETE /v1/keys/{kid}/shares/{caller}`：每人一截突发+窗口，从密钥总量里切出；占用、确认、退回都落在调用方自己的池子里，两边互不相通 |
| 限制短时间突发 | 令牌桶（容量=突发上限，按毫秒匀速补令牌） |
| 限制长周期总量 | 相邻两个固定桶加权的**滑动窗口**，避免固定窗口边界 2x 突发 |
| 同密钥并发相对公平 | ① 数据面 per-key FIFO 锁，同密钥按到达顺序向 Redis 申请；② 判定+登记在 Redis 单线程的一段 Lua 内原子完成，每次请求只占自己的 1 个额度，不存在“先到的一批把额度抢光” |
| 重启后已扣额度不丢、不增 | 判定、登记、了结都是各自原子落盘；Redis 开 **AOF `appendfsync always`**（每笔写 fsync）、关闭 RDB 回退、`noeviction`；两端自身无状态，随便重启 |
| Redis 重启后无需重启两端 | 两端对 `NOSCRIPT`（按异常类型识别并重新 `SCRIPT LOAD` 刷新 SHA）、连接拒绝、`LOADING/READONLY` 做自动重载+退避重试；Redis 恢复后下一次请求即自愈。宕机窗口返回明确 **503 + Retry-After**，不是裸 500；数据面连不上 Redis 启动时进入降级态而不是退出 |
| 停用后仍可查余量 | 停用密钥的余量查询返回 200、`state=revoked`：可再占显示 0，但**还占着 / 真用掉保持真实**（不抹账），配置口径照常返回 |
| 两边对不上时以已对外发生的扣减为准 | **数据面只写计数，控制面只写配置**。控制面查余量时实时读计数；控制面从不回写/重置计数，历史扣减永远是事实来源 |
| Docker 部署 | `docker compose up -d --build`，含 Redis（持久化卷）、控制面、数据面、模拟上游 |

## 架构

```
                管理员
                  │ Bearer ADMIN_TOKEN
                  ▼
          ┌────────────────┐   写配置 hash / 份额表（签发/停用/设份额）
          │  control-plane │────────────────┐
          │  (FastAPI)     │◀─只读查余量────┤
          └────────────────┘                │
                                            ▼
   调用方 X-Api-Key              ┌─────────────────────┐
   ────────────────────────────▶│   Redis 7（AOF）     │
                                │  {qk:id}cfg    配置   │
                                │  {qk:id}shares 份额表 │
                                │  {qk:id}tb     令牌桶 │
                                │  {qk:id}win:*  窗口计数│
   ┌────────────────┐           │  {qk:id}holds/wholds 占用登记（公共池）│
   │  data-plane    │ EVAL Lua  │  {qk:id}stb/swin:*  份额计数          │
   │ (aiohttp 代理) │───────────────▲ {qk:id}sholds/swholds:* 份额占用  │
   │ 占→调→了结     │               │  {qk:id}res:*   占用单            │
   └───────┬────────┘               │
           │ 占到才放行，调完了结    │
           ▼                        │
       上游真实服务（mock_upstream）
```

关键边界：

- **控制面**写 `{qk:<kid>}cfg` 与 `{qk:<kid>}shares`（份额表，经
  set_share.lua 原子校验“Σ份额 ≤ 密钥总量”后写入，并把预留合计回写到
  cfg 的 `reserved_burst/reserved_window`）；**数据面**对配置只读，只写
  计数与占用（`tb` / `win:*` / `holds` / `wholds` / `stb:*` / `swin:*` /
  `sholds:*` / `swholds:*` / `res:*`）。生产环境建议用两套 Redis ACL
  用户分别授权（ACL 对 Lua 内访问的 key 同样生效）。
- 数据面**不缓存“是否放行/剩余多少”的结论**，每请求实时执行 Lua；
  因此停用在下一次调用立即生效，在飞占用的确认也会被拒。
- 判定时间一律取 Redis `TIME`，不相信调用方或本机时钟。
- 令牌以千分之一为单位存整数，杜绝浮点漂移凭空产生额度。

## 占用式额度：一笔调用的生命周期

```
  调用方                数据面                 Redis                上游
    │  X-Api-Key          │                     │                   │
    │────────────────────▶│ 1) reserve.lua      │                   │
    │                     │    判定+登记占用 ───▶│ holds/res 占用单   │
    │                     │    （只登记，不真扣） │                   │
    │                     │ 2) 透传 ─────────────────────────────────▶│
    │                     │◀───────────────────────────────── 应答 ──│
    │                     │ 3) settle.lua       │                   │
    │                     │    成了→confirm ───▶│ 真扣令牌桶/窗口     │
    │◀──── 上游应答 ──────│    没成→release ───▶│ 占用登记作废        │
```

- **占（reserve）**：原子完成 幂等去重 → 校验密钥 → 选池（份额/公共）→
  清过期占用 → 令牌桶/滑动窗口判定 → 登记占用。占不住返回 429 并告知
  还要等多久；占住了，这笔额度立刻从别人的可占余量里消失。
- **了结（settle）**：上游 2xx/4xx（业务应答，调用方自己的问题也照算）→
  **confirm**，此刻才真扣令牌桶、才进窗口计数；上游 5xx / 连不上 /
  调用方中途断连 → **release**，占用登记作废，额度立即可被别人再占。
- **超时兜底**：占用后超过约定回音时限（`RESERVATION_TTL_SECONDS`，
  默认 60s，必须大于上游总超时）还没了结，占用登记自动失效——
  数据面崩溃、调用方失联都不会把额度占死。迟到的确认仍会按真实调用
  记账（可能让总量短暂超出，这是“宁可在占时保守、也不丢真实调用”的
  取舍；把回音时限调到大于上游 p99 延迟即可避免）。

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

# 调用开放接口（数据面 -> 上游）；建议带上业务号做幂等
curl -i localhost:8080/v1/hello \
  -H 'X-Api-Key: qk_xxx' -H 'Idempotency-Key: order-123'

# 查余量（总盘 + 公共池 + 各份额；每个视角都有 可再占/还占着/真用掉）
curl -s localhost:8000/v1/keys/<kid>/quota \
  -H "Authorization: Bearer change-me-admin-token"

# 给调用方 alice 预留一截：突发 2、窗口 20（从密钥总量里切出）
curl -s -X PUT localhost:8000/v1/keys/<kid>/shares/alice \
  -H "Authorization: Bearer change-me-admin-token" \
  -H 'Content-Type: application/json' \
  -d '{"burst_capacity":2,"window_quota":20}'
# 之后 alice（X-Client-Id: alice）只占用自己的 2/20；其他调用方只能占
# 公共池（突发 3、窗口 80）。Σ份额超总量会被 409 拒绝；取消预留：
#   curl -X DELETE localhost:8000/v1/keys/<kid>/shares/alice ...

# 停用
curl -s -X POST localhost:8000/v1/keys/<kid>/revoke \
  -H "Authorization: Bearer change-me-admin-token"
```

一键端到端自检（突发、滑动窗口、公平性、占用→确认/退回/超时、幂等、
停用、份额、重启持久化）：

```bash
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
重启/不可用，带 `Retry-After: 2`，Redis 恢复后自动自愈、无需重启两端）、
`upstream_unavailable`（502，这次没调成，**占着的额度已退回**）、
`idempotent_replay_confirmed` / `idempotent_replay_released`（409，
同一业务号重放，见下节）。

> 停用后的密钥仍可查余量：`GET /v1/keys/{kid}/quota` 返回 200 且
> `state="revoked"`，可再占显示 0，还占着/真用掉照实，配置口径照常返回。

## 幂等重试（同一业务号）

调用方在请求头带 `Idempotency-Key: <业务唯一号>`：

- 占用单以业务号命名：**同一业务号再来，命中同一张占用单，绝不二次占额**；
- 首次调用已确认调成的，重放返回 409 `idempotent_replay_confirmed`，
  不再接触上游、不再占额；
- 首次调用没调成（占用已退回）的，重放返回 409
  `idempotent_replay_released`——这笔业务已了结为失败，**换一个业务号**
  才能再试（避免把“没调成”的账算两次）；
- 占用还在约定回音时限内（上次可能调到一半数据面崩了）：重试直接带着
  同一张占用单调上游，调完照常了结，**同一笔业务只算一次**；
- 占用单保留 24h（占用本身的额度效力只到约定回音时限，过期自动退回）。

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
   Lua 访问的 cfg/tb/win/holds/res 一定同 slot，而不同密钥自然分散到不同
   slot 实现水平分片）。
2. **AOF 策略**：默认 `appendfsync always`（每笔写 fsync，崩溃不丢已
   对外的占用/确认，代价是每次约 0.1~1ms 磁盘同步）；吞吐优先可改
   `everysec`，崩溃窗口 ≤1s 且只可能丢“尚未返回给调用方”的占用。已关闭
   RDB 快照，避免旧 dump 与 AOF 混用造成计数回退。
3. **回音时限**：`RESERVATION_TTL_SECONDS` 必须大于上游调用的总超时
   （默认 60s vs 数据面上游超时 30s）。调小了，慢上游的占用会被提前退回，
   迟到的确认仍会计账（总量可能短暂超出）；调大了，崩溃残留的占用退回
   变慢。占用登记与占用单都带 TTL，不会泄漏。
4. **脚本缓存与重连**：两端启动时 `SCRIPT LOAD`，运行期 Redis 重启导致
   `NOSCRIPT` 时由 `common/script_runner.py` 自动重载并刷新 SHA；连接
   拒绝/`LOADING`/`READONLY` 指数退避重试。因此升级或重启 Redis 后无需
   重启任何一面。
5. **管理员鉴权**：`ADMIN_TOKEN` 换成正式 IAM/JWT；管理口走内网或 mTLS。
6. **密钥元数据**：当前密钥列表用 `SCAN cfg:*`。密钥量级很大或需要
   审计/多版本配置时，控制面加 PostgreSQL 作为元数据 SoR，但**计数事实
   仍以 Redis 为准**（对账时以 `win/tb` 计数覆盖元数据中的派生值）。
7. **数据面扩缩**：无状态，直接加多副本；上游地址用 `UPSTREAM_URL`
   配置，代理默认透传 query/path/header（剔除 hop-by-hop 与密钥头）。
8. **可观测**：建议数据面对 `burst_limited/window_limited/revoked/
   quota_store_unavailable/upstream_unavailable` 与占用超时退回打点上报
   Prometheus（本仓库为聚焦核心未包含）。

## 目录

```
common/               两端共享的协议（构建时各自打进镜像）
  reserve.lua         数据面：原子 去重→校验→选池(份额/公共)→清过期占用→
                      令牌桶→滑动窗口→判定→登记占用（只登记、不真扣）
  settle.lua          数据面：了结占用——confirm 真扣 / release 退回，幂等
  quota.lua           控制面：只读余量查询（总盘+公共池+各份额，
                      每个视角都有 可再占/还占着/真用掉），口径与 reserve 一致
  set_share.lua       控制面：原子校验“Σ份额 ≤ 总量”并写入份额表
  keys.py             key 规则、hash、配置字段
  script_runner.py    NOSCRIPT 自动重载 + 连接/LOADING/READONLY 退避重试
control_plane/        FastAPI：签发/停用/列表/查余量/设份额
data_plane/          aiohttp 反向代理：per-key 轮转公平 + 占用→透传→了结
  fairness.py         同一密钥内按调用方轮转的 FIFO 调度器
mock_upstream/        被保护的真实服务示例（标准库；/fail 模拟故障、
                      /slow?ms=N 模拟慢上游，用于验证退回与超时）
redis/redis.conf      AOF(always) + 关闭 RDB + noeviction
demo/e2e_demo.py      端到端自检
```

## 按调用方预留份额（同一把密钥内的额度隔离）

- 份额从密钥总量里切出：每人一份 `burst_capacity` + `window_quota`，补充速率与
  窗口长度沿用密钥配置。`set_share.lua` 在 Redis 内原子校验
  **Σ份额 ≤ 密钥总量**（突发、窗口两个维度分别约束），多控制面并发也不会超分；
  剩余部分即公共池，留给未点名调用方。
- 判定在 `reserve.lua` 内原子完成：请求带 `X-Client-Id` 命中份额表 → 只占自己
  的预留池（独立令牌桶 `{qk:<kid>}stb:<sha1(caller)>`、窗口计数
  `{qk:<kid>}swin:*` 与占用登记 `{qk:<kid>}sholds/swholds:<sha1(caller)>`），
  **自己的占满了公共池还有也不能拿**，429 照常给出 `retry_after_ms`；
  未命中 → 只占公共池（`总量 − Σ预留`），**吃不到别人留的**。
- 占用、确认、退回都落在调用方自己的池子里：点名的调成了扣自己那份，
  没调成退回自己那份；没点名的占用公共池，互不相通。
- 查余量：`GET /v1/keys/{kid}/quota` 一次返回总盘（顶层 `burst/window`）、
  公共池（`shared_pool`）与每人的配额、余量、在占数（`shares[]`）。
- 密钥停用后所有份额与公共池**立即**失效（revoked 检查在任何份额逻辑
  之前）；在飞的占用确认也会被拒，等约定时限自动退回；停用密钥不能再设
  份额（409）；份额配置保留，余量查询照常返回口径。
- 取消份额（`DELETE`）后预留立即回到公共池；该调用方已发生的扣减计数原样
  保留（有 TTL 自然过期），不回写、不补偿。
