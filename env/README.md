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
| 查余量看得出“还占着 / 真用掉” | `GET /v1/keys/{kid}/quota` 每个视角都返回 `remaining`（可再占）、`held`（还占着）、`consumed`（真用掉，冲正后立即减少）、`reversal_credit`（冲正退回、当前超出配额的溢余部分）四个数；只读 Lua，口径与数据面完全一致 |
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
| 占用流水（占到/调成/退回/冲正各一笔，留了不能改） | 每把密钥一条只追加 Stream `{qk:id}ledger`，在 reserve/settle/sweep 的 Lua 内与计数变更**同一原子动作** XADD，冲正在 reverse.lua 内同样原子追加一笔；只 XADD，从不 XDEL/XTRIM/改写；按调用方 `lci:`、按业务号 `lii:` 各挂 ZSET 索引。`GET /v1/keys/{kid}/ledger` 按密钥 + 调用方/业务号/时间段翻 |
| 同一业务号占、结、退、冲各只能记一笔 | 幂等由占用单状态机保证（`""→confirmed/released/expired`）：已终结的单子再来只回原结论、不产生第二笔；超时退回的单子迟到真回音可补一笔 `confirm(late=1)`，退回流水仍只有 timeout 那笔；冲正由 reverse.lua 查业务号流水索引+占用单标记双重去重，一个业务号至多一笔 `reversal`，重复提交幂等回原结论 |
| 对账单：占过/真用掉/退回/还占着 | `GET /v1/keys/{kid}/statement?start=&end=`：区间内 reserve/confirm/release 累计、`held_open` 还占着的单独列（不算真用掉），并把**流水侧真用掉与余量 win 计数侧真用掉**对账，对不上时 `ledger_confirmed`、`counter_consumed`、`difference` 两边数都亮出 |
| 密钥停了流水还在、对账单还能拉 | revoke 只改 cfg.revoked，不碰 Stream/索引；ledger/statement 对 revoked 密钥照常返回；重启后流水随 AOF(appendfsync always) 落盘不丢，两端无需重启自动自愈 |
| 冲正已真用掉的一笔 | `POST /v1/keys/{kid}/reversals`，`reverse.lua` 原子完成：只能冲**有效密钥**上、**已调成**的那一笔（写明业务号 + 数量）；一笔业务号只能冲一次（重复提交幂等回原结论），冲的数量不能超过那笔当时真用掉的；冲回的量立刻退回原池令牌桶/原窗口桶，**立刻能再占**；冲正自己在只追加流水上留一笔 `reversal`，之后不能改。密钥停用后拒绝新冲正，已冲过的流水/对账单仍可翻 |
| 多把有效密钥共用一个跨密钥额度池 | `POST /v1/pools` 开池并写明突发/窗口上限；`PUT/DELETE /v1/pools/{pid}/keys/{kid}` 放入/拿出密钥。数据面每次占用必须同时占住“密钥自己的额度”和“池聚合额度”；任一边占不住即拒绝并给 Retry-After。`GET /v1/pools/{pid}` 查池剩余/在池密钥/还占着/真用掉，密钥余量接口也带 `quota_pool` |
| 密钥换新（换调用明文，不换配额身份） | `POST /v1/keys/{kid}/rotate` 给**还有效**的密钥换新明文，并写明旧明文宽限多久（`grace_seconds`）。宽限期内新旧两把都能占，吃的是这把密钥**同一份**突发/窗口（真用掉不清零、桶不重填）；宽限过点或 `POST /v1/keys/{kid}/rotate/retire` 提前收掉后旧明文不能再占**新的**；用旧明文开了头还没结的在飞单照样结完。`GET /v1/keys/{kid}/quota` 的 `rotation` 节列出此刻哪些明文还能用、旧的还剩多久、新旧各自真用掉多少。同一业务号跨新旧明文命中同一占用单，不二次占；上一档宽限没到不能再换 |
| 两把有效密钥间划转当前可再占额度 | `POST /v1/keys/{kid}/transfers` 写明目标密钥、划转号、突发/窗口各划多少；`transfer.lua` 原子保证两边同时做成。只能划源密钥此刻“未真用、未在占”的余量，不能超过目标密钥原配额下的空位；任一头停用、源不足、目标空位不足都整笔拒绝。旧占用单不碰，仍按原池结完。同一划转号原参数重放回原结论、不二次划转，参数不同返回冲突；`GET /v1/keys/{kid}/transfers` 查划出/划入累计和每笔明细，`GET /v1/transfers/{id}` 按号查 |
| 按调用方熔断 | `PUT /v1/keys/{kid}/circuit-breaker` 给有效密钥配置连续失败次数和冷静秒数；状态按 `(密钥, X-Client-Id)` 分开。真实调用 5xx/连不上/调用方断连，或过了约定回音时限没回音，记一笔连续没成；次数到了只断这个人，返回 503 `caller_circuit_open` 和还要等多久，其他调用方照常占。断着期间已占住的旧调用继续结；调成清零连续失败。冷静到点后 Redis 原子只放一笔试探，试探成了才解开，败了立刻再断且冷静重新算；池满/池停导致没进上游只释放试探位，不算失败。`GET .../circuit-breaker` 查每个调用方的连续失败、剩余冷静、是否正在试；`POST .../circuit-breaker/{caller}/reset` 可提前手动解开。同一业务号始终回第一次结论，熔断不二次占额；密钥停用后不能占新，熔断状态仍可查，AOF 保证重启后仍断 |
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
  cfg 的 `reserved_burst/reserved_window`）；**冲正**（reverse.lua）是控制面
  唯一触碰计数的路径——只在管理员对一笔已确认真用掉做事实修正时，原子地把量
  退回**数据面当初写下的同一个**令牌桶/窗口桶并追加一笔 reversal 流水，不新建
  计数、不改写任何历史。正常运行路径下**数据面**对配置只读，只写
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
# 取消预留：
#   curl -X DELETE localhost:8000/v1/keys/<kid>/shares/alice ...

# 给这把密钥配置按调用方熔断：连续 3 次真实没调成/超时没回音就断，冷静 30s
curl -s -X PUT localhost:8000/v1/keys/<kid>/circuit-breaker \
  -H "Authorization: Bearer change-me-admin-token" \
  -H 'Content-Type: application/json' \
  -d '{"failure_threshold":3,"cooldown_seconds":30}'

# 查询哪些调用方正断着、连续失败几次、还要多久、是否正在放唯一试探
curl -s localhost:8000/v1/keys/<kid>/circuit-breaker \
  -H "Authorization: Bearer change-me-admin-token"

# 提前手动解开 alice（清连续失败；旧试探立即失效）
curl -s -X POST localhost:8000/v1/keys/<kid>/circuit-breaker/alice/reset \
  -H "Authorization: Bearer change-me-admin-token"

# 停用
curl -s -X POST localhost:8000/v1/keys/<kid>/revoke \
  -H "Authorization: Bearer change-me-admin-token"
```

一键端到端自检（突发、滑动窗口、公平性、占用→确认/退回/超时、幂等、
停用、份额、重启持久化）：

```bash
python3 demo/e2e_demo.py            # 基础项
python3 demo/e2e_demo.py --restart  # 额外重启 Redis 验证 AOF 持久化
python3 demo/failover_demo.py       # 主备顶上
python3 demo/rotation_demo.py       # 密钥换新（新旧明文宽限/收掉/停用/幂等）
python3 demo/transfer_demo.py       # 跨密钥额度划转（原子/余量/在飞/幂等/查询）
python3 demo/circuit_breaker_demo.py # 按调用方熔断（阈值/冷静/唯一试探/手动解开/幂等）
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
`missing_api_key`（401）、`api_key_retired`（403，出示的旧明文已过换新宽限
或被提前收掉，需改用当前明文）、`quota_store_unavailable`（503，记额度层
重启/不可用，带 `Retry-After: 2`，Redis 恢复后自动自愈、无需重启两端）、
`pool_stopped`（403，跨密钥共享池已停，新占用和在飞确认都不算）、
`caller_circuit_open`（503，这个调用方在这把密钥上熔断；`retry_after_ms`
是剩余冷静时间，冷静到点后系统只放一笔试探，其他并发请求仍被拒）、
`upstream_unavailable`（502，这次没调成，**占着的额度已退回**）、
`idempotent_replay_confirmed` / `idempotent_replay_released` /
`idempotent_replay_timeout`（409，同一业务号重放：已确认 / 已退回 /
约定时限过了没回音已按超时退回，后两者都要换新业务号再试；见下节）。

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

## 占用流水与对账单

每笔占、调（确认）、退都在同一段 Lua 里与计数变更原子留一笔，冲正则由
reverse.lua 在退回计数的同一原子动作里留一笔，落在该密钥专属的只追加
Stream `{qk:<kid>}ledger`（entry id 即 Redis 毫秒时间戳），另按调用方
（`{qk}lci:<sha1(caller)>`）、业务号（`{qk}lii:<sha1(业务号)>`）各挂一个 ZSET
索引。**代码里只有 XADD，没有 XDEL/XTRIM/改写路径**——留下之后不能改；
密钥停用、两端重启都不影响已落盘流水（AOF `appendfsync always`）。

每笔流水字段：`kind`（reserve/confirm/release/**reversal**）、`caller`、`idem`、
`pool`（shared/share）、`cost`、`reason`（release 时 upstream/timeout，冲正为
reversal）、`late`（约定时限过后才赶到的迟到确认，为 1）、`res`（占用单号）、
`cred`（这笔实际出示的明文哈希，换新后用于区分新旧明文各真用掉多少）；
冲正另有 `of`（被冲那笔当时真用掉的量）、`note`（管理员备注）。

翻流水（控制面，管理员鉴权；时间参数给秒或毫秒都行，end 不含）：

```bash
# 按密钥（必选）翻全部
curl -s "localhost:8000/v1/keys/<kid>/ledger?order=desc&limit=200" -H "Authorization: Bearer $ADMIN"
# 叠加：按调用方 / 按业务号 / 按时间段
curl -s "localhost:8000/v1/keys/<kid>/ledger?caller=alice" -H ...
curl -s "localhost:8000/v1/keys/<kid>/ledger?idem_key=order-123" -H ...
curl -s "localhost:8000/v1/keys/<kid>/ledger?start=1789000000&end=1789003600" -H ...
```

对账单（`GET /v1/keys/{kid}/statement?start=&end=`，可加 `caller=` 只出某
调用方的账）：

- `totals.reserved/confirmed/released`：这段**占过多少、净真用掉多少、退回多少**
  （`released_upstream`=没调成即时退，`released_timeout`=过约定时限没回音自动退）；
  对账单按**占用单的最终结局归并**，不是逐笔流水累加，因此三类互斥且
  `reserved = confirmed_gross + released + held_pending`：一笔单只要最终调成
  （哪怕先超时退回过、之后迟到确认），只计入 `confirmed_gross`（迟到量单列
  `confirmed_late`），**绝不再计入退回**；只有最终没调成的才计入 `released`；
  拉账单时还没结局的计入 `held_pending`；
- **冲正单独列**：`totals.reversed` 是这段【发生】的冲正合计，`reversals[]`
  逐笔写明冲哪个业务号、冲多少、被冲那笔当时真用掉多少（`confirmed_cost`）、
  被冲业务是不是本区间调成的（`confirmed_in_range`）；
- **真用掉按净额、不按冲正前**：`totals.confirmed = confirmed_gross −
  reversed_of_confirmed_in_range`，余量里的真用掉也同步变少；更早调成、这段
  才冲回的计入 `reversed_carried`（冲正单列，但不冲减本区间净真用掉——历史
  时点的账不能被未来改写，冲正自己那笔落在哪段就算在哪段）；
- `held_open`：**还占着的单独列**（区间内占的 `items_in_range`、更早挂过来的
  `items_carried` 分开），这些不计入真用掉；
- `reconciliation`：流水侧**净额**真用掉（每笔冲正挂回它原确认落的固定桶扣减，
  区间对齐到桶）与余量侧真用掉（同一批 `win/swin` 计数——冲正时
  `reverse.lua` 就是从原确认桶 `INCRBY -amount`，和 `GET /quota` 同口径）对照。
  一致则 `matched=true`；**对不上 `matched=false` 且两边的数都亮出**：
  `ledger_confirmed`（净）、`ledger_confirmed_gross`、`ledger_reversed`、
  `counter_consumed`、`difference`，并在 `note` 给出原因
  （迟到确认记进了后一个桶 / 区间超出计数桶 2 窗口 TTL / 未点名调用方不可
  按 X-Client-Id 归因）。

## 冲正（把已经真用掉的冲回去）

管理员可以对一笔**已经调成（真用掉）**的业务做冲正，由控制面执行
（冲正是对账期的事实修正，与签发/份额同属管理动作；脚本与数据面共用同一批
计数 key，不经过数据面转发）：

```bash
curl -s -X POST localhost:8000/v1/keys/<kid>/reversals \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"idem_key":"order-123","amount":1,"note":"对账冲回"}'
# -> 201 {"reversal_id":"<stream entry id>","idem_key":"order-123",
#         "amount":1,"confirmed_cost":1,
#         "refunded":{"burst":true,"window":true}, ...}
```

`reverse.lua` 在 Redis 单线程内原子保证全部硬规则：

1. **只能冲有效密钥**：密钥不存在 404、已停用 409（`key_revoked`）。停用后
   这道门永久关上；已经冲过的流水仍可翻、对账单仍可拉。
2. **只能冲已经调成的那一笔**：以业务号索引翻这笔业务的流水，必须存在
   `confirm`；还占着 / 已退回 / 查无此业务号一律 409（`not_confirmed`）。
   因此请求必须带当初调用时的 `Idempotency-Key`（业务号），没带业务号的匿名
   调用没有业务号可点名，不可冲正。
3. **写明冲哪一笔、冲多少**：`idem_key` 点名业务号，`amount` 给数量；
   `amount` 必须是正整数且 **≤ 那笔当时真用掉的量**（`confirmed_cost`），
   超出 422（`amount_exceeds`）。当前每请求 cost=1，实际只能冲 1。
4. **一笔业务号只能冲一次**：流水里已有该业务号的 reversal（或占用单上留过
   标记）时，重复提交 **200 + `already_reversed=true`** 幂等返回上一次结论，
   不二次退额度、不写第二笔流水。不支持“分多次冲”。
5. **冲过的额度立刻回到还能再占**：突发维度把令牌加回**原池令牌桶**（允许
   暂时高于桶容量——溢余令牌不随时间补充被 `min(cap)` 抹掉，与 confirm 允许
   扣成负数的“先花后补”对称）；窗口维度从当初 confirm 落的**同一个固定桶**
   `INCRBY -amount`（settle.lua 确认时把桶 key 记在占用单 `cbucket` 上；
   老单子按 confirm 流水时间反推）。计数 key 已过 TTL 老化的维度无账可退，
   对应 `refunded` 标志为 false，不凭空新建溢余/负数 key。
6. **冲正自己也留一笔，留下之后不能改**：与计数退回在**同一段 Lua 的同一
   原子动作**里 XADD 一笔 `kind=reversal` 到只追加 Stream，挂同样的调用方/
   业务号 ZSET 索引；代码里没有 XDEL/XTRIM/改写路径。
7. **退回哪个池以原单为准**：点名调用方的冲正退回他自己的份额池
   （`stb/swin`），未点名的退回公共池，两边互不相通。占用单已过期时按
   confirm 流水里的 `pool/caller` 与当前配置重建（注：业务调成后其份额被
   删除的极少数情况下，重建的桶 key 可能已随 TTL 消失，该维度按“无账可退”
   处理；流水事实不变）。

余量视图（`GET /v1/keys/{kid}/quota`）在冲正后：`consumed`（真用掉）立即
减少、`remaining`（可再占）立即增加；冲正净退回超过当前自然用量时
`consumed=0`，超出容量的部分单列在每个视角的 `reversal_credit` 字段里。

约定时限内没回音的占用有两条终结路径，都会留 `release/reason=timeout` 一笔：
① 下一次同池占用时 reserve.lua 顺手清；② 数据面后台 sweep 任务
（`SWEEP_INTERVAL_SECONDS`，默认 2s）扫所有 holds 登记兜底，冷池也不挂死。
迟到的真实回音随后赶到，仍把单子推进为 confirmed 并补一笔 `confirm(late=1)`
（宁可在占时保守、也不丢真实调用）；同一业务号在超时退回后再来返回 409
`idempotent_replay_timeout`，换新业务号才能再试。

> 流水是事实：同一单的 `release(timeout)` 与迟到的 `confirm(late)` 两笔都保留、
> 都不能改；对账单只负责按最终结局归并展示。控制面向后补结局的宽限窗由
> `SETTLE_LOOKAHEAD_MS`（默认 300000ms）控制，需大于回音时限 `RESERVATION_TTL_SECONDS`。

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
3. **Redis 部署形态**：当前同一把密钥的脚本同 slot，可水平分片；跨密钥共享池的
   Lua 会同时访问 `{qk:<kid>}` 与 `{qp:<pid>}`，因此该能力适用于单机/主从
   Redis。Redis Cluster 部署若要启用共享池，应先把池归属与配对占用调整为同
   slot 数据模型，或改为带补偿的两阶段事务协调。
4. **回音时限**：`RESERVATION_TTL_SECONDS` 必须大于上游调用的总超时
   （默认 60s vs 数据面上游超时 30s）。调小了，慢上游的占用会被提前退回，
   迟到的确认仍会计账（总量可能短暂超出）；调大了，崩溃残留的占用退回
   变慢。占用登记与占用单都带 TTL，不会泄漏。
5. **脚本缓存与重连**：两端启动时 `SCRIPT LOAD`，运行期 Redis 重启导致
   `NOSCRIPT` 时由 `common/script_runner.py` 自动重载并刷新 SHA；连接
   拒绝/`LOADING`/`READONLY` 指数退避重试。因此升级或重启 Redis 后无需
   重启任何一面。
6. **管理员鉴权**：`ADMIN_TOKEN` 换成正式 IAM/JWT；管理口走内网或 mTLS。
7. **密钥元数据**：当前密钥列表用 `SCAN cfg:*`。密钥量级很大或需要
   审计/多版本配置时，控制面加 PostgreSQL 作为元数据 SoR，但**计数事实
   仍以 Redis 为准**（对账时以 `win/tb` 计数覆盖元数据中的派生值）。
8. **数据面扩缩**：无状态，直接加多副本；上游地址用 `UPSTREAM_URL`
   配置，代理默认透传 query/path/header（剔除 hop-by-hop 与密钥头）。
9. **可观测**：建议数据面对 `burst_limited/window_limited/revoked/
   quota_store_unavailable/upstream_unavailable` 与占用超时退回打点上报
   Prometheus（本仓库为聚焦核心未包含）。
10. **流水增长**：`ledger` Stream 与调用方/业务号索引只追加、不设 TTL、
   不裁剪（“留下之后不能改”的硬要求）。长期运行需要在 Redis 之外做归档：
   用 `XRANGE`/`XREAD` 增量消费（entry id 即毫秒时间戳，天然支持断点续传），
   落到对象存储/数仓后再按合规期限决定是否离线保存；在线 Stream 的裁剪必须
   走独立的、有审计的运维流程，应用本身不提供任何改写/删除入口。

## 目录

```
common/               两端共享的协议（构建时各自打进镜像）
  reserve.lua         数据面：原子 去重→校验→选池(份额/公共)→清过期占用→
                      令牌桶→滑动窗口→判定→登记占用（只登记、不真扣），
                      并 XADD 一笔 reserve 流水；顺手终结已过期占用（timeout 流水）
  settle.lua          数据面：了结占用——confirm 真扣 / release 退回，幂等，
                      confirm/release 各 XADD 一笔流水（迟到确认标 late=1），
                      确认时记下落入的窗口桶（cbucket）供冲正退回原桶
  reverse.lua         控制面：冲正一笔已调成的真用掉——校验有效密钥/已调成/
                      未冲过/不超原量，退回原池令牌桶与原窗口桶，XADD 一笔
                      reversal（幂等，一笔业务号只能冲一次）
  sweep.lua           数据面后台兜底：把冷池里过期未回音的占用终结为超时退回
  quota.lua           控制面：只读余量查询（总盘+公共池+各份额，
                      每个视角都有 可再占/还占着/真用掉/冲正溢余），口径与 reserve 一致
  set_share.lua       控制面：原子校验“Σ份额 ≤ 总量”并写入份额表
  pool_reserve.lua    数据面：跨密钥共享池的第二道原子占用（密钥+池两边都占住）
  pool_member.lua     控制面：密钥入池/出池，原子保证一把密钥只在一个池
  pool_quota.lua      控制面：只读共享池聚合余量/在占/成员数
  rotate.lua          控制面：密钥换新——写“明文哈希→逻辑kid”别名与宽限状态，
                      不清零/不重填；上一档宽限没到拒绝再换
  rotate_retire.lua   控制面：宽限期没到提前收掉上一版旧明文（在飞单不碰）
  transfer.lua        控制面：两把有效密钥原子划转当前可再占突发/窗口（幂等、
                      只划未真用且未在占的余量，不超目标空位，不碰在飞单）
  keys.py             key 规则、hash、配置字段（含 ledger/共享池/换新/划转规则）
  script_runner.py    NOSCRIPT 自动重载 + 连接/LOADING/READONLY 退避重试
control_plane/        FastAPI：签发/停用/列表/查余量/设份额/冲正/翻流水/拉对账单
  ledger.py           占用流水查询（按密钥+调用方/业务号/时间段）与对账单聚合
data_plane/          aiohttp 反向代理：per-key 轮转公平 + 占用→透传→了结，
                      后台 sweep 任务兜底超时占用
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

## 密钥换新（换调用明文，不换配额身份）

密钥泄露、定期轮换时，给一把**还有效**的密钥换一把新的调用明文，配额账本
一笔不动。换的是“认证明文”，不是密钥本体：逻辑 `kid`（=签发时那把明文的
SHA-256 前 32 位）名下的 cfg / 令牌桶 / 窗口计数 / 占用登记 / 份额 / 流水 /
共享池 / 主备关系全部原样；新明文只多一条“明文哈希 → 逻辑 kid”的别名
（`{qk:<cred32>}rcr`）与一份换新状态（`{qk:<kid>}rstat`）。

```bash
# 换新：写明旧明文还能再用多久（宽限期，秒；默认 24h，最长 30 天）
curl -s -X POST localhost:8000/v1/keys/<kid>/rotate \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"grace_seconds":86400}'
# -> {"key_id":"<逻辑kid>","api_key":"qk_新明文（只返回这一次）",
#     "current_key_id":"<新明文哈希>","previous_key_id":"<上一版明文哈希>",
#     "grace_until_ms":...}

# 宽限期没到时提前把旧明文收掉（收掉以后旧的不能再占新的）
curl -s -X POST localhost:8000/v1/keys/<kid>/rotate/retire -H "Authorization: Bearer $ADMIN"

# 查：此刻哪些明文还能用、旧的还剩多久、新旧各自真用掉多少
curl -s localhost:8000/v1/keys/<kid>/quota -H "Authorization: Bearer $ADMIN"
```

硬规则（reserve.lua 的换新门与 rotate.lua / rotate_retire.lua 在 Redis
单线程内原子保证）：

- **换新不清账、不重填**：rotate 只写别名与换新状态，绝不碰令牌桶/窗口计数/
  占用登记。已经真用掉的还是那些数，突发和窗口也不会被重新装满。
- **宽限期内新旧共用一份**：两把明文解析到同一个逻辑 kid，占的是同一批
  holds、扣的是同一个 `tb`/`win:*`，不会“各算各的”多出一份突发或窗口。
- **过点 / 提前收掉**：宽限时间过了，或管理员在宽限期内 `retire`，旧明文
  立刻不能再占**新的**（数据面返回 403 `api_key_retired`）；新明文照常。
- **在飞单不受牵连**：换新门只拦“新占”，幂等判定在它之前。用旧明文开了头
  还没结的占用（占用单上记着 `cred`），旧明文作废后照样能带着原单调完、
  确认进同一份计数——**不会因为明文作废就把额度放掉**。
- **同一业务号不占两次**：占用单以 `(逻辑 kid, 业务号)` 命名，用旧明文占过
  的业务号换新明文再来命中同一张单子，按原结论 409，绝不当成另一笔再占。
- **上一档宽限没到不能再换**：`rotate` 返回 409 `rotation_in_grace` 并带上
  `grace_until_ms`；只有宽限**自然到期**后才能再换——提前 `retire` 收掉旧明文
  只是让旧的不能再占新的，**不缩短说好的宽限时间**。再换后更早那把明文不再认得
  （无别名 → 401 `invalid_api_key`）。
- **密钥停了新旧都停**：revoked 检查在换新门之前，密钥停用后新旧明文、任何
  版本都不能再占新的；停用的密钥也不能再换新（409）。
- **没换过的只认签发时那一把**：没有换新状态时出示别的哈希一律无效（401）。
- **各自真用掉可查、余量仍是一份**：`rotation.credentials[]` 给出每版明文的
  `state`（current/grace/retired/expired）、`usable`、`grace_remaining_ms`、
  `consumed`（确认时按占用单的 `cred` 归因到该版明文，冲正同步减回净额；
  顶上备钥确认的归因与流水一起镜像回主钥）。顶层 `burst/window` 的
  可再占/还占着/真用掉仍是这把密钥的一份总数，不因换新而拆分。
- **流水带明文版本**：reserve/confirm/release/reversal 每笔流水都有
  `cred` 字段（该笔实际出示的明文哈希），翻流水时以 `credential_id` 返回，
  可核对某版明文发生过哪些调用。换新、收掉都不删任何历史流水。

> 换新只支持“签发时那把 → 新一版 → 再下一版”的线性轮换，同一时刻只有
> “当前 + 上一版（宽限内）”两把明文能占新的；更早版本的别名保留可审计，
> 但不再认得。换新/收掉的管理脚本与共享池一样会访问同一逻辑密钥 tag 下的
> 别名 key（`{qk:<cred32>}rcr`），适用于单机/主从 Redis。

## 跨密钥共享额度池

当多把还有效的密钥要受同一个总额度约束时，开一个共享池。池不是替代密钥自己的
限额，而是额外的一道聚合闸门：成员密钥仍各用各的令牌桶/窗口，只有
**密钥自己占得住、池也同时占得住** 才能放行。

```bash
# 1) 开池：突发上限、补充速率、窗口长度、窗口总量都写明
curl -s -X POST localhost:8000/v1/pools \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"name":"partner-pool","burst_capacity":10,"burst_refill_ms":1000,
       "window_seconds":60,"window_quota":100}'
# -> {"pool_id":"...", "state":"active", ...}

# 2) 把指定密钥放入池（一次只能在一个池里；已在别的池返回 409）
curl -s -X PUT localhost:8000/v1/pools/<pid>/keys/<kid> -H "Authorization: Bearer $ADMIN"

# 3) 查池：还能再占多少、在池密钥、多少还占着、多少已真用掉
curl -s localhost:8000/v1/pools/<pid> -H "Authorization: Bearer $ADMIN"

# 4) 拿出密钥：新调用立即不受该池限制；拿出前已占着的旧单仍按原池走完
curl -s -X DELETE localhost:8000/v1/pools/<pid>/keys/<kid> -H "Authorization: Bearer $ADMIN"

# 5) 停池：成员密钥立刻不能占新的池额度；在飞单回来也不算池真用掉
curl -s -X POST localhost:8000/v1/pools/<pid>/stop -H "Authorization: Bearer $ADMIN"
```

硬规则：

- **放入/拿出都点名密钥**：`PUT/DELETE /v1/pools/{pid}/keys/{kid}`。归属以
  `{qk:<kid>}pool` 为唯一事实；同一把密钥同时只能在一个池里，重复放入别的池
  返回 409 `already_in_pool`。没进池的密钥完全不受池影响。
- **双重占用**：数据面先占密钥侧单，再占池侧单。池满时即使密钥自己还有额度，
  也返回 `burst_limited` / `window_limited` 和 Retry-After；刚占的密钥侧额度会
  立即释放。两张单子用同一个业务号关联，业务号重放不会二次占额。
- **出池不影响旧在飞单**：删除归属只阻止新请求选这个池；旧占用单上记录了
  原 `pool_pid/pool_pres`，confirm/release 仍操作原池，直到这笔正常调成或退回。
- **停池语义**：`stopped=1` 后新池占用立即拒绝（`403 pool_stopped`）；已经占着、
  上游之后才回来的，settle 原子释放两边占用并返回 `403 pool_stopped`，不计入
  池真用掉。密钥自己若仍有效，该笔的失败仍按一次未调成调用处理，不产生确认。
- **超时兜底**：池侧 `{qp:<pid>}holds/wholds` 与密钥侧 holds 都由 sweep 扫描；
  配对的两张单任一先超时，会摘掉另一边已到期的登记，避免池额度挂死。
- **冲正**：对成员密钥上已确认的一笔冲正时，reverse.lua 同时把密钥原计数器和
  池聚合计数器各退回一笔；响应中的 `pool_refunded` 标明池两个维度是否有账可退。

> 当前实现的跨密钥 Lua 会访问 `{qk:<kid>}` 与 `{qp:<pid>}` 两类 hash tag，适用
> 于 docker-compose 使用的单机 Redis；Redis Cluster 要求单脚本同 slot，需要改成
> 外部两阶段协调或将池归属/计数重新设计为同 slot 键。

## 跨密钥额度划转（只划此刻还能再占的那一截）

两把**都还有效**的逻辑密钥之间，可以把源密钥此刻没有真用掉、也没有占着的
突发/窗口额度划给目标密钥。划转是管理面动作，由一段跨密钥 Lua 原子完成，
不会出现源扣了、目标没到的半笔。

```bash
curl -s -X POST localhost:8000/v1/keys/<source_kid>/transfers \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"transfer_id":"tr-20260911-001","target_key_id":"<target_kid>",
       "burst_amount":2,"window_amount":10}'
# 目标需要先有对应空位（例如已经真用掉一部分且未占满）。成功 ->
# 201 {"transfer_id":"tr-...","source_key_id":"...","target_key_id":"...",
#         "burst_amount":2,"window_amount":10,
#         "source_remaining":{"burst":...,"window":...},
#         "target_remaining":{"burst":...,"window":...}}
```

查询：

```bash
# 一把密钥划出/划入累计 + 明细（direction=out|in，缺省两边都列）
curl -s "localhost:8000/v1/keys/<kid>/transfers?direction=out" -H "Authorization: Bearer $ADMIN"
# 按划转号查单笔：从哪把到哪把、突发/窗口各多少、何时完成
curl -s localhost:8000/v1/transfers/tr-20260911-001 -H "Authorization: Bearer $ADMIN"
# 全部划转单
curl -s localhost:8000/v1/transfers -H "Authorization: Bearer $ADMIN"
```

硬规则：

- **划转号幂等**：`transfer_id` 必填。同一号、同一源/目标/数量再来，返回 200
  与第一次的划转单（`already_transferred=true`），不二次划转；同号但参数不同
  返回 409 `transfer_id_conflict`，不能借同一个号改账。
- **只能在有效密钥间划**：源或目标不存在返回 404；任一方已停用返回 409。
- **不划已真用掉、不划还占着的**：可划突发按令牌桶当前令牌扣掉未到期占用计算；
  可划窗口按滑动窗口已用扣掉未到期占用计算。源不足返回 422
  `insufficient_burst/window`，响应同时给出 `source_available`、`target_room`
  与 `held`。
- **不能超过目标自己还装得下的空位**：目标的空位 = 原配额下现在已经真用掉
  与在飞占住导致“装不下新请求”的容量；满额未用、或已有冲正/划入溢余不会产生
  空位。目标空位不足时整笔拒绝。
- **两边同时做成**：突发维度在源/目标令牌桶原子增减，窗口维度在两边当前固定
  桶原子记正/负用量，随后 `GET /quota` 与数据面下一次 reserve 立刻按新余量。
- **在飞单不受影响**：划转不读、不改、不释放任何 `res:*` 或 holds 成员。已经
  占着的单子仍按原占用单记录的池、容量与计数路径确认/退回；新占用才按划转后
  的余量判定。
- **窗口划转随窗口自然老化**：这笔“源已用、目标可用”的调整落在划转时刻的
  滑动窗口固定桶，到窗口滑出后自然结束；它不是永久修改两边的 `window_quota`
  配置。突发划转则是此刻令牌的立即转移，之后仍按各密钥自己的补充速率补充。
- **份额池按各自空位参与，不被跨钥混用**：公共池与每个点名份额池独立计算
  当前余量和目标空位；脚本按“公共池优先、份额按调用方名排序”从各池扣出/填回，
  一次划转可由多个池凑齐。目标旧在飞单不释放，填入其空位的新余量在旧单确认后
  成为可再占额度。
- **与共享池相互独立**：划转改变的是成员密钥自己的余量；跨密钥共享池仍是额外
  一道聚合闸门，不会因为密钥侧划转而平白增加池额度。
