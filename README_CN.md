# auto_squid

一个轻量级正向代理:把**每个请求并行竞速发到一整池上游代理**,谁先出首字节谁拿下全部。它在此之上提供多目标 Cost 排序、域名缓存、会话粘性、HTTP 响应缓存、CONNECT 连接预热,以及 SQLite 持久化的终身指标——全部由两个端口和一个 YAML 文件驱动。

> [English →](README.md)

---

## 这是什么

客户端把系统级或应用级代理指向 auto_squid。每个请求(普通 HTTP 或 `CONNECT`)都会**并行**派发给一组上游代理:候选按加权 Cost 排序,最优的几个先发射,其余按定时器补位(RFC 8305 §5 交错启动)。第一个产出首字节的响应胜出,其余一律取消并关闭。

相比钉死单个代理,你得到更低的 TTFB、代理退化时的自动切换,而且上游侧不需要任何配置改动。

竞速一旦分出胜负,结果会被记住:按域名缓存、可选的会话粘性(稳定出口 IP)、到上游的 TCP 预热,以及按代理/按域名的统计——这些又反过来喂给下一次排序决策。

## 快速开始

```bash
# 1. 安装(运行时依赖:fastapi、uvicorn[standard]、httpx、pydantic、typer、pyyaml)
uv venv .venv --seed && uv sync

# 2. 声明上游代理(proxies.yaml 已被 gitignore)
cp examples/proxies.yaml proxies.yaml     # 填 host/port/auth

# 3. 可选:调整行为(config.yaml 同样被 gitignore)
cp examples/config.yaml config.yaml

# 4. 启动
python -m auto_squid.cli
# 或:auto-squid                          # pyproject.toml 里注册的入口
# 参数:--config ./config.yaml --proxies ./proxies.yaml --db ./auto_squid.db

# 5. 验证
curl http://127.0.0.1:18080/health        # → {"status":"ok"}
curl http://127.0.0.1:18080/proxies
curl -x http://127.0.0.1:10808 http://www.baidu.com
curl -x http://127.0.0.1:10808 https://www.baidu.com
```

控制台:<http://127.0.0.1:18080/>(会 307 跳到 `/panel/domains`)。

**默认值偏保守。** 竞速、Cost 排序、域名缓存、HTTP 响应缓存开箱即用;会话粘性、连接池、主动探测、簇预测、自适应 TTL 和自动调参器都是**关闭**状态,需要你显式打开。管理 API 默认只绑回环且不带鉴权——见[安全模型](#安全模型)。

## 端口

| 端口 | 用途 | 默认绑定 |
|------|---------|--------|
| `10808` | 代理端口 — HTTP/`CONNECT` 入口(`-x http://127.0.0.1:10808 …`),可选客户端 Basic 鉴权 | `0.0.0.0` |
| `18080` | 管理 API + 控制台 + `/docs` | `127.0.0.1` |

## 能力

**竞速与选择**
- **交错并行竞速** — 首批发射 `stagger_initial` 个候选,其余每 `stagger_interval_ms`(下限 100 ms)补位;首个首字节胜出,败者取消。
- **多目标 Cost** — `w_lat·norm(latency) + w_sr·norm(1−success_rate) + w_tp·norm(1−throughput)`,在**候选集内部**做 min–max 归一化,因此权重可直接横向比较。延迟项默认取终身 TTFB **P99**(t-digest),缺失时回退 EWMA;成功率做 Laplace 平滑,优先使用域名级桶。`cost_sort_enabled: false` 可立刻回退到纯 EWMA 排序。
  吞吐项**只由明文响应体供数**:隧道的"字节数 ÷ 隧道墙钟寿命"衡量的是*客户端*闲置了多久,而不是代理转发有多快,因此它永远不参与 Cost(仍以 `tunnel_relay_ewma_mbps` 可观测)。
- **最少请求偏置**(`lb_bias`)与连续失败惩罚(`fail_penalty_weight`),把负载从积压或不稳定的代理身上引开。
- **熔断 + 慢启动** — `circuit_threshold` 次连续传输失败打开熔断,指数退避上限 `circuit_max_backoff`;恢复经慢启动限速。业务性拒绝(上游对 `CONNECT` 返回非 200、HTTP 5xx)单独分类,**不**触发熔断。
- **主动探测** — `probe_canary`(或 `probe_canaries` 列表)无需等待客户端流量即可复测已熔断的代理。`probe_with_get` 可在 CONNECT 探测后追加一次白名单 GET,让真实 TTFB/字节指标继续更新。
- **预警降级**(`single_send_degrade_*`)— 在熔断尚未触发时,就先把出现退化苗头的粘性/缓存代理解开。
- **本地竞速 / fail-open** — 网关主机可作为自己的一个节点参与竞速(`enable_local_racing`);`local_direct_domains` 直连源站、绕过上游。

**缓存与粘性**
- **域名缓存** — 某域名的竞速胜者在其 TTL 内被复用;`adaptive_ttl` 依据域名稳定性放大或重置该 TTL。
- **会话粘性** — 按 `client_ip + domain` 钉住代理以获得稳定出口 IP;失败或退化的钉选会被剔除并重新竞速;`recheck_hits` 周期性复核钉选。表格有硬上限(`max_entries`,LRU)。
- **HTTP 响应缓存** — 幂等 `GET` 的响应体缓存在内存(`ttl`,遵循 `Cache-Control`);`POST/PUT/DELETE/PATCH` 会失效该域名已缓存的 GET。单个响应体超过 `max_bytes/2` 会被刻意跳过缓存。
- **在途 GET 合并** — 同一 URL 的并发请求共享一次上游取回。

**连接池(CONNECT)**
- **第一阶段** — 到每个上游代理的空闲 TCP 连接池。
- **第二阶段**(`target_prewarm`)— 同类裸 TCP,按 `(proxy, target)` 分桶,在域名缓存/粘性命中或竞速胜出预示"还会再来"时提前建好。
- **跨阶段借用** — 第一阶段桶空时,从**同一代理**的第二阶段桶里借一条(TCP 本身与 target 无关,`target` 只是桶标签)。护栏:只从持有 ≥2 条的桶里取,按最旧优先,保证每个 target 都给自己留一条。
- **簇预测**(`cluster_predict`)— 学习短窗口(一次页面加载)内哪些 target 共同出现,在下个请求到来前预热预测出的伴随 target。
- **陈旧检测** — 对端已经半关闭(CLOSE_WAIT)的连接,在 `is_closing()` 看来完全健康。取到它要白烧一整个 CONNECT 往返并换回"无响应",因此读取侧会检查 EOF/残留字节并当场丢弃。

**运维**
- **管理 API + 控制台** — 代理列表、域名统计、粘性表、Cost 分解、熔断/探测状态、自动调参器控制。
- **Cost 参数热更新**(`POST /cost`)— 下一次竞速即刻生效,无需重启。
- **自动调参器**(`auto_tune`,默认关)— 保守的山丘爬坡(hill climbing):只有胜出 TTFB 改善 ≥ `hysteresis` 且成功率无回退才采纳改动,变差则回滚。
- **SQLite 终身指标** — 按代理、按域名的窗口分位数(最近 256 样本)*加上* t-digest 终身汇总、错误分类、HTTP 版本分布、平滑成功率与低置信标记。
- **加载期配置校验** — `extra="forbid"`、跨字段检查、可读消息、退出码 2。
- **按大小轮转日志**、优雅停机、Docker 镜像 + compose 示例。

## 请求链路

决策链(`auto_squid/router.py`):

1. **接入与解析** — `handle_client` 读取请求头,先执行客户端 Basic 鉴权(在任何上游动作之前返回 `407`),请求头超限则只打一行 INFO 而不是抛 traceback。`CONNECT` → `_handle_connect`;明文 HTTP → `_handle_http_request`。
2. **短路** — `local_direct_domains`(直连源站)、在途 GET 合并、HTTP 响应缓存命中。
3. **派发**(`_dispatch_single`)— 按优先级:
   1. **会话粘性** → 在钉选的代理上单发;
   2. **域名缓存** → 在 TTL 内的上次胜者上单发;
   3. **竞速**(`_race_staggered`)→ Cost 排序候选,交错发射。
4. **连接获取** — 已建立的隧道绝不复用;每次请求按 **第二阶段 → 第一阶段 → 跨阶段借用 → 新建连接** 的顺序取一条 TCP,然后发送 `CONNECT`。取回时若发现连接已陈旧,立即丢弃并重开新连接——且**不**计入熔断。
5. **响应成帧** — 逐跳头部被剥离(`proxy-authorization` 绝不会漏到上游),请求体为 chunked 时先解码再重算 `Content-Length`,`Transfer-Encoding` 优先于 `Content-Length`(RFC 9112 §6.1)。HTTP/1.0 客户端拿到 close 定界而非 chunked 成帧(RFC 9112 §2.7);`HEAD`、`204`、`304`、1xx 一律不带响应体。上游响应被截断时会被检测并上报,而不是静默转发。
6. **反馈** — 胜者更新 EWMA/TTFB/OFB、协议版本、吞吐(明文响应体)和域名缓存;失败调用 `record_failure`,喂给 `selector.py` 里的熔断器。退化的单发触发 `_degrade_send_proxy`,让该域名的*下一个*请求跳过粘性/缓存直接重新竞速。

### 各旋钮作用在哪

| 旋钮 | 作用阶段 | 效果 |
|---|---|---|
| `stickiness.*` | 派发 3.1 | 是否钉选、TTL、复核节奏、容量 |
| `cache_ttl`、`adaptive_ttl.*` | 派发 3.2 | 竞速胜者被复用多久 |
| `max_retries`、`stagger_initial`、`stagger_interval_ms` | 竞速 3.3 | 扇出宽度与补位节奏 |
| `cost_*` | 候选排序 | 多目标排序 vs 纯 EWMA |
| `lb_bias`、`fail_penalty_weight` | 候选排序 | 积压与不稳定惩罚 |
| `conn_pool.*` | 连接获取 4 | 池深度、空闲 TTL、预热、预测 |
| `circuit_threshold`、`circuit_max_backoff`、`slow_start_*` | 失败统计 5–6 | 打开 / 退避 / 恢复 |
| `probe_*` | 后台 | 复测已熔断代理、保持指标热度 |
| `single_send_degrade_*` | 降级闸门 6 | 在熔断跳闸前解开钉选 |
| `policies` | 候选集 | 收窄哪些代理可以服务某域名 |
| `concurrency_limit.*` | 准入 | 按代理自适应在途上限 |

## 配置

全部在 `config.yaml`。每个键都是可选的,下面的值就是**代码默认值**。`examples/config.yaml` 给出一份生产形态的替代配置。

```yaml
listen:
  host: "0.0.0.0"                  # 代理端口绑定
  port: 10808

api:
  host: "127.0.0.1"                # 回环;鉴权关闭时绑 0.0.0.0 会打 WARNING
  port: 18080
  auth: {enabled: false, username: "", password: ""}   # 除 /health 外所有路由的 HTTP Basic

logging:
  level: "INFO"                    # 只作用于【文件】级别(控制台固定 WARNING+)
  file: null                       # null → "auto_squid.log",相对启动时的 CWD 解析
  max_bytes: 33554432              # 单文件 32 MiB,到量轮转
  backup_count: 5                  # 保留 .1 … .5(0 = 原地截断)

router:
  enable_local_racing: false
  local_direct_domains: []         # 裸 host/IP(不带端口)— 直连源站
  cache_ttl: 600
  max_retries: 3                   # 首批竞速数量;同时钳制 stagger_initial
  stagger_start: true
  stagger_initial: 2               # 1..max_retries(越界会被钳制)
  stagger_interval_ms: 100         # RFC 8305 补位间隔,钳制到 [100, 2000]
  auth: {enabled: false, username: "", password: ""}   # 面向客户端的 Basic 鉴权(:10808)

  policies: []                     # match{domain_suffix|domain_exact|domain_regex} → proxies{tags|ids}
  stickiness:
    enabled: false
    ttl: 1800                      # 命中时滑动续期
    recheck_hits: 100              # 0 = 钉住的代理永不复核
    max_entries: 100000
    probe_interval_sec: 0.0        # 0 = 关闭后台复测
    probe_fanout: 2
  http_cache:
    enabled: true
    ttl: 60                        # 秒,命中时滑动续期
    max_entries: 10000             # LRU
    max_bytes: 268435456           # 256 MiB 预算;响应体 > max_bytes/2 不缓存
    stream_cache_limit: 1048576    # 仍具备缓存资格的单响应缓冲上限
  adaptive_ttl: {enabled: false, min_sec: 60.0, max_sec: 1800.0}
  switch_damping: {enabled: false, min_wins: 2, ratio: 0.8, abs_ms: 30.0}
  concurrency_limit: {enabled: false, min: 2, initial: 16, max: 128,
                      add_on_success: 4, mult_on_failure: 0.5, failure_window: 20}
  conn_pool:
    enabled: false                 # 两个池默认都是【关】
    per_proxy: 4
    total: 64                      # 第一 + 第二阶段共享的 fd 预算
    idle_timeout: 30.0             # 必须【低于】上游自身的空闲关闭
    refill_interval: 5.0           # 0 = 只取不补
    refill_target: 2               # 每代理的第一阶段水位(钳制到 per_proxy)
    connect_timeout: 10.0
    target_prewarm: false          # 第二阶段;需要 enabled: true
    refill_pause_minutes: 60.0     # 0 = 空闲时不挂起
    refill_pause_silence_sec: 120.0      # 【已弃用】仅为向后兼容保留;请用下面两个旋钮
    refill_pause_activity_window: null   # null → max(30, refill_pause_silence_sec/4)
    refill_pause_min_requests: 3
    cluster_predict: false         # 需要 enabled: true 且 target_prewarm: true
    cluster_window_sec: 2.0
    cluster_predict_topk: 3
    cluster_predict_throttle_sec: 30.0
    cluster_min_support: 2
    cluster_graph_ttl_sec: 86400
    cluster_graph_max_entries: 100000
    cluster_proxy_fanout: 2        # 在最可能的 N 个胜者代理上都预热
    cluster_probe_decay_sec: 3600.0
    cluster_pool_idle_timeout: 600.0    # 预测出的预热比被动预热活得更久
  circuit:
    probe_interval_sec: 30.0       # 0 = 不做主动探测
    probe_canary: "1.1.1.1:443"    # probe_canaries 非空时被其取代
    probe_canaries: []             # [{name, target, tags}] — 按候选组分别探测
    circuit_threshold: 3
    circuit_max_backoff: 300.0
    slow_start_window: 60.0
    slow_start_success: 3
    lb_bias: 1.0                   # weight = ewma × (1 + in_flight)^bias;0 = 纯 EWMA
    fail_penalty_weight: 4.0       # 0 = 关闭
    single_send_degrade_fail: 0    # 0 = 关闭;合理取值 = circuit_threshold - 1
    single_send_degrade_ratio: 0.0        # 同时驱动粘性慢速剔除
    single_send_degrade_slack_ms: 10.0    # 给比例规则兜底的绝对下限
    single_send_degrade_success_rate: 0.0 # 需要 ≥ 8 个域名样本
    single_send_degrade_p99_ms: 0.0       # 需要 ≥ 4 个域名样本
    single_send_degrade_min_throughput: 0.0  # 只算明文响应体;需要 ≥ 4 个样本
    single_send_slow_log_ms: 0.0   # 唯一能把卡顿对应到客户端 IP 的日志
    connect_tunnel_timeout_sec: 3.0
    http_read_timeout_sec: 3.0
    local_direct_timeout_sec: 10.0 # 只作用于 local_direct_domains 路径
    probe_with_get: false          # probe_get_targets 为空时等于无效开关
    probe_get_targets: []
    probe_get_interval_sec: 60.0
    probe_get_timeout_sec: 5.0
    probe_get_max_bytes: 65536
    cost_sort_enabled: true
    cost_latency_metric: "p99"     # "p99" | "ewma"
    cost_weight_latency: 1.0
    cost_weight_success_rate: 0.6
    cost_weight_throughput: 0.1
    cost_latency_min_samples: 1
    cost_throughput_min_bytes: 1000000   # 按【实测响应体字节】计,不按隧道寿命流量
  auto_tune:
    enabled: false
    window_sec: 900.0
    min_samples: 50
    step: 0.25                     # 每次探测 ±25%
    hysteresis: 0.05               # 只有 ≥5% 的胜出 TTFB 改善才采纳
    sr_guard: 0.005                # 成功率下降超过此值则否决
    persist: true                  # 状态存 SQLite(tuner_state)
```

### 加载期校验

`config_schema.py` 的模型都是 `extra="forbid"`(除了刻意宽松的 `ProxyInfo` 和 `ProbeCanaryConfig` 这两个数据模型)。拼写错误和跨字段冲突会在启动时以**退出码 2** 中止,并给出可读消息而不是 traceback:

```text
config error: invalid configuration:
  1 validation error for Config
  router.stagger_inital
    Extra inputs are not permitted
```

强制约束:`logging.level` ∈ {DEBUG, INFO, WARNING, ERROR, CRITICAL};`max_bytes > 0`;`backup_count ≥ 0`;启用 `auth` 时 username 与 password 都必须非空(否则 `Basic Og==` 会相等);`stagger_initial ≥ 1` 且 `≤ max_retries`;启用时 `adaptive_ttl.min_sec ≤ max_sec`;启用时 `concurrency_limit.min ≤ initial ≤ max`;`target_prewarm` 与 `cluster_predict` 需要 `conn_pool.enabled`,且 `cluster_predict` 额外需要 `target_prewarm`。

有两个值**不做**校验,而是静默兜底而不是响亮报错——当某项改动看起来"没起作用"时值得知道:

- YAML 里的 `circuit.cost_latency_metric`:除 `"p99"`/`"ewma"` 之外的任何值都会被强制成 `"p99"`(`POST /cost` 接口反倒**会**以 400 拒绝)。
- `stagger_interval_ms`:越界值被钳进 `[100, 2000]`,而不是拒绝。

## 上游代理列表

代理单独放在 `proxies.yaml`(一个裸列表,生产中已 gitignore):

```yaml
- id: squid-01                 # 必填、唯一 —— 所有指标都以它为键
  name: beijing-01             # 可选展示名
  host: 10.14.25.86            # 必填:裸主机名 / IPv4 / [IPv6] —— 不带 scheme、端口、@ 或 /
  port: 3128
  protocol: http               # 目前只实现了 "http",见下方说明
  auth:                        # 可选上游凭据
    username: "..."
    password: "..."
  enabled: true
  tags: {region: cn}           # 可选,由 router.policies 消费
```

> **`protocol` 目前是个装饰字段。** 代码里没有任何地方按它分支:所有上游都被当作 HTTP/`CONNECT` 代理使用,`https` 或 `socks5` 会被静默当作 `http`。因此写入路径(`POST /proxies`)直接拒绝非 `http` 的值,不让加进一个行为不符的节点——但手工编辑的 `proxies.yaml` 不会被校验,仍然会被当 HTTP 用。

运行时可用 `POST /proxies` 增删节点,它会重写整个绑定的 `proxies.yaml`(注释会丢失)。**没有删除或 patch 接口**——更新一个节点意味着用同一个 `id` POST 完整对象。

## 管理 API

全部在 `:18080`。启用 `api.auth.enabled` 后,除 `GET /health` 外所有接口都要求 HTTP Basic——`Authorization` 与 `Proxy-Authorization` 都接受,两个字段都用 `hmac.compare_digest` 比对。失败返回 `401` + `WWW-Authenticate: Basic realm="auto_squid"`。

### 读

| 接口 | 说明 |
|---|---|
| `GET /` | 307 → `/panel/domains` |
| `GET /panel/{domains,stickiness,metrics}` | 控制台本体;未知 view → 404 |
| `GET /static/panel.js` | 控制台脚本,从内存直出 |
| `GET /health` | 存活检查;免鉴权 |
| `GET /proxies` | 节点列表;`auth` 的值默认掩码为 `***`,除非 `?include_secrets=true`(键名始终可见) |
| `GET /stats` | 每代理的 `request_counts`(胜出)与 `attempted_counts`(尝试) |
| `GET /domains` | 按域名胜出数 —— 取 **胜出数** Top-N(`?limit=`,默认 2000,`0` = 全部) |
| `GET /domains/meta` | 按域名的默认代理 + `updated_at`(启用 `adaptive_ttl` 时附带 TTL 字段)—— 取 **最近更新** Top-N(`?limit=`) |
| `GET /stickiness` | 钉选表 `client_ip|domain → proxy` —— 取 **最近使用** Top-N(`?limit=`) |
| `GET /policies` | 策略路由快照 |
| `GET /config` | 目前只有 `{"enable_local_racing": …}` |
| `GET /metrics` | `request_counts`、`attempted_counts`、**全量** `domain_stats`、约 80 项 `counters`、`proxy_metrics` |
| `GET /metrics/per-destination?domain=` | 按 `(domain, proxy)` 的分位数、错误分类与终身 `cumulative` 对象;省略 `domain` 返回整张网格(剥掉原始样本),带上则是一次便宜的钻取 |
| `GET /quality` | 每代理 EWMA 首字节延迟(秒)—— 历史遗留的排序依据 |
| `GET /quality/meta` | 每代理:窗口 TTFB/OFB 分位数、`throughput_ewma_mbps`(仅响应体)、`tunnel_relay_ewma_mbps`(仅供观测)、`body_bytes_transferred`、窗口 + 终身成功率、错误分解、HTTP 版本计数、`cumulative` 终身对象,以及 `cost_breakdown` |
| `GET /circuit` | `circuit_open_count`、探测计数、`single_send_degrades`、`degraded_single_send`,以及每代理 `{open, open_until, backoff, consec_fail, slow_start}` |
| `GET /server-stats` | CPU% 与事件循环延迟 —— 仅在 `bench/` 下运行时填充;普通 CLI 启动返回 `{}` |
| `GET /cost` | 当前 Cost 参数、自动调参器状态、安全边界 |
| `GET /docs` | OpenAPI UI(和其他接口一样受鉴权) |

`/domains`、`/domains/meta`、`/stickiness` 默认 2000 行,是因为这些表可能装到 ~10 万条,而 payload 是在**转发你流量那个同一个事件循环**上构造的。`GET /metrics` 不做截断。

### 写

| 接口 | 效果 | 是否持久化? |
|---|---|---|
| `POST /proxies` | 按 `id` 新增/覆盖节点;立刻可参与竞速 | 重写 `proxies.yaml`;响应报告 `persisted`(附 `persist_error`)—— `persisted: false` 时该节点只服务到重启,之后消失 |
| `POST /cost` | 热更新 7 个 Cost 字段的任意子集 —— 下次竞速生效 | 否(重启恢复 `router.circuit.cost_*`),但同时也重置了调参基线 |
| `POST /tuner` | `{"enabled": true\|false}`;打开会重启评估循环,**关闭会把权重回滚**到最后一次采纳的基线 | 否 |
| `POST /quality/reset` | 清空 EWMA、域名指标、熔断、在途与降级标记 → 全部按"未知质量"重新竞速 | **同时清掉 SQLite 里持久化的终身行** —— 破坏性 |
| `POST /circuit/reset` | 关闭所有熔断、清退避与连续失败计数、重装慢启动;延迟历史不动 | 否 |

未知 JSON 字段在 `/cost` 与 `/tuner` 上以 `422` 拒绝(`extra="forbid"`),但会被 `POST /proxies` **静默忽略**。`POST /proxies` 还额外拒绝含 scheme/端口/`@`/`/` 的 `host`,要求 `protocol == "http"`,并拒绝值全为 `***` 的 `auth` 块——你无法把掩码后的 GET 结果原样回灌。

### 安全模型

- 代理端口默认绑 `0.0.0.0` 且客户端鉴权**关闭**——凡是能连到它的,都是开放代理。请设置 `router.auth.enabled`,或在防火墙上收紧绑定/ACL。
- API 默认只绑回环。非回环绑定且 `api.auth` 关闭时会打一条启动 WARNING,但不会拒绝启动。
- **跨站写入被拦截。** `POST/PUT/PATCH/DELETE` 在**鉴权之前**就被检查,且无论 `api.auth` 是否开启都生效:`Sec-Fetch-Site: cross-site` → `403 {"detail":"cross-site admin write blocked"}`;否则 `Origin` 主机与本主机不符即拒绝(忽略 scheme,以便 HTTPS 终结的反代正常工作)。缺失 `Origin`(curl、脚本)与 `Origin: null` 放行。之所以重要:浏览器会自动重发缓存的 Basic 凭据,"攻击者页面读不到响应"并不能阻止写入落地。
- `proxy-authorization` 在转发前被剥离,客户端代理凭据不会到达上游。

## 值得盯的指标

`GET /metrics` → `counters`。真正能告诉你这台机器状态的是这几组:

| 计数器 | 含义 | 健康的样子 |
|---|---|---|
| `http_cache_hits` / `_misses`、`domain_cache_hits`、`sticky_cache_hits` | 短路命中率 | 命中率高意味着竞速次数少 |
| `racing_invocations`、`upstream_attempts` | 竞速放大 = 尝试数 ÷ 请求数 | CONNECT 为主的流量下 ≤ ~1.5 |
| `conn_pool_hits` / `_misses` / `_steals` / `_expired`、`target_pool_*` | 池有效性 —— `steals` 是跨阶段借用,`misses` 现在的语义是"两个池都空 → 只能新建 TCP" | `expired` 相对 `creates` 很少 |
| `pool_stale_discards` | 取回时抓到的半关闭连接 | `idle_timeout` 调低后,应与 `stale_conn_retries` 一起下降 |
| `stale_conn_retries` | `pooled conn STALE` 重试,每次代价一个 CONNECT 往返 | **≈ 0**;持续上升说明池的空闲 TTL 比上游的更长 |
| `single_send_degrades` | 预警解钉次数 | 低位、平稳 |
| `circuit_open_count`、`probes_sent/ok/skipped/failed` | 熔断 + 探测活动 | `probes_skipped` 不断增长 = 某个代理走 canary 不通 |
| `cluster_pool_bucket_miss` / `_timing_miss` | 预测为何落空(预测胜者不是实际胜者 / 到得太晚) | 只在 `cluster_predict: true` 时有意义 |
| `http_cache_evictions`、`sticky_cache_size`、`cluster_graph_size` | 表相对各自上限的压力 | 稳定,而不是顶格 |

窗口值覆盖最近 256 样本并在重启时清零;`cumulative` 与 t-digest 分位数是终身且 SQLite 持久化的。别把两者放一起比。

## 调优

三份起步画像(带注释的完整版见 `examples/config.yaml`):

```yaml
# 稳定优先 —— 出口 IP 稳定,登录/风控敏感场景
router:
  stickiness: {enabled: true, ttl: 1800, recheck_hits: 100}
  circuit: {probe_interval_sec: 30, single_send_degrade_fail: 2}
```

```yaml
# 速度优先 —— 最低 TTFB,可接受出口切换
router:
  cache_ttl: 900
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 100        # RFC 8305 下限
```

```yaml
# 扇出优先 —— CONNECT 密集,压到最低的竞速放大
router:
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 200
  circuit:
    probe_interval_sec: 30
    single_send_degrade_fail: 1   # 在熔断跳闸前解开
```

只有在生产环境才会暴露出来的几条:

- `conn_pool.idle_timeout` 必须**低于上游代理自身的空闲关闭**,否则池里的连接会半关闭躺在那儿,每次复用都白付一个 CONNECT 往返。改完之后盯 `pool_stale_discards` 与 `stale_conn_retries`。
- `probe_canary` 必须既能从网关本机到达、也能通过每个上游到达,否则整轮探测都被计入 `probes_skipped`。
- `single_send_slow_log_ms`(默认关)是唯一能把"打不开/要刷新"的卡顿对应到具体客户端 IP 的日志行 —— 成功路径不记录 IP。
- `http_read_timeout_sec` 为 3 s;想再收紧之前,先在真实负载下看 p99 与 fd 用量。
- `conn_pool.enabled` 与 `target_prewarm` 默认为 **关**。重复流量不够却打开 `target_prewarm`,表现就是 `target_pool_expired`(建好又闲置到过期被回收)—— 跨阶段借用能挽回其中大部分库存,但该看的是这个比值。
- `conn_pool.refill_pause_minutes: 0` 会关闭空闲挂起。若心跳流量是突发的(每 2–5 分钟来一簇),任何窗口/阈值组合都不会触发挂起,而这份空闲开销便宜到可以不管。
- `cluster_predict` 在这里关闭是**基于实测**而非出于谨慎:约 4 天生产数据里它只命中 29.5%,且 63.8% 的落空是 `bucket_miss`(预测的胜者不是实际胜者)。`cluster_proxy_fanout: 2` 也没能纠正这一点。
- Fail-open:若所有外部代理都已熔断而本机能够到达源站,路由会静默直连 `local`。无需配置。

## 日志

文件日志使用单进程的 `RotatingFileHandler`:每文件 `max_bytes`,保留 `backup_count` 个历史文件,启动横幅打印解析后的绝对路径与生效策略:

```text
INFO:auto_squid.cli:file log: /srv/auto_squid/auto_squid.log (level=INFO, rotate=33554432 bytes, keep=5 backups)
```

注意事项:

- **不要再对这个文件配 logrotate。** logrotate 会把文件从进程脚下改名走(进程继续写旧 inode,而"新"文件一直空着),或者用 `copytruncate` 丢掉截断竞态期间写入的行。应用内轮转与 logrotate 互斥,二选一。
- 手工切出来的历史文件(`auto_squid.log.261011`)不在 `.1 … .N` 轮转链里:既不复用也不删除。请手动清理一次。
- `logging.level` 设置的是**文件**级别;控制台 handler 固定 WARNING+,以免转发流量把它刷屏。在 systemd 或 Docker 下,stdout 那侧的量因此很小,归 journald / Docker 日志驱动管。
- `RotatingFileHandler` 不是多进程安全的:两个实例共用一个 `logging.file` 会互相踩坏轮转链。各给一个路径。

## 容器部署

```bash
docker compose -f examples/docker/docker-compose.yml build
docker compose -f examples/docker/docker-compose.yml up -d
curl http://127.0.0.1:18080/health
curl -x http://127.0.0.1:10808 http://www.baidu.com
```

多阶段镜像、非 root 用户、健康检查,以及一个存放 SQLite 的 `./data` 卷。两个日志决策是关键的,`examples/docker/` 都已做好:把 `logging.file` 指到 `/data/…`(默认的相对路径会落在容器可写层,重建即丢),以及用 compose 的 `logging:` 选项给 **stdout** 流设上限——json-file 驱动默认不限大小,`max_bytes` 管不到它。挂载真实上游的说明见 [`examples/docker/README.md`](examples/docker/README.md)。

## 测试

```bash
uv run pytest -q --timeout=60      # 489 项,约 85 秒
```

覆盖范围包括 HTTP/`CONNECT` 转发与成帧(HTTP/1.0 降级、`HEAD`、截断检测)、HTTP 缓存(LRU 顺序、驱逐、合并)、域名缓存、竞速与聚合超时、客户端与 API 鉴权、CSRF 防护、熔断/探测/EWMA/在途加权、粘性、按域名统计与 SQLite 持久化、UTF-8 与二进制安全响应体、两个池加跨阶段借用、空闲挂起与预算记账、配置层校验与退出码,以及事件循环卫生测试(后台任务留下孤儿 DNS future 就会失败)。`tests/test_phase_metrics.py` 覆盖指标与排序层,`tests/test_tuner.py` 覆盖自动调参器。

CI 在 Python **3.10、3.11、3.12** 上跑同一套用例(`.github/workflows/test.yml`),每项超时 60 s;覆盖率(约 87%)只在 3.11 那条腿上测量,并且刻意不设 `fail_under`——异步计时会让覆盖率有约 0.2% 的抖动。

> **Python 3.12 说明:** `StreamWriter.wait_closed()` 和 `Server.wait_closed()` 对端上 FIN 的要求变严格了。router 给这些等待加了上界;mock 上游会在空闲 5 s 后主动关闭连接。

## 压测

`bench/` 是一套受控夹具:可配置延迟/大小/分块/失败率的 mock 上游集群消除了真实网络抖动;被测 router 跑在独立子进程里,因此客户端的工作量永远不会虚增被测服务端开销。

```bash
uv run python -m bench.stress --quick                 # 约 10 秒冒烟
uv run python -m bench.stress --mode staircase        # 找饱和点
uv run python -m bench.stress --mode soak --duration 600 --open-loop
uv run python -m bench.stress --mode conn-reuse       # keep-alive 复用率(仅 mock)
uv run python -m bench.stress --upstream real --proxies proxies.yaml
uv run python -m bench.github --mode tunnel --duration 60   # 对真实目标打 CONNECT
uv run python -m bench.stress --rounds 5              # 均值 ± 标准差,每轮新子进程
```

报告输出吞吐、TTFB/总耗时的 P50/P95/P99、错误分类、缓存命中率、竞速放大、池命中/借用/落空,以及服务端 CPU 与事件循环延迟;`bench_report.json` 会带上 git revision。详见 [`bench/README.md`](bench/README.md)。

`test_routing.py` 用来观察某个 URL 的决策链(粘性/域名缓存命中、策略收窄、候选顺序、熔断状态)而不真正发流量 —— `--test-request` 会执行一次真实请求。

## 局限

- HTTP 解析属于 MVP 级别:头部与成帧处理得相当小心,但极大的流式响应仍可能踩到边界情况。
- 上游 `protocol` 实际上只支持 HTTP(`https`/`socks5` 没有实现)。
- `CONNECT` 隧道是原始字节管道 —— 不做 TLS 中间人,因此按隧道的"吞吐"无法描述源站的传输速度。
- 内存态数据(粘性、域名缓存、HTTP 响应缓存、窗口样本)重启即清空;只有终身计数、t-digest 和域名胜出数在 SQLite 里存活。
- 不启用 `router.auth` 或不在网络层收紧,代理端口就是一个**开放代理**。
- `POST /proxies` 会整份重写 `proxies.yaml` 并丢弃注释;没有删除接口。
- `http_read_timeout_sec`(3 s)在高延迟链路上可能把"慢但健康"的源站判超时;要调就明确调,并验证 p99 与 fd。

## 延伸阅读

[`docs/README.md`](docs/README.md) 索引了设计、审计与事故复盘文档(`性能优化总纲`、`改进方案_响应速度评估`、`代码审计报告`、2026-09 内网熔断事故分析)。

## 许可

MIT
