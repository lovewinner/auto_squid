# auto_squid（中文说明）

一个轻量级正向代理，支持**并行竞速**、**多目标 Cost 排序**、**域名缓存**、**HTTP 响应缓存**、**会话粘性**以及 **SQLite 持久化统计**。运行在网关主机上，将每个 HTTP/HTTPS 请求转发到上游代理池中——最快者胜出，其余取消释放。

> [English →](README.md)

---

## 是什么

每个客户端请求（HTTP 或 `CONNECT`）都会在上游代理之间并行竞速：候选按延迟 + 成功率 + 吞吐量的加权 Cost 排序，最优的 1–2 个先发（RFC 8305 §5 错峰启动），其余以 ~250 ms 间隔补发；首个首字节成功即胜出并取消其余。结果是比单代理更低的 TTFB，内置代理退化时的自动切换，且上游无需任何手动配置。

除竞速外，auto_squid 还提供：域名级竞速胜出缓存、可选会话粘性（稳定出站 IP）、内存中的 HTTP GET 响应缓存、三层 CONNECT TCP 连接池、内置管理 API 与仪表盘，以及每代理/每域名的**窗口（最近 256 次）**与**终身（t-digest，跨重启）**双族分位数指标。

## 快速上手

```bash
# 1. 安装
uv venv .venv --seed && uv sync          # 运行时依赖: fastapi, uvicorn[standard], httpx, pydantic, typer, pyyaml

# 2. 准备上游代理（schema 见 proxies.yaml）
cp proxies.yaml examples/proxies.yaml   # 占位 —— 填你自己的

# 3. 启动
python -m auto_squid.cli
# 或: auto-squid            # pyproject.toml 注册的入口
# 选项: --proxies ./proxies.yaml --db ./auto_squid.db --config ./config.yaml

# 4. 验证
curl http://127.0.0.1:18080/health        # → {"status":"ok"}
curl http://127.0.0.1:18080/proxies
curl http://127.0.0.1:18080/stats

# 5. 作为代理使用
curl -x http://127.0.0.1:10808 http://www.baidu.com
curl -x http://127.0.0.1:10808 https://www.baidu.com
```

然后打开仪表盘：http://127.0.0.1:18080/

## 特性

- **并行竞速 + 错峰启动** —— 候选按 EWMA 排序；最优 1–2 个先发，其余以 ~250 ms 补发（RFC 8305 §5）；首个首字节成功即胜出并取消/关闭其余
- **多目标 Cost 排序**（默认开启）—— 竞速顺序 = `w_lat·norm(延迟) + w_sr·norm(1−成功率) + w_tp·norm(1−吞吐)`，候选集内 **min–max 归一化**（权重可直接比较、与规模无关）。延迟项使用**终身 TTFB P99**（t-digest）+ EWMA 回退；成功率拉普拉斯平滑（优先域名级）；吞吐需要 1 MB 字节下限。`cost_sort_enabled: false` 一键回滚纯 EWMA 排序
- **Cost 热更新 + 自动调参**（P1）—— `GET/POST /cost` 运行时读写全部 Cost 参数（**下一次竞速即生效，无需重启**）；`POST /tuner` 切换保守爬山自动调参器，仅在满足 ≥5% 赢家 TTFB 改善且成功率无回退时采纳，恶化时自动回滚
- **域名缓存** —— 某个代理为某域名竞速胜出后，在 `cache_ttl` 内复用，避免每次请求都竞速
- **会话粘性** —— 同一客户端 IP + 域名/目标复用同一代理（稳定出站 IP）；失败或返回 5xx 的粘性代理被驱逐并回落竞速（redispatch）；按 `recheck_hits` 周期探路重竞速；内存表带硬容量上限
- **HTTP 响应缓存** —— 幂等 `GET` 响应内存缓存（TTL 60 s，遵循 `Cache-Control`；POST/PUT/DELETE/PATCH 使该域名缓存的 GET 失效）
- **在途 GET 合并** —— 同一 URL 的并发请求共享一个上游请求（有界等待，超时后回落竞速）
- **本机竞速** —— 可选让网关主机自身作为代理节点直接参与竞速（不走上游）
- **三层 CONNECT TCP 连接池** —— 通用上游池、目标半预连接（`target_prewarm`）、已握手复用（`established_reuse`）；另有**请求簇预测预暖**（`cluster_predict`），预建下一页加载窗口内预测会共现目标的"到上游" TCP
- **业务对齐探活**（默认关闭）—— `probe_with_get` 在 CONNECT 探路后追加一次白名单内的轻量 GET，将 TTFB/协议/吞吐记入真实域指标桶
- **可观测性** —— 每代理/每域名**窗口**（最近 256 次）**与终身**（t-digest，跨重启）双族分位数；错误分类（超时/连接/5xx/HTTP 状态/TLS/协议/取消/其他）；HTTP 协议版本分布；每代理 **Cost 分解**显示哪个分量主导排序；贝叶斯平滑成功率带低样本警示
- **域名统计** —— 每域名胜出次数持久化到 SQLite，重启不丢失
- **Web 界面** —— 内置仪表盘 `/`，浏览域名统计、默认代理、胜出次数并自动刷新；点击统计卡片可过滤出以该代理为默认代理的域名
- **配置驱动** —— 全部旋钮在 `config.yaml`（`router.circuit.*`、`router.stickiness`、`router.conn_pool` 等），加载时校验（`extra="forbid"`、跨字段校验、清晰退出码 2 与可读报错）
- **客户端认证** —— 可选代理端口（`:10808`）与/或管理 API（`:18080`）的 HTTP Basic 认证，默认关闭
- **逐跳头过滤** —— 转发动态前剥离 `proxy-authorization`（客户端凭证不会泄露）；剥离 transfer/encoding 头后重写 `Content-Length` 为实际体长
- **优雅关闭** —— 在途连接取消并排空后再关闭 DB

## 端口

| 端口 | 用途 |
|------|------|
| `10808` | 代理端口 —— HTTP/HTTPS 流量进入（`-x http://127.0.0.1:10808 ...`） |
| `18080` | 管理 API + Web UI（`/health`、`/proxies`、`/metrics`、`/`） |

## 请求链路

每个连接走同一条决策链（全部在 `auto_squid/router.py`）：

1. **连接与认证** —— `handle_client` 读取请求头；在任何上游工作之前校验 `Proxy-Authorization`（407）。`CONNECT` → `_handle_connect`，普通 HTTP → `_handle_http_request`。
2. **短路检查** —— `local_direct_domains` 白名单（直接拨源站，不走上游）；在途 GET 合并。
3. **调度决策**（`_dispatch_single`）—— 按优先级：
   - **会话粘性命中** → 钉住代理上单发（不竞速）
   - **域名缓存命中** → 域名最后胜出代理上单发（`cache_ttl` 内）
   - **竞速**（`_race_staggered`）—— Cost 排序候选，错峰启动；首个首字节胜出
4. **退化反馈** —— 失败的单发触发 `_degrade_send_proxy`，使该域名的*下一次*请求跳过粘性与缓存、直接回落竞速；新胜出建立时清除即时退化标记。
5. **失败计数** —— 竞速败者每次尝试调用 `record_failure`，喂入 `selector.py` 的熔断器：`circuit_threshold` 次连续失败后打开熔断，指数退避（8 s → 最多 300 s）。业务拒绝（`http_status`，如 CONNECT 上游返回非 200）**不触发熔断**——只有代理质量信号会。
6. **连接复用** —— 三层连接池位于一切之下：通用上游 TCP 池、目标半预连接、已握手复用；池内陈旧连接在握手时检测并重试全新连接，不惩罚熔断器。

### 各旋钮的作用点

| 旋钮 | 作用点 | 效果 |
|---|---|---|
| `cache_ttl` | 域名缓存（第 3.2 步） | 胜出代理复用时长 |
| `stickiness_*` | 粘性查找（第 3.1 步） | 启用 pinning / TTL / 重查 / 容量 |
| `max_retries` | 竞速（第 3.3 步） | 首批竞速扇出宽度 |
| `cost_sort_enabled` | 候选排序 | 多目标排序 vs 纯 EWMA |
| `circuit_threshold` | 失败计数（第 5 步） | 连续失败几次后打开熔断 |
| `single_send_degrade_*` | 退化门（第 4 步） | 在熔断前先取消钉住的失败单发 |

## 配置

全部配置在 `config.yaml`。以下是带默认值的完整结构（敏感信息已省略）：

```yaml
listen:                        # 代理端口 host/port（默认 0.0.0.0:10808）
api:
  host: "0.0.0.0"              # 管理 API host
  port: 18080                    # 管理 API 端口
  auth:                          # 管理 API 可选 HTTP Basic 认证（默认关闭）
    enabled: true
    username: "..."
    password: "..."
router:
  enable_local_racing: false     # 让网关主机作为代理节点参与竞速
  cache_ttl: 600                 # 域名缓存 TTL（秒）
  max_retries: 3                 # 竞速扇出：前 N 个候选在首批竞速
  stagger_start: true            # RFC 8305 错峰启动（最优先发，按间隔补发）
  stagger_initial: 2             # 无 EWMA 历史时的首批大小（min 1, max max_retries）
  stagger_interval_ms: 100       # RFC 8305 补发间隔（ms；允许下限: 100）
  local_direct_domains: []       # 直接拨源站的目标（不走上游）—— 内网服务
  circuit:                       # 探活 + 熔断 + 单发退化信号
    probe_interval_sec: 20
    probe_canary: "www.baidu.com:443"
    circuit_threshold: 3         # 连续失败次数后打开熔断
    circuit_max_backoff: 300      # 指数退避上限（秒）
    slow_start_window: 60        # 退出慢启动所需连续成功次数
    slow_start_success: 3
    lb_bias: 0.5                 # 在途积压对代理排序的惩罚程度
    single_send_degrade_fail: 2      # 早期预警阈值（连续失败；0=关闭）
    single_send_degrade_ratio: 1.5   # EWMA 相对钉住时基线的恶化比（0=关闭）
    single_send_degrade_slack_ms: 10   # 绝对下限（ms），防止小延迟下误报
    single_send_degrade_success_rate: 0.0  # 域名成功率下降 → 取消钉住（需 ≥8 样本）
    single_send_degrade_p99_ms: 0.0        # 域名 P99 尖峰 → 取消钉住（需 ≥4 样本）
    single_send_degrade_min_throughput: 0.0 # 域名吞吐下降 → 取消钉住（需 ≥4 样本）
    single_send_slow_log_ms: 1500  # 慢单发采样日志（ms；0=关闭）：记录客户端 IP
    connect_tunnel_timeout_sec: 3.0  # CONNECT 连接/读取超时（秒）
    http_read_timeout_sec: 3.0       # HTTP 单发读取超时（秒）
    probe_with_get: false          # CONNECT 探路后追加 GET（Phase 4，默认关闭）
    probe_get_targets: []          # GET 探路白名单
    probe_get_interval_sec: 60.0   # 每 (代理, 目标) 最小间隔
    probe_get_timeout_sec: 5.0
    probe_get_max_bytes: 65536
    cost_sort_enabled: true        # 多目标 Cost 排序（false = 纯 EWMA）
    cost_latency_metric: "p99"     # 延迟项: "p99"（尾部优先）或 "ewma"
    cost_weight_latency: 1.0
    cost_weight_success_rate: 0.6
    cost_weight_throughput: 0.1    # 极小: 隧道流量很少产生有效体吞吐
    cost_latency_min_samples: 1
    cost_throughput_min_bytes: 1000000
  stickiness:                    # 会话粘性（默认关闭）
    enabled: false
    ttl: 1800
    recheck_hits: 100
    max_entries: 100000
  conn_pool:                     # CONNECT TCP 连接池（Phase 1，默认开启）
    enabled: true
    per_proxy: 4
    total: 64
    idle_timeout: 30.0
    refill_interval: 5.0
    refill_target: 2
    connect_timeout: 10.0
    target_prewarm: true         # Phase 2: 为热目标预开"上游 → 目标" TCP
    established_reuse: true      # 复用已握手的空闲隧道
    established_idle_timeout: 300.0
    prehandshake: false          # P1: 额外 CONNECT 预建已握手隧道（需限流!）
    prehandshake_throttle_window_sec: 2.0
    prehandshake_throttle_max_per_window: 6
    cluster_predict: false       # P3: 预建下一页加载的预测共现目标 TCP
    cluster_window_sec: 2.0
    cluster_predict_topk: 3
    cluster_min_support: 2
    cluster_graph_ttl_sec: 86400
    cluster_graph_max_entries: 100000
    cluster_predict_throttle_sec: 30.0
  policies: []                   # 策略路由: 按域名/标签收窄竞速候选集
  adaptive_ttl: {enabled: false, ...}  # 按稳定性调整每域名 TTL
  switch_damping: {enabled: false, ...}  # 当差距很小时抑制代理切换
  concurrency_limit: {enabled: false, ...}  # 每代理自适应并发上限
logging:
  level: INFO                    # 文件日志级别（控制台始终 WARNING+ 避免刷屏）
  file: "auto_squid.log"
```

生产就绪片段见 `examples/config.yaml` 以及下方 [速度调优](#速度调优) 一节。

## 加载时配置校验

`config_schema.py` 用 pydantic 模型定义配置并设 `extra="forbid"`。拼写错误、未知字段、跨字段违规在加载时拦截，报错清晰并**退出码 2**（无 traceback）：

```bash
$ python -m auto_squid.cli
config error: invalid configuration:
  1 validation error for Config
  router.circuit.local_direct_domains
    Extra inputs are not permitted
```

## 代理存储

上游代理声明在 `proxies.yaml`（生产环境 gitignore）：

```yaml
- id: squid-01
  name: beijing-01
  host: 10.14.25.86
  port: 3128
  protocol: http              # http | https | socks5
  auth:                        # 可选；省略 = 无认证
    username: "..."
    password: "..."
  enabled: true
  tags:                        # 可选，用于策略路由
    region: "cn"
```

`ProxyStore` 支持运行时 CRUD（通过管理 API）——变更持久化回 `proxies.yaml`。

## API 端点

全部在管理端口 `:18080`。`POST` 体为 JSON；未知字段会被拒绝（`422`）。启用 `api.auth` 时，除 `/health` 外所有端点需要 HTTP Basic 认证（通过 `Authorization` 或 `Proxy-Authorization`）。

### 代理与统计

| 端点 | 说明 |
|----------|-------------|
| `GET /` | Web UI 仪表盘（域名统计、默认代理、胜出次数、自动刷新） |
| `GET /health` | 健康检查（始终开放） |
| `GET /proxies` | 列出已配置代理 |
| `POST /proxies` | 添加代理（`ProxyIn` JSON）；持久化到 `proxies.yaml` |
| `GET /stats` | 每代理 `request_counts`（胜出）+ `attempted_counts`（总尝试） |
| `GET /domains` | 每域名胜出统计（SQLite） |
| `GET /domains/meta` | 每域名默认代理 + 最后更新时间（adaptive TTL 开启时含 TTL 字段） |
| `GET /stickiness` | 会话粘性表（client_ip\|domain → 粘性代理） |
| `GET /policies` | 策略路由配置快照 |
| `GET /config` | 路由器配置快照 |

### 指标与质量

| 端点 | 说明 |
|----------|-------------|
| `GET /metrics` | `request_counts`、`attempted_counts`、域名统计、服务器性能计数器（缓存命中、竞速扇出、探活计数器、池大小等） |
| `GET /metrics/per-destination` | 每 (域名, 代理) 指标：窗口分位数、成功率、错误分类，外加 `cumulative` 子对象（终身均值、平滑成功率、t-digest 分位数、总字节） |
| `GET /quality` | 每代理 EWMA 首字节延迟（秒）—— 旧版竞速排序依据 |
| `GET /quality/meta` | 增强版每代理指标：窗口 P50/P95/P99、平滑成功率、错误分类、HTTP 协议版本计数、`cumulative` 终身对象、每代理 `cost_breakdown` |
| `POST /quality/reset` | 清除全部代理 EWMA 质量 |
| `GET /circuit` | 每代理熔断器 + 探活状态（`open`、退避、`probes_sent/ok/skipped`、`single_send_degrades`、慢单发日志计数） |
| `POST /circuit/reset` | 解除全部熔断，保留 EWMA 质量 |
| `GET /server-stats` | 服务器资源采样（CPU%、事件循环延迟），由 bench 子进程填充 |

### Cost 排序与自动调参（P1）

| 端点 | 说明 |
|----------|-------------|
| `GET /cost` | 当前 Cost 参数（7 个字段）、自动调参器状态、安全边界 |
| `POST /cost` | 热更新任意 Cost 子集 —— **下一次竞速即生效，无需重启** |
| `POST /tuner` | `{"enabled": true\|false}` —— 运行时切换自动调参器 |

```bash
# 哪个分量主导各代理的排序？
curl http://127.0.0.1:18080/quality/meta | jq '.["239-192"].cost_breakdown'
# → {"rank":2,"cost":0.31,"latency":{"contrib":0.21},
#     "success_rate":{"contrib":0.05},"throughput":{...}}

# 提高吞吐权重 —— 无需重启
curl -X POST http://127.0.0.1:18080/cost \
     -H 'Content-Type: application/json' \
     -d '{"cost_weight_throughput": 0.4}'

# 即时回滚，不动进程
curl -X POST http://127.0.0.1:18080/cost -d '{"cost_sort_enabled": false}'
```

## 速度调优

调优取决于流量特征。以下是三个起步模板（完整 YAML 见 `examples/config.yaml`）：

**稳定性优先**（稳定出站 IP、登录/反爬敏感站点）：

```yaml
router:
  stickiness: {enabled: true, ttl: 1800, recheck_hits: 100}
  circuit:
    probe_interval_sec: 30
    single_send_degrade_fail: 2
```

**速度优先**（最低 TTFB，可接受出站 IP 切换）：

```yaml
router:
  cache_ttl: 900
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 100   # RFC 8305 下限
```

**低扇出优先**（CONNECT 密集，最小化竞速放大）：

```yaml
router:
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 200
  circuit:
    probe_interval_sec: 30
    single_send_degrade_fail: 1   # 在熔断前先取消钉住
```

> **调优提示**
> - `probe_canary` 必须在本地和每个上游都可达 —— 观察 `GET /circuit` 中增长的 `probes_skipped`。
> - `single_send_degrade_fail` 是熔断器的早期预警 —— 设为 `circuit_threshold - 1`。
> - `single_send_slow_log_ms`（默认 0=关闭）是唯一能把"打不开/需要多次刷新"故障归因到具体客户端 IP 的锚点（成功路径不输出 IP 日志）。
> - `http_read_timeout_sec` 默认 3 s —— 在进一步收紧前，先在生产观察 p99/fd；详见文末 [局限性](#局限) 小节。
> - **策略路由**（`router.policies`）按域名/标签收窄竞速候选集 —— 形状见 `examples/config.yaml`。
> - `conn_pool.target_prewarm` 需要 `conn_pool.enabled`；`conn_pool.prehandshake` 需要 `enabled` + `target_prewarm` + `established_reuse` —— 三者全开或全关。
> - `conn_pool.cluster_predict` 需要 `conn_pool.enabled` **且** `conn_pool.target_prewarm`。
> - **逃生走廊（fail-open）：** 当所有外部代理都熔断且该域名在本机可达时，路由器会自动回退到直接拨 `local` —— 无需额外配置。

## 容器部署（Docker / docker compose）

一个多阶段镜像和 compose 示例让你一条命令运行 auto_squid，带持久化数据卷和健康检查：

```bash
docker compose -f examples/docker/docker-compose.yml build
docker compose -f examples/docker/docker-compose.yml up -d
curl http://127.0.0.1:18080/health
curl -x http://127.0.0.1:10808 http://www.baidu.com
```

- 镜像内的默认上游是占位符，仅供自举验证 —— 接入真实代理见 [`examples/docker/README.md`](examples/docker/README.md)（挂载你自己的 `proxies.yaml` 或构建时注入节点 ID）。
- SQLite 统计持久化在 `./data` 卷；日志输出到 stdout（`docker compose logs -f`）。
- 镜像以非 root 用户运行，暴露端口 `10808` 和 `18080`。

## 测试

```bash
.venv/bin/python -m pytest -q
```

测试套件覆盖 HTTP/CONNECT 转发、HTTP 响应缓存（LRU/淘汰、在途合并）、域名缓存、竞速/聚合超时、客户端认证、熔断器/探活/EWMA 选择/在途权重、会话粘性、每域名统计 + SQLite 持久化、UTF-8/二进制安全体处理、连接热池 + 已握手复用 + 空闲暂停、健壮性（头限制、截断响应）、配置层（`extra="forbid"`、跨字段校验、退出码 2）。Phase-2+ 指标/排序层有独立套件：`tests/test_phase_metrics.py` 和 `tests/test_tuner.py`。

CI 通过 GitHub Actions（`.github/workflows/test.yml`）在 **Python 3.10、3.11、3.12** 上运行套件，每测试 60 s 超时。

> **Python 3.12 说明：** 3.12 的 `StreamWriter.wait_closed()` 和 `Server.wait_closed()` 更严格（会等对端 FIN）。路由器对半开池连接加了短超时；测试中的 mock 上游在 5 s 后关闭空闲连接。3.11 无需此处理即可通过。

## 压测

可控、可重复、可归因的压测工具在 `bench/`。mock 上游代理（可配置延迟/响应大小/chunked/失败率，带命中计数器）消除真实网络抖动。详情见 [`bench/README.md`](bench/README.md)。快速参考：

```bash
.venv/bin/python -m bench.stress --quick            # ~10 s 冒烟跑
.venv/bin/python -m bench.stress --mode staircase   # 并发阶梯 → 饱和点
.venv/bin/python -m bench.stress --mode soak        # 稳定性/资源泄漏检查
.venv/bin/python -m bench.stress --rounds 5         # 新鲜子进程 × N 轮，取均值±标准差
.venv/bin/python -m bench.stress --profile          # cProfile → bench_profile.txt
```

关键指标：吞吐、TTFB 与总延迟 P50/P95/P99、按类别错误率、**缓存命中率**与**竞速放大率**（来自服务器端 `/metrics` 计数器 —— mock 与真实上游统一计算），以及资源采样（RSS、fd 数、池大小、CPU%、事件循环延迟）。结果写入带 git 修订版标签的 `bench_report.json`。

独立路由检查器：`test_routing.py` —— 分析指定 URL 的决策链（粘性/域名缓存命中、策略路由、代理排序、熔断状态），无需发真实请求（`--test-request` 实际转发一个）。

## 局限性

- HTTP 解析为 MVP 级别；大流式响应可能存在边界情况
- 管理 API **默认开放** —— 在可信网络外暴露 18080 端口前请启用 `api.auth`（HTTP Basic）
- CONNECT 隧道使用原始管道（无 TLS 拦截）
- `http_read_timeout_sec`（默认 3 s）可能超时高延迟链路上健康但慢的源站 —— 调整前请谨慎并在生产验证 p99/fd
- 内存表（粘性、域名缓存、HTTP 响应缓存）重启后清空；SQLite 仅持久化域名胜出次数与 t-digest 终身指标

## 许可证

MIT
