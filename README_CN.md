# auto_squid（中文说明）

轻量级正向代理，支持**并行竞速**、域名缓存、HTTP 响应缓存，以及 SQLite 持久化的统计。

> [English →](README.md)

## 概述

- 在网关主机运行，接受 HTTP/HTTPS 代理流量，将每个请求转发到上游代理
- **并行竞速 + 错峰启动**：每次请求按 EWMA 延迟排序候选，先发最优 1~2 个（RFC 8305 §5），间隔 ~250ms 补发；首个首字节成功即取消其余并释放连接。错峰大幅减少 CONNECT 隧道扇出与 HTTP 双写流量
- **多目标 Cost 排序（Phase 2）**：竞速候选按「延迟（P99 尾部优先）+ 成功率 + 吞吐」加权 Cost 排列，候选集内 min–max 归一化——"首字节快但爱失败"的代理不再压过"稍慢但稳"的代理。默认开启，`cost_sort_enabled: false` 一键回滚纯 EWMA 排序
- **自调节（P1）**：Cost 权重支持运行时热更新（`POST /cost`，下一次竞速即生效，无需重启）；可选的保守自动调参器（`router.auto_tune`）以实测赢家 TTFB 为目标爬山调权重，带成功率守卫与自动回滚
- **域名缓存**：某个代理为某域名竞速胜出后，在 `cache_ttl` 有效期内复用该代理，避免每个请求都竞速
- **会话粘性**：可选，同一客户端 IP + 域名/目标复用同一代理（保持 egress IP 稳定），粘性代理失败/返回 5xx 即驱逐并回落竞速（redispatch），并按 `recheck_hits` 周期探路重竞速
- **HTTP 响应缓存**：幂等 `GET` 响应在内存中缓存（TTL 60s，遵循 `Cache-Control`）
- **本机竞速**：可选，让网关主机自身作为代理节点直接参与竞速（不走上游）
- **可观测性**：每代理/每域名的窗口（近期 256 次）与**终身**（t-digest，跨重启）双族分位数、错误分类、HTTP 协议版本分布、能看出"哪个分量主导排序"的 **Cost 分解**、带低样本警示的贝叶斯平滑成功率
- **域名统计**：各域名胜出次数持久化到 SQLite，重启不丢失
- **Web 界面**：内置仪表盘 `/`，可浏览域名统计、默认代理、胜出次数，支持自动刷新；点击统计卡片可过滤出以该代理为 Default Proxy 的域名

## 功能

- HTTP 与 HTTPS（`CONNECT`）转发，**并行竞速多个上游代理**（按 EWMA 排序 + 错峰启动）
- **多目标 Cost 排序**（Phase 2，默认开启）：候选序 = `w_延迟·norm(延迟) + w_成功率·norm(1−成功率) + w_吞吐·norm(1−吞吐)`，候选集内 min–max 归一化（权重直接可比、与量纲无关）。延迟主项用**终身 TTFB P99**（t-digest，尾部优先），不足回退 EWMA；成功率 Laplace 平滑（域名级优先）；吞吐需累计字节过门槛才参与（隧道流量基本测不到按响应体的吞吐，故权重极小）。缺数据项中性（0.5）。`cost_sort_enabled: false` = 一键回滚纯 EWMA 排序
- **Cost 热更新 + 自动调参器**（P1）：`GET/POST /cost` 运行时读/改全部 Cost 参数（下一次竞速即生效，无需重启）；`POST /tuner` 启停自动调参器——每评估窗口对三权重做一次 ±25% 单维扰动，赢家 TTFB 均值改进 ≥5% 且不拿成功率换延迟（守卫）才采纳，恶化立即回滚，已采纳基线持久化到 SQLite
- **单发降级门控**（Phase 3，阈值默认全关）：除连续失败/EWMA 恶化信号外，被钉住（粘性/域名缓存）的代理在域名级成功率、P99 延迟或吞吐越过阈值时主动降级回竞速——补上"握手快但爱失败/下载慢"的盲区
- **业务对齐探测**（Phase 4，`probe_with_get`，默认关闭）：可选在 CONNECT 探活后经该上游对白名单 URL 发一次轻量 GET（按 代理+目标 限速、独立短连接），把 TTFB/协议/吞吐写入**该域名的真实指标桶**——探活延迟终于贴近业务延迟
- **指标鲁棒性**：自包含 t-digest 的终身分位数（内存有界、JSON 落盘、跨重启）与近期 256 窗口并存；低样本分位数带 `low_confidence` 警示；成功率 Laplace 平滑，1/1 的代理不再显示完美 1.0
- **HTTP 协议版本统计**（Phase 1.4）：HTTP 路径按代理/域名累计 `HTTP/1.1`/`HTTP/2`/`HTTP/3` 计数（H2 占比兼作连接复用的代理信号；CONNECT 隧道的复用/TLS 会话恢复对正向代理架构上不可见，不伪造指标）
- 域名级缓存（`cache_ttl`），按域名复用胜出代理
- 会话粘性（per-client+domain，内存-only，滑动 TTL），粘性代理失败自动回落竞速并回填；5xx 驱逐、周期重竞速、容量上限
- 慢单发采样日志（`router.circuit.single_send_slow_log_ms`）：粘性/域名缓存命中的单发（跳过竞速的路径）"发起到首字节"耗时超阈值即记一条**带客户端 IP** 的日志——成功路径不打 IP 日志,这是按 IP 归因"打不开/要反复刷新"的唯一锚点
- CONNECT 上游 TCP 预热池（第一阶段，`router.conn_pool`）：为每上游维护少量空闲 TCP，CONNECT 跳过"本机→上游代理"建连；目标半预连接（第二阶段，`conn_pool.target_prewarm`）：命中域名缓存/粘性**或竞速胜出**的高频 CONNECT target 后台预建"到上游"的 TCP（每条 target 补 2 条、取走仍留 1 条备用），与第一阶段共享 fd 预算与空闲超时
- 请求簇预测预热（第三阶段，`conn_pool.cluster_predict`）：学习客户端页面加载窗口内 CONNECT target 的共现规律,下次窗口开口即预测同簇 co-target 并预建"到上游代理"的裸 TCP(不 CONNECT 源站),把子资源突发期的建连 TTFB 省掉
- 内存级 HTTP `GET` 响应缓存，遵循 `Cache-Control`
- 在途 GET 去重聚合：同 URL 并发 GET 命中未命中缓存时，等待在途的上游请求结果，不再重复打上游（有界等待，超时回落竞速）
- 写方法缓存失效：`POST`/`PUT`/`DELETE`/`PATCH` 转发前清空该域名下所有已缓存 `GET` 响应，后续 `GET` 不会返回过期内容
- 可选本机竞速节点（网关与上游一同竞速）
- Hop-by-hop 头双向过滤：请求头（`proxy-authorization`、`connection` 等）在转发上游前剔除，避免客户端访问本代理的凭据泄漏到下一跳；响应头（`transfer-encoding`、`content-encoding`、`content-length` 等）剔除并按实际 body 长度重写 `Content-Length`
- 请求体处理设有 10 MB 上限（超限返回 `413`）；`Content-Length: 0` 处理正确（不会卡死）
- CONNECT 隧道设有连接/读取超时，挂死的上游不会永久占用竞速槽位
- SQLite 访问加锁串行化，在 FastAPI/uvicorn 线程池下安全
- 优雅关闭：先取消并排空在途连接，再关闭数据库
- 运行时 ProxyStore（YAML 加载/保存），管理 API CRUD
- 域名级胜出统计持久化到 SQLite（`auto_squid.db`）
- 管理 API + 单页 Web 界面

## 客户端认证

代理端口（`:10808`）可要求客户端通过 HTTP Basic 认证。**默认关闭**，在 `config.yaml` 中开启：

```yaml
router:
  auth:
    enabled: true
    username: "admin"
    password: "secret"
```

开启后，每个请求（HTTP 与 `CONNECT`）都需携带 `Proxy-Authorization: Basic <base64(用户名:密码)>` 头（客户端可回退到 `Authorization`）。缺失或凭据错误时返回 `407 Proxy Authentication Required`，并带 `Proxy-Authenticate: Basic realm="auto_squid"`，**不会进行任何上游请求**。

```bash
# 被拒绝（无凭据）
curl -x http://127.0.0.1:10808 http://example.com        # → 407
# 被接受
curl -x http://admin:secret@127.0.0.1:10808 http://example.com
```

> 认证默认仅保护**代理端口**。管理 API（`:18080`）自带可选 HTTP Basic 认证（`api.auth`，默认关闭）：开启后除 `/health` 外全部管理端点均需凭据。

### 管理 API 认证

管理 API（`:18080`）**默认开放**。如需保护，在 `config.yaml` 中开启：

```yaml
api:
  auth:
    enabled: true
    username: "admin"
    password: "secret"
```

开启后，除 `/health` 外的全部端点无凭据访问返回 `401`：

```bash
curl http://127.0.0.1:18080/proxies        # → 401
curl -u admin:secret http://127.0.0.1:18080/proxies   # → 200
curl http://127.0.0.1:18080/health          # 始终开放
```

内置仪表盘（`/`）使用同一套认证：浏览器打开后输入凭据，自动刷新请求会复用（浏览器按 origin 缓存 Basic 凭据）。`/health` 保持开放，供负载均衡与监控探活。

## 会话粘性

**默认关闭**。开启后，同一客户端 IP 访问同一域名/目标时，复用该键上次胜出的代理单发（跳过竞速），保持 **egress IP 稳定**——目标站点常把登录态/风控/CAPTCHA 与出口 IP 绑定，竞速中途换代理会导致掉登录、触发风控。

```yaml
router:
  stickiness:
    enabled: true
    ttl: 1800          # 粘性有效期（秒），粘性命中成功会滑动刷新，活跃会话不过期
    recheck_hits: 100  # 粘性命中 N 次后触发一次探路重竞速（0=关闭），默认 100
    max_entries: 100000 # 粘性表容量硬上限，超限驱逐更新时间最旧的一条
```

行为要点：

- 键为 `客户端IP|域名`（HTTP 用 URL 域名，CONNECT 用 `host:port`），**优先级高于域名缓存**；粘性命中即单发，未命中再查域名缓存，最后才竞速。
- **redispatch**：粘性代理单发失败 → 驱逐该条目 → 回落域名缓存/竞速；竞速赢家回填粘性表，下一次请求自动切换到新代理。
- **5xx 驱逐**：粘性单发成功但返回 HTTP 5xx 同样驱逐（响应已流式发出无法重试，下一请求竞速换新），防止坏代理长期霸占出口。
- **探路重竞速（recheck）**：粘性命中累计 `recheck_hits` 次后，驱逐旧条目并**跳过域名缓存**直接竞速，用新赢家替换可能已变慢的粘性代理；新赢家命中计数归零。
- **本机竞速（local）**：`enable_local_racing` 开启时，本机胜出也会写入粘性表并稳定粘住（直连），不再被误判为失效代理驱逐。
- **容量上限**：`max_entries` 硬上限，写前先清过期条目，仍超限则驱逐 `updated_at` 最旧的一条，防止客户端 IP 集合过大时内存无界增长。
- 纯内存（HAProxy 式内存表），重启即清；后台周期清扫过期条目，防止无界增长。
- 粘性代理被删除/停用后，取用时会校验并自动驱逐。
- 查看当前粘性表：`curl http://127.0.0.1:18080/stickiness`；管理面板首页可切换"会话粘性"视图查看全表与 条目/命中/驱逐 统计。

## 快速开始

1. 创建虚拟环境并安装依赖：

   ```bash
   uv venv .venv --seed && uv sync
   ```

   运行依赖：`fastapi`、`uvicorn[standard]`、`httpx`、`pydantic`、`typer`、`pyyaml`。
   开发依赖：`pytest`、`pytest-asyncio`（`asyncio_mode = "auto"`）。

2. 准备 `proxies.yaml`：

   ```yaml
   - id: squid-01
     name: beijing-01
     host: 10.14.25.86
     port: 3128
     protocol: http
     auth:
       username: "user"
       password: "pass"
     enabled: true
   ```

3. 启动：

   ```bash
   python -m auto_squid.cli
   # 或使用已安装的入口：
   auto-squid
   ```

   可选参数：`--proxies ./proxies.yaml` `--db ./auto_squid.db` `--config ./config.yaml`

4. 验证：

   ```bash
   curl http://127.0.0.1:18080/health
   curl http://127.0.0.1:18080/proxies
   curl http://127.0.0.1:18080/stats
   curl http://127.0.0.1:18080/domains
   ```

   浏览器打开仪表盘：`http://127.0.0.1:18080/`

5. 作为代理使用：

   ```bash
   curl -x http://127.0.0.1:10808 http://www.baidu.com
   curl -x http://127.0.0.1:10808 https://www.baidu.com
   ```

## 架构

```
客户端 ──HTTP/S──> auto_squid（代理 :10808）
                      │
                      ├── 竞速 ──> 上游代理 1（squid）
                      ├── 竞速 ──> 上游代理 2（squid）
                      ├── 竞速 ──> 上游代理 3（squid）
                      └── 竞速 ──> 本机（可选，直连）
                      │
                      ▼ 最先成功的响应被使用，其余取消并关闭
                      │
                      ▼ 按域名缓存默认代理（cache_ttl）
```

- **HTTP 请求**：通过 `httpx.AsyncClient` 并行竞速（流式，每个上游一个长驻池化 client），胜出响应回写，落败响应关闭
- **CONNECT 请求**：通过 `asyncio.open_connection` 隧道并行竞速，带连接/读取超时
- **选择**：`ProxySelector` 产出竞速顺序——已启用代理、剔除熔断中者，按加权 least-request（`ewma × (1 + active)^bias`，快且空闲者靠前）排序，slow-start 恢复期与未知质量代理垫底；前 `max_retries` 个组成首批（错峰启动），失败后再对剩余代理竞速兜底
- **域名缓存**：胜出后把代理记入 `domain_meta`，在 `cache_ttl` 有效期内对该域名复用
- **会话粘性**：同一客户端 IP + 域名/目标复用上次胜出的代理单发（跳过竞速），优先级高于域名缓存；粘性代理失败则驱逐该条目并回落竞速（redispatch），竞速赢家回填粘性表
- **统计**：`request_counts` 各代理胜出次数，`attempted_counts` 各代理尝试次数；`domain_stats` 各域名下各代理胜出次数（SQLite 持久化）

### 模块结构

路由核心被拆成一个薄编排层 + 若干聚焦的协作类模块。每个协作类持有自己的状态与方法；`Router` 经白名单 `__getattr__`/`__setattr__` 把热路径成员名转发到对应协作对象——`api.py`/测试/压测的调用方继续用同名属性，无需改动。

| 文件 | 负责 |
|------|------|
| `router.py` | `Router`——客户端处理、竞速、域名缓存、单发降级、策略路由、SQLite 持久化，以及到各协作类的转发 shim |
| `selector.py` | `ProxySelector`——每代理 EWMA 延迟、熔断 + slow-start、自适应并发限制、多目标 Cost 排序与分解、竞速排序 |
| `tuner.py` | `AutoTuner`——三个 Cost 权重的保守爬山自动调参器（P1，默认关闭） |
| `digest.py` | `TDigest`——自包含 t-digest（dict 子类、可 JSON 落盘），支撑终身分位数 |
| `pools.py` | `ConnectionPools`——三套 CONNECT 预热池（通用池 / 目标半预连接 / 已建握手复用） |
| `sticky.py` | `StickyCache`——per-client+domain 会话粘性表 |
| `http_cache.py` | `HttpCache`——GET 响应缓存（LRU、在途去重聚合、写方法失效） |
| `cluster.py` | `ClusterGraph`——请求簇共现图（per-client 窗口 → 全局边）；预测页面簇的下一批 co-target 并预建"到上游"的裸 TCP |
| `config_schema.py` | `Config` / `RouterConfig` / … pydantic 模型（`extra="forbid"`、跨字段校验） |
| `api.py` / `cli.py` | 管理 API + 仪表盘；入口（uvloop、配置加载、uvicorn） |

domain_cache 簇（`_meta_cache`、自适应 TTL、切换阻尼、质量感知单发降级、SQLite 持久化）仍留在 `Router`——它与胜出记录决策链紧耦合且被 `api.py` 直读。

## API 端点

均在管理端口 `:18080`。`POST` 请求体为 JSON；未知字段直接拒绝（`422`）。`api.auth` 开启后，除 `/health` 外全部端点需要 HTTP Basic 凭据。

### 代理与统计

| 端点 | 说明 |
|------|------|
| `GET /` | Web 仪表盘（域名统计、默认代理、自动刷新；点击统计卡片可按 Default Proxy 过滤域名） |
| `GET /health` | 健康检查（始终开放，`api.auth` 开启时也不例外） |
| `GET /proxies` | 列出已配置代理 |
| `POST /proxies` | 添加代理（`ProxyIn` JSON：`id`、`host`、`port`、`protocol`、`auth`、`enabled`、`tags`）；持久化到 `proxies.yaml` |
| `GET /stats` | `request_counts`（胜出）+ `attempted_counts`（总尝试） |
| `GET /domains` | 从 SQLite 读取的域名胜出统计 |
| `GET /domains/meta` | 各域名默认代理 + 最近更新时间（自适应 TTL 开启时含 `ttl`/`expires_at`/`switch_count`） |
| `GET /stickiness` | 会话粘性表（客户端IP\|域名 → 粘性代理 + 更新时间） |
| `GET /policies` | 策略路由配置快照（匹配条件 + 允许的代理子集） |
| `GET /config` | 路由配置（`enable_local_racing`） |

### 指标与质量

| 端点 | 说明 |
|------|------|
| `GET /metrics` | `request_counts`、`attempted_counts`、域名统计与服务端性能计数器（缓存命中、竞速扇出、探活计数含 `probe_get_sent/ok/failed/throttled`） |
| `GET /metrics/per-destination` | 按（域名，代理）的实测指标：窗口分位数、成功率、错误分类，及 `cumulative` 子对象（终身均值、平滑成功率、t-digest 的 `ttfb_percentiles`/`ofb_percentiles`、累计字节） |
| `GET /quality` | 各代理 EWMA 首字节延迟（秒），旧版竞速排序依据 |
| `GET /quality/meta` | **增强版每代理指标**：窗口 P50/P95/P99、平滑窗口成功率、错误分类、HTTP 协议版本计数、`cumulative` 终身对象，以及每代理的 `cost_breakdown`（见 [Cost 排序与自动调参](#cost-排序与自动调参)） |
| `POST /quality/reset` | 清空全部代理 EWMA 质量（网络切换后调用） |
| `GET /circuit` | 各代理熔断 + 探活状态（`open`、退避、`probes_sent/ok/skipped`、`single_send_degrades`、`single_send_slow_log_ms`/`logged`） |
| `POST /circuit/reset` | 手动解除全部熔断（保留 EWMA 质量） |
| `GET /server-stats` | 服务端资源采样（CPU 占用、事件循环延迟），由压测子进程填充；正常运行返回空快照 |

### Cost 排序与自动调参（P1）

| 端点 | 说明 |
|------|------|
| `GET /cost` | 当前 7 个 Cost 参数、自动调参器状态（`enabled`、安全边界、`baseline`、窗口样本、`last_decision`） |
| `POST /cost` | 热更新任意子集：`cost_sort_enabled`、`cost_latency_metric`（`"p99"`/`"ewma"`）、`cost_weight_latency`、`cost_weight_success_rate`、`cost_weight_throughput`、`cost_latency_min_samples`、`cost_throughput_min_bytes`——**下一次竞速即生效**，无需重启。负权重钳 0；调参器开启时手动更新会使其重测基线（下一窗口把手动值测为新已知好点） |
| `POST /tuner` | `{"enabled": true|false}`——运行时启停自动调参器。开启=全新开始（重测基线）；关闭=回滚到最近采纳的基线权重。详见 [Cost 排序与自动调参](#cost-排序与自动调参) |

使用示例：

```bash
# 当前排序长什么样?每个代理哪个分量在主导?
curl http://127.0.0.1:18080/quality/meta | jq '.["239-192"].cost_breakdown'
# → {"rank":2,"cost":0.31,"latency":{"raw":0.124,...,"contrib":0.21},
#     "success_rate":{"failure":0.026,...,"contrib":0.05},"throughput":{...},"load_mult":1.0}

# 成功率还行但下载慢 → 调大吞吐权重,无需重启
curl -X POST http://127.0.0.1:18080/cost \
     -H 'Content-Type: application/json' \
     -d '{"cost_weight_throughput": 0.4}'

# 行为不对劲 → 立即回滚,不碰进程
curl -X POST http://127.0.0.1:18080/cost -d '{"cost_sort_enabled": false}'

# 运行时启停自动调参器
curl -X POST http://127.0.0.1:18080/tuner -d '{"enabled": true}'
```

每代理 `cost_breakdown` 含：`rank`（与竞速同排序键：slow-start 分层 → 未知质量 → cost）、`cost`（总）、三分量的 `raw`/`norm`（0=最优，1=最差，0.5=无数据）/`contrib`（权重×norm——**贡献最大者即当前排序主导因素**）、`load_mult`（连续失败×在途惩罚，已折进延迟）、`slow_start_rank`/`unknown_quality` 标记（区分"cost 差"与"恢复期垫底"）。不在当前候选集的代理（熔断/禁用/并发超限）为 `cost_breakdown: null`。

## 配置

通过 `--config` 传入 YAML 配置文件，结构见 `config_schema.py`：

```yaml
listen:
  host: "0.0.0.0"
  port: 10808
api:
  host: "0.0.0.0"
  port: 18080
router:
  enable_local_racing: false   # 让网关主机作为代理节点参与竞速
  cache_ttl: 600               # 域名缓存有效期（秒）
  stickiness:
    enabled: false             # 会话粘性（per-client+domain）
    ttl: 1800                  # 粘性有效期（秒），滑动刷新
    recheck_hits: 100          # 粘性命中 N 次后触发探路重竞速（0=关闭）
    max_entries: 100000        # 粘性表容量硬上限
  circuit:
    single_send_degrade_fail: 2     # 单发降级:连续失败阈值(熔断早告警,0=关闭)
    single_send_degrade_ratio: 3.0  # 单发降级:EWMA 相对钉住基线恶化倍数(0=关闭)
    single_send_degrade_slack_ms: 10  # 降级绝对下限(ms),防极低延迟误判
    # ── Phase 3: 域名级单发降级信号(默认全 0=关闭) ──
    single_send_degrade_success_rate: 0.0   # 域名成功率低于此值即降级(需样本>=8)
    single_send_degrade_p99_ms: 0.0         # TTFB/OFB 取较大者,超此毫秒即降级(需样本>=4)
    single_send_degrade_min_throughput: 0.0 # 域名吞吐 EWMA 低于此 MB/s 即降级(需样本>=4)
    single_send_slow_log_ms: 1500     # 慢单发采样日志(ms,0=关闭):粘性/域缓存命中的单发"发起到首字节"耗时超阈值即记一条含客户端 IP 的日志(成功路径不打 IP,这是按 IP 归因"打不开/要刷新"的唯一锚点);单发失败(建连/握手超阈值)记 slow single send FAILED(计入 single_send_fail_logged),补上建连失败型卡顿的 IP 归因
    connect_tunnel_timeout_sec: 3.0   # CONNECT 隧道建连/读响应超时(秒,默认 3,原硬编码 15):_try_tunnel 向源站 CONNECT 的统一上限,防某代理 egress→源站建连/握手偶发卡死把请求拖成 10s+。测得 CDN 首字节实际 0.6s,3s 给 5 倍余量
    http_read_timeout_sec: 3.0        # HTTP 单发读首字节超时(秒,默认 3,原 10):_upstream_timeout.read。曾有收紧 header 等待非净赢(引爆 soak p99+fd 已回退),生产灰度须盯 p99/fd
    # ── Phase 4: 业务对齐探测(默认关闭) ──
    probe_with_get: false             # CONNECT 探活后追加一次轻量 GET
    probe_get_targets: []             # 白名单,如 ["https://api.github.com/"];按代理轮转
    probe_get_interval_sec: 60.0      # 每(代理,目标)最小间隔——不要打爆目标站
    probe_get_timeout_sec: 5.0
    probe_get_max_bytes: 65536        # 够算吞吐即可,不做大下载
    # ── Phase 2: 多目标 Cost 排序(默认开启) ──
    cost_sort_enabled: true           # false = 一键回滚纯 EWMA 排序
    cost_latency_metric: "p99"        # 延迟主项: "p99"(尾部优先) 或 "ewma"
    cost_weight_latency: 1.0
    cost_weight_success_rate: 0.6
    cost_weight_throughput: 0.1       # 保持极小:隧道流量基本测不到按响应体的吞吐
    cost_latency_min_samples: 1       # P99 项所需 digest 最小样本(1 = 与 EWMA obs>=1 一致)
    cost_throughput_min_bytes: 1000000  # 吞吐项需累计字节 >=1MB
  # ── P1: Cost 权重自动调参器(默认关闭) ──
  # auto_tune:
  #   enabled: true        # 对三权重做保守爬山
  #   window_sec: 900      # 评估窗口(15 分钟)
  #   min_samples: 50      # 每窗口最少赢家样本数(不足自动扩窗,最多 3 次)
  #   step: 0.25           # 每窗口单维度 ±25% 扰动
  #   hysteresis: 0.05     # 改进 >=5% 才采纳;恶化超 5% 判退化
  #   sr_guard: 0.005      # 试跑成功率跌幅超 0.5pp 即拒绝
  #   persist: true        # 已采纳基线持久化到 SQLite,跨重启恢复
logging:
  file: "auto_squid.log"
```

## Cost 排序与自动调参

Phase 2 把单信号（EWMA）竞速排序升级为**多目标 Cost**——根治"首字节快但爱失败/下载慢"的代理被选中。权重依据生产数据推定：成功率高度聚集（0.94–0.98，区分度低）、TTFB 跨度大（66–151ms，主信号）、隧道流量基本测不到按响应体的吞吐（近 0 → 极小权重 + 1MB 字节门槛）。

**观测工作流**（整套功能围绕这个闭环搭建）：

1. `python test_routing.py --metrics`（或 `GET /quality/meta`）——每个代理一行 `cost_breakdown`，如 `rank=3 cost=1.65 [延迟1.00 成功率0.60 吞吐0.05] (p99=800ms 失败率=50.0%)`。**贡献最大的分量即当前排序主导因素**——那就是值得调的权重。
2. 观察几天。代理因 P99 尾部尖峰频繁换位？那是 `cost_latency_metric: "p99"` 在起作用；嫌抖就换 `"ewma"`。
3. 调参要么手动（`POST /cost`，下一次竞速即生效），要么交给自动调参器（`auto_tune.enabled: true` / `POST /tuner`）。

**自动调参器保证**（保守爬山，(1+1)-ES 风格）：

- 每窗口（默认 15 分钟）只试跑**一个** ±25% 扰动（维度×方向轮转）；硬安全边界（`latency ∈ [0.2, 4.0]`、`success_rate ∈ [0, 2.0]`、`throughput ∈ [0, 1.0]`）。
- 目标是**赢家 TTFB 均值**（纯赢家样本，在竞速胜出点经 task 侧信道采集——单用 `record_ttfb` 会混入未被取消的败者）。
- 采纳需 **改进 ≥5%** 且成功率跌幅 ≤0.5pp；恶化 ≥5%（或守卫被突破）**立即回滚**；两者之间视为噪声，换下一个扰动重试。
- 每 10 个窗口强制重测基线（防流量结构漂移）；低流量窗口自动扩窗（最多 3 次）而不是在噪声上做决策；已采纳基线持久化 SQLite（`tuner_state`），跨重启恢复。
- 调参器只动三个权重——绝不碰 stagger/max_retries/超时。三重熔断：`POST /tuner {"enabled": false}` → `POST /cost {"cost_sort_enabled": false}` → 改配置重启。

## 速度调优

代理的大部分延迟优化杠杆已内置(`examples/config.yaml` 展示了全部字段)。按流量特征选参数。三套推荐配置:

**稳定优先**(egress IP 稳定,登录态/风控敏感站点):

```yaml
router:
  stickiness:
    enabled: true
    ttl: 1800
    recheck_hits: 100
  circuit:
    probe_interval_sec: 30
    probe_canary: "www.baidu.com:443"
    single_send_degrade_fail: 2
    single_send_degrade_ratio: 3.0
    single_send_degrade_slack_ms: 10
```

**速度优先**(低 TTFB,容忍出口切换):

```yaml
router:
  cache_ttl: 900
  stagger_start: true
  stagger_initial: 1        # 先发最优代理,按 interval 补发
  stagger_interval_ms: 100  # RFC 8305 允许的最短补发间隔
  circuit:
    probe_interval_sec: 20
    lb_bias: 0.5            # 不要过度避让最快代理
    single_send_degrade_fail: 2
    single_send_degrade_ratio: 3.0
    single_send_degrade_slack_ms: 10
```

**低扇出优先**(CONNECT 占比高,希望最小化竞速放大):

```yaml
router:
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 200
  circuit:
    probe_interval_sec: 30
    lb_bias: 1.0
    single_send_degrade_fail: 1   # 比熔断更早脱离钉住
    single_send_degrade_ratio: 2.0
    single_send_degrade_slack_ms: 10
```

调参说明:

- **`probe_canary` 必须"本机直连可达 + 经所有上游都可达"。** 部署后看 `GET /circuit`——若 `probes_skipped` 持续增长,说明 canary 不适合当前网络,探活被静默跳过(不会误熔断,但也失去了质量信号)。
- **`single_send_degrade_fail` 是熔断的早告警**:建议设为 `circuit_threshold - 1`(默认 3 时取 2),让被钉住代理开始失败时**先**降级回竞速,而不是等熔断。
- **`single_send_slow_log_ms`**(默认 0=关闭):采样粘性/域名缓存命中单发(跳过竞速的路径)的"发起到首字节"耗时,超阈值即按请求记一条**带客户端 IP** 的日志(`slow single send client=<ip> … ttfb=<ms>`),并计入 `single_send_slow_logged`。单发**失败**(建连超时/握手失败)超阈值也记 `slow single send FAILED`(独立计数 `single_send_fail_logged`)——生产卡顿多为**建连失败型**(被钉代理 egress→源站建连 10s+),只采样成功看不到。成功请求路径完全不打 IP 日志,这是本机上按 IP 归因"打不开/需要反复刷新"的**唯一锚点**。阈值设宽松些(如 1500=1.5s)只捕真慢、避免刷屏;看 `/metrics`(或 opt.log)的 `single_send_slow_logged`/`single_send_fail_logged` 观察两类触发频率。
- **`connect_tunnel_timeout_sec`**(默认 3.0,可配置;原硬编码 15):单次 CONNECT 的统一上限——既含到上游的 `open_connection`,也含向源站 CONNECT 的 `readline` 等 200。被钉上游 egress→源站(Fastly CDN)偶发建连/握手卡死时,这个上限把"卡满 10-15s"变成"卡 3s 后回退竞速"。测得 CDN 首字节 0.6s,3s 是 5 倍余量。除非真实 CDN 首字节持续高于新界,否则只应调低;失败单发自动回退竞速。
- **`http_read_timeout_sec`**(默认 3.0,可配置;原 10):`_upstream_timeout.read`,普通 HTTP 单发读首字节上限。⚠️ 历史曾收紧 HTTP header 等待(`_RACE_HEADER_TIMEOUT`+`asyncio.wait_for`)被回退(非净赢:5s 配置引爆 soak p99+fd 堆积),本值灰度须先验 p99/fd 无回归再放开。
- **`lb_bias`** 控制在途积压对竞速排序的惩罚(`ewma × (1 + active)^bias`)。慢代理易被打爆就调高;最快代理被过度避让就调低。
- **策略路由**(`router.policies`)按域名/标签收窄竞速候选集,直接降低 TTFB 与 `racing.amplification`——形状见 `examples/config.yaml`。
- **`conn_pool.target_prewarm`**(第二阶段)需 `conn_pool.enabled` 为 true;命中域名缓存/粘性**或竞速胜出**(竞速是多数 CONNECT 流量的主体路径,不加则预热只服务极少数缓存命中请求)的高频 CONNECT target 后台预建"到上游"的 TCP,每条 target 补 2 条、取走仍留 1 条备用。`conn_pool.total` 是两阶段合并的全局 fd 预算。看 `/metrics` 的 `target_pool_hits` vs `target_pool_misses` 确认热 target 真的复用了预建连接。
- **`conn_pool.refill_pause_minutes`**(默认 60):连续 N 分钟无客户端请求时(如深夜),后台 refill/目标预热**挂起**,停止"建连→空闲过期→重建"的零流量空转。生产实测:6 代理深夜 6h 空转白建 ~1400 条连接(100% 超时被清,约 233 条/小时)。暂停期间过期连接照常清理(池渐空),任一新请求到来立即恢复补充。设 `0` 保持旧行为(始终 refill)。
- **`conn_pool.refill_pause_activity_window`**(默认 120)与 **`conn_pool.refill_pause_min_requests`**(默认 3):活动判定改为**簇度计数**——真实流量是簇(一次页面加载数秒内对多个 hostname 并发 CONNECT,窗口内计数 5-30),后台心跳(GitHub Desktop 的 `alive.github.com` / Windows 的 `client.wns.windows.com` / Edge 云消息,间隔 3-10 分钟)是孤例(窗口内计数 1,极少 2)。窗口内请求数 ≥ 阈值才刷新活动时间戳——心跳无法阻止空闲暂停,同时**真实孤立请求不再被误伤**(旧 `refill_pause_silence_sec` 的"间隔一刀切"会把任何间隔 >120s 的真实请求当心跳忽略,导致 refill 白天从不恢复)。窗口设 `0` 或阈值 ≤1 保持旧行为(任意请求都刷新)。注意:**空闲暂停只挂起后台预建(refill/目标预热),永不卡请求路径**——暂停期间真实请求照常取池/新建/复用已握手连接。
- **`conn_pool.established_reuse`**(默认 false):复用**已建 CONNECT 握手**的隧道。隧道结束若上游连接干净(无残留缓冲数据)则归还 `_established_pool` 而非关闭;下次同 `(proxy, target)` 请求直接复用,跳过 CONNECT 发送+200 校验——省掉慢线路上的一次完整往返(如 github)。严格验证:有残留即丢弃不复用,宁可不复用也不污染。池受全局 `conn_pool.total` 预算约束(与另两池同口径)且单键上限 2 条;复用前做 50ms 活性探测(`read(1)`),对端已关(FIN/RST)即丢弃并回落新建——死隧道不可能靠零 I/O 赢竞速。归还连接设 `SO_KEEPALIVE`,由 OS 在驻池期间清掉半开对端。看 `/metrics` 的 `established_pool_hits` vs `established_pool_misses` 确认复用。需 `conn_pool.enabled` 为 true。
- **`conn_pool.cluster_predict`**(默认 false,需 `conn_pool.enabled` **且** `conn_pool.target_prewarm`):学习每个客户端页面加载窗口(默认 2s 内的一组 CONNECT target)跨客户端形成的**全局共现图**,在**下一次窗口开口**(HTML 请求刚到达、js/css/CDN 子资源突发还没来)就为预测出的 top-K co-target **提前预建"本机→上游代理"的裸 TCP**(不 CONNECT 到源站)。它是**预测**预热,与 `target_prewarm` 的**被动**预热互补:子资源真实到达时 TCP 已就绪,取用阶梯在 target 池直接取走。错预测的代价只是一条 30s 空闲后被淘汰的 TCP,且所有预建共享 `conn_pool.total` fd 预算。`cluster_window_sec` 决定目标分组簇宽;`cluster_predict_topk` 每次开口最多预测的 co-target 数;`cluster_min_support` 最低共现窗口数(免疫单次偶然共现);`cluster_graph_max_entries` + `cluster_graph_ttl_sec` 约束图体积;`cluster_predict_throttle_sec` 防止 reload 对同对反复预建。看 `/metrics` 的 `cluster_windows_learned` / `cluster_predictions` / `cluster_prewarm_spawned` 三个 cluster 计数器随 `cluster_predict` 开启后变化。

## 容器化部署（Docker / docker compose）

仓库提供多阶段镜像与 compose 示例,一键启动、数据卷持久化、健康检查:

```bash
docker compose -f examples/docker/docker-compose.yml build
docker compose -f examples/docker/docker-compose.yml up -d
curl http://127.0.0.1:18080/health
curl -x http://127.0.0.1:10808 http://www.baidu.com
```

- 默认占位上游仅供自举验证;接入真实代理见 [`examples/docker/README.md`](examples/docker/README.md)
  （挂载自己的 `proxies.yaml` 或构建时注入节点 id）。
- SQLite 统计持久化在 `./data` 卷;日志走 stdout(`docker compose logs -f`)。
- 镜像以非 root 用户运行,`EXPOSE 10808 18080`。

## 测试

```bash
.venv/bin/python -m pytest -q
```

测试套件（**328** 个用例）覆盖 HTTP/CONNECT 转发、HTTP 响应缓存（含 LRU/淘汰与在途去重聚合）、域名缓存、竞速/聚合超时、客户端认证、熔断/探活/EWMA 选路/在途加权、会话粘性（含质量感知单发降级**与慢单发采样日志**）、域名统计 + SQLite 持久化、UTF-8 头安全、二进制安全的请求体处理、连接预热（通用池 + target 半预连接 + 已建握手隧道复用）+ 空闲暂停（含"暂停不卡请求路径"回归）、健壮性（请求头行数/字节上限、截断响应检测）、配置层（`extra="forbid"` 拒绝拼错键、跨字段校验、退出码 2），以及模块拆分回归（`router_cfg=` vs kwarg 等价、池/缓存/粘性转发同一性）。

Phase 2+ 的指标/排序层有专属测试文件：`tests/test_phase_metrics.py`（t-digest 上界/往返/精度、协议版本统计、双作用域双计数回归、Phase 3 降级门控、Phase 4 GET 探测、Phase 2 Cost 排序含 EWMA 等价与回滚等价、Cost 分解）与 `tests/test_tuner.py`（采纳/拒绝/回滚判定、成功率守卫、扰动轮转与边界、赢家 TTFB task 侧信道、基线持久化/恢复、`/cost` + `/tuner` 热更新端点）。

CI 通过 GitHub Actions（`.github/workflows/test.yml`）在 **Python 3.10 与 3.11 与 3.12** 三版本跑测试套件，并带单测试超时（`pytest --timeout=60`），挂起的测试会快速失败并报出测试名，而非无限阻塞任务。

> **Python 3.12 兼容性说明**：3.12 中 `StreamWriter.wait_closed()` 与 `Server.wait_closed()` 变得更严格——会等待对端 FIN / 活跃 handler 协程退出。预热池连接是"半连接"（只建 TCP 未发数据），对端永不主动关闭，因此 router 侧用短超时限制关闭等待、测试里的 mock 上游对空闲连接 5s 超时自动关闭（模拟真实上游 idle 超时）。此问题仅在 CI 的 3.12 矩阵暴露——3.11 无此问题。

## 性能压测

`bench/` 提供一套**可控、可重复、可归因**的压测工具。它启动一组**受控 mock 上游代理**(延迟/响应大小/chunked/失败率可配,每个实例带命中计数器),排除真实网络抖动,再驱动 `Router` 承载负载,输出吞吐、延迟分位、缓存命中率、racing 放大率与资源占用。

> 完整说明见 [`bench/README.md`](bench/README.md)。速查:

```bash
# 默认:mock 上游,并发阶梯(找饱和点)
python -m bench.stress

# 快速冒烟(~10s,小规模)
python -m bench.stress --quick

# 禁用 HTTP 响应缓存,测纯路由性能
# (开/关各跑一次,对照即缓存收益)
python -m bench.stress --no-http-cache

# 四种模式全跑(阶梯 / 速率 / 混合 / 长时)
python -m bench.stress --mode all

# 长时稳定性 + 泄漏检查(默认 60s)
python -m bench.stress --mode soak --duration 120

# cProfile 覆盖(输出 bench_profile.txt)
python -m bench.stress --profile

# 指向真实上游代理(替代 mock 集群)
python -m bench.stress --upstream real --proxies proxies.yaml

# 同一条件跑 N 轮(每轮全新子进程/SQLite/缓存),取均值±标准差去环境噪声(默认 3)
python -m bench.stress --rounds 5
```

模式:

| 模式 | 负载形态 | 回答什么问题 |
|------|---------|------------|
| `staircase` | 并发数 1→800,每级固定请求数 | 吞吐/延迟随并发的变化 → **饱和点** |
| `rate` | 目标 RPS 100→2000,持续发 | 延迟/错误率随负载的变化 → **容量上限** |
| `mixed` | 30%热 + 20%大响应 + 20%chunked + 20%冷 + 10%CONNECT | 贴近真实流量的**混合画像** |
| `soak` | 固定并发长时持续(默认 60s) | **稳定性与资源泄漏** |

关键指标:吞吐(req/s)、TTFB 与 total 的 P50/P95/P99、错误率(按类型分类)、**缓存命中率**与 **racing 放大率**(由服务端 `/metrics` 计数器推导,mock 与 real 两种模式统一)、以及资源采样(RSS、文件描述符数、连接池大小、HTTP 缓存条目数、服务端 CPU 占用与事件循环延迟)。结果在终端以表格输出,并写入 `bench_report.json`(带 git 版本号,跨版本可 diff)。

**多轮取均值(`--rounds N`,默认 3)**:每轮在同一条件下跑——全新的 `server_proc` 子进程、新的 SQLite DB、全新的 Router 缓存与计数、全新的 mock 上游实例——轮间方差纯环境噪声。报告给出各指标的**均值 ± 标准差**,并附 `round_results`(每轮完整数据)与 `aggregates`(min/max/mean/stddev);`--rounds 1` 时报告与单轮版 schema 完全一致。

## 限制

- HTTP 解析为 MVP 级别，大流量流式响应可能有边界情况
- 管理 API 默认开放——将端口 18080 暴露到非可信网络前,请开启 `api.auth`(HTTP Basic)并用防火墙限制访问
- CONNECT 隧道使用原始管道（无 TLS 拦截）

## 许可证

MIT
