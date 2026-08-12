# auto_squid（中文说明）

轻量级正向代理，支持**并行竞速**、域名缓存、HTTP 响应缓存，以及 SQLite 持久化的统计。

> [English →](README.md)

## 概述

- 在网关主机运行，接受 HTTP/HTTPS 代理流量，将每个请求转发到上游代理
- **并行竞速 + 错峰启动**：每次请求按 EWMA 延迟排序候选，先发最优 1~2 个（RFC 8305 §5），间隔 ~250ms 补发；首个首字节成功即取消其余并释放连接。错峰大幅减少 CONNECT 隧道扇出与 HTTP 双写流量
- **域名缓存**：某个代理为某域名竞速胜出后，在 `cache_ttl` 有效期内复用该代理，避免每个请求都竞速
- **会话粘性**：可选，同一客户端 IP + 域名/目标复用同一代理（保持 egress IP 稳定），粘性代理失败/返回 5xx 即驱逐并回落竞速（redispatch），并按 `recheck_hits` 周期探路重竞速
- **HTTP 响应缓存**：幂等 `GET` 响应在内存中缓存（TTL 60s，遵循 `Cache-Control`）
- **本机竞速**：可选，让网关主机自身作为代理节点直接参与竞速（不走上游）
- **域名统计**：各域名胜出次数持久化到 SQLite，重启不丢失
- **Web 界面**：内置仪表盘 `/`，可浏览域名统计、默认代理、胜出次数，支持自动刷新；点击统计卡片可过滤出以该代理为 Default Proxy 的域名

## 功能

- HTTP 与 HTTPS（`CONNECT`）转发，**并行竞速多个上游代理**（按 EWMA 排序 + 错峰启动）
- 域名级缓存（`cache_ttl`），按域名复用胜出代理
- 会话粘性（per-client+domain，内存-only，滑动 TTL），粘性代理失败自动回落竞速并回填；5xx 驱逐、周期重竞速、容量上限
- CONNECT 上游 TCP 预热池（第一阶段，`router.conn_pool`）：为每上游维护少量空闲 TCP，CONNECT 跳过"本机→上游代理"建连；目标半预连接（第二阶段，`conn_pool.target_prewarm`）：命中域名缓存/粘性**或竞速胜出**的高频 CONNECT target 后台预建"到上游"的 TCP（每条 target 补 2 条、取走仍留 1 条备用），与第一阶段共享 fd 预算与空闲超时
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
- **选择**：`ProxySelector.ordered_proxies()` 返回随机打乱的已启用代理列表；前 `max_retries` 个先竞速，失败后再对剩余代理竞速兜底
- **域名缓存**：胜出后把代理记入 `domain_meta`，在 `cache_ttl` 有效期内对该域名复用
- **会话粘性**：同一客户端 IP + 域名/目标复用上次胜出的代理单发（跳过竞速），优先级高于域名缓存；粘性代理失败则驱逐该条目并回落竞速（redispatch），竞速赢家回填粘性表
- **统计**：`request_counts` 各代理胜出次数，`attempted_counts` 各代理尝试次数；`domain_stats` 各域名下各代理胜出次数（SQLite 持久化）

## API 端点

| 端点 | 说明 |
|------|------|
| `GET /` | Web 仪表盘（域名统计、默认代理、自动刷新；点击统计卡片可按 Default Proxy 过滤域名） |
| `GET /health` | 健康检查 |
| `GET /proxies` | 列出已配置代理 |
| `POST /proxies` | 添加代理（JSON body） |
| `GET /stats` | `request_counts` + `attempted_counts` |
| `GET /metrics` | `request_counts`、`attempted_counts`、域名统计与服务端性能计数器（缓存命中 / 竞速扇出） |
| `GET /server-stats` | 服务端资源采样（CPU 占用、事件循环延迟），由压测子进程填充；正常运行返回空快照 |
| `GET /config` | 路由配置（`enable_local_racing`） |
| `GET /domains` | 从 SQLite 读取的域名胜出统计 |
| `GET /domains/meta` | 各域名默认代理 + 最近更新时间（自适应 TTL 开启时含 `ttl`/`expires_at`/`switch_count`） |
| `GET /stickiness` | 会话粘性表（客户端IP\|域名 → 粘性代理 + 更新时间） |
| `GET /quality` | 各代理 EWMA 首字节延迟（秒），竞速排序依据 |
| `POST /quality/reset` | 清空全部代理 EWMA 质量（网络切换后调用） |
| `GET /circuit` | 各代理熔断 + 探活状态（`open`、退避、`probes_sent/ok/skipped`、`single_send_degrades`） |
| `GET /policies` | 策略路由配置快照（匹配条件 + 允许的代理子集） |
| `POST /circuit/reset` | 手动解除全部熔断（保留 EWMA 质量） |

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
logging:
  file: "auto_squid.log"
```

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
- **`lb_bias`** 控制在途积压对竞速排序的惩罚(`ewma × (1 + active)^bias`)。慢代理易被打爆就调高;最快代理被过度避让就调低。
- **策略路由**(`router.policies`)按域名/标签收窄竞速候选集,直接降低 TTFB 与 `racing.amplification`——形状见 `examples/config.yaml`。
- **`conn_pool.target_prewarm`**(第二阶段)需 `conn_pool.enabled` 为 true;命中域名缓存/粘性**或竞速胜出**(竞速是多数 CONNECT 流量的主体路径,不加则预热只服务极少数缓存命中请求)的高频 CONNECT target 后台预建"到上游"的 TCP,每条 target 补 2 条、取走仍留 1 条备用。`conn_pool.total` 是两阶段合并的全局 fd 预算。看 `/metrics` 的 `target_pool_hits` vs `target_pool_misses` 确认热 target 真的复用了预建连接。

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

测试套件覆盖 HTTP/CONNECT 转发、HTTP 响应缓存、域名缓存、本机竞速、`ProxyStore` CRUD、API，以及二进制安全的请求体处理。

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
