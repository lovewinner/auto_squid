# auto_squid 性能压测工具

`bench/` 提供一套**可控、可重复、可归因**的压测,用于准确评价 auto_squid 的性能。

## 文件

- `mock_upstream.py` — 受控上游代理集群。模拟真实 HTTP/HTTPS 代理(绝对 URL 请求 + CONNECT 回显隧道),延迟/响应大小/chunked/失败率由配置决定,排除真实网络抖动。每实例带命中计数器与**新建连接计数器**(后者供连接复用场景)。
- `server_proc.py` — **被测方子进程入口**。独立事件循环内启动 Router(+ mock 集群)+ 管理 API(uvicorn,复用 `auto_squid.api:app`),经 stdout 打印 `READY` 握手,捕获 SIGTERM 优雅关闭。进程隔离的核心。
- `stress.py` — 压测主驱动(纯客户端)。启动 `server_proc` 子进程 → 读 READY 握手 → 按模式跑负载 → 跨进程拉 `/metrics` `/server-stats` → 输出终端表格 + 结构化 JSON。

## 进程隔离(本版核心)

被测 Router(+ mock 上游)跑在**独立子进程**,有自己的事件循环;压测客户端留在主进程,经 `127.0.0.1` 回环打过去。这从根上消除了旧版"客户端与服务端同进程同事件循环争抢"导致的吞吐/延迟测量失真——客户端的 socket 读写与 task 调度开销不再计入被测方。

服务端性能计数器(缓存命中/竞速扇出/CPU/事件循环延迟)经子进程的管理 API(`/metrics` `/server-stats`)跨进程拉取,在 mock 与 real **两种模式统一**计算缓存命中率与竞速放大率(real 模式不再记 N/A)。

## 多轮取均值(`--rounds N`)

单次结果易受环境扰动(CPU 争抢、网络抖动等)。`--rounds N`(默认 3)让同一条件跑 N 轮,报告给均值±标准差,规避单次噪声:

- **每轮全新子进程**:独立的 `server_proc`、独立的 tempfile SQLite DB、全新的 Router 缓存与计数、全新的 mock 上游实例。
- **条件完全一致**:`mock_specs`(mock 集群规格)在循环外只算一次,每轮相同;唯一的跨轮差异是进程启动时机与机器负载——即**纯环境噪声**。
- **报告结构**:`rounds`(轮数)、`round_results`(每轮完整报告列表)、`aggregates`(每项指标的 min/max/mean/stddev);顶层 `scenarios` 为跨轮**均值视图**,schema 与单轮报告完全一致,旧式跨版本 diff 不受影响。
- **`--rounds 1`** 时输出与旧版**逐字节兼容**(无 `round_results`/`aggregates`)。

```bash
python -m bench.stress --quick --rounds 3        # 快速:3 轮,看方差
python -m bench.stress --mode all --rounds 5     # 全模式 × 5 轮
python -m bench.stress --rounds 2 --profile      # profile 只覆盖第 1 轮
```

## 快速开始

```bash
# 默认:受控 mock 上游,并发阶梯(测饱和点)
python -m bench.stress

# 快速冒烟(~10s,小规模)
python -m bench.stress --quick

# 禁用 HTTP 响应缓存,测纯路由性能(隔离缓存层)
python -m bench.stress --no-http-cache

# 四种模式全跑
python -m bench.stress --mode all

# 用真实上游代理(需 proxies.yaml 可达)
python -m bench.stress --upstream real --proxies proxies.yaml

# 连接复用率(测 keepalive,仅 mock)
python -m bench.stress --mode conn-reuse

# 开环 soak(不限速,测真实容量上限)
python -m bench.stress --mode soak --open-loop --duration 30

# cProfile 覆盖(仅客户端进程,输出 bench_profile.txt)
python -m bench.stress --profile

# 同一条件跑 3 轮(默认),每轮全新子进程/SQLite/缓存,取均值去环境噪声
python -m bench.stress --rounds 3
python -m bench.stress --rounds 5 --mode all
```

## 压测模式

| 模式 | 负载形态 | 测什么 |
|------|---------|--------|
| `staircase` | 并发数 1→200 阶梯,每级固定请求数 + 预热 | 吞吐/延迟随并发的变化,**找饱和点** |
| `rate` | 目标 RPS 100→2000 阶梯,持续发 | 延迟/错误率随负载的变化,**找容量上限** |
| `mixed` | 30%热+20%大响应+20%chunked+20%冷+10%CONNECT + 预热 | 贴近真实流量的**混合画像** |
| `soak` | 固定速率长时持续(默认 60s) | **稳定性与资源泄漏** |
| `soak --open-loop` | 固定并发不限速 | **真实容量上限**(区分主动限速 vs 被动撑不住) |
| `conn-reuse` | 固定小并发长时同域名 | **keepalive 连接复用率**(仅 mock) |
| `all` | 依次跑 staircase/rate/mixed/soak | 全面评价(不含 conn-reuse) |

每个场景含**预热**(warmup):正式统计前发一批请求填缓存,冷启动不混入稳态指标。`requests.warmup` 记录预热数。

## 关键指标(分组报告)

报告 JSON 按**分组**组织,各场景含:

- **requests**: `client` / `injected` / `warmup` / `success` / `errors`。`injected` vs `completed` 差 = 被丢弃/超时(soak 据此区分主动限速与被动撑不住)。
- **throughput**: `completed_rps`(完成)、`injected_rps`(注入)。
- **latency**: TTFB 与 total 的 P50/P95/P99/mean(客户端 raw socket 精确到状态行)。
- **errors**: 分类(conn / timeout / `http:<状态码>` / echo-mismatch)+ 错误率。
- **status_distribution**: 所有结果(含成功)的状态码分布。real 模式下揭示"成功"背后的真实状态码(如全是 503 = 上游 DNS 失败)。
- **cache**: `http_hit_rate` / `domain_hit_rate` + `http_hits` / `http_misses` / `http_cache_entries_end`。**服务端计数器,两种模式通用**(real 不再 N/A)。
- **racing**: `amplification`(上游扇出/客户端请求)、`upstream_attempts`、`invocations`。
- **resources**: 客户端 RSS/fd/连接池末值 + **服务端 CPU% 与事件循环延迟**(`server_loop_lag_ms` 的 p95/max,反映 Router 是否被同步操作卡住)。
- **correctness**: mock 模式响应体校验(body 大小/内容,缓存命中字节一致);real 模式 `checked: false`。
- **attribution**: `upstream_throttled`(是否 429/503)、`bottleneck`(`proxy` / `upstream`)——区分代理瓶颈与上游瓶颈。

> 计数器跨进程拉取若失败,`cache`/`racing` 组记 `null` 并标 `counter_fetch_failed: true`,不阻断压测。

## 真实上游模式(`--upstream real`)

指向 `proxies.yaml` 里的真实上游代理,贴近生产。与 mock 模式有几处关键差异:

- **主机名**:真实代理会真正解析主机名,故压测打向**内置默认大站池**(www.baidu.com 等,可被 `--real-hosts host1,host2,...` 覆盖)。
- **成功判定**:真实站点对压测路径(`/p0` 等)常返回 3xx/4xx,但代理已成功转发,源站状态码与代理性能无关。故 real 模式"收到任何 HTTP 响应"都记成功(仅 conn/timeout 算真失败)。**务必看状态码分布**。
- **CONNECT**:真实 TLS 隧道加密 payload,无法回显校验,故 real 模式"建隧道即成功"。
- **缓存指标**:经服务端计数器测,两种模式统一(real 也能测缓存命中率/放大率)。
- **上游限流**:real 模式可能触发源站 429(压测太快把真实站点打限流)。看 `attribution.upstream_throttled` 与 `status_distribution` 区分"代理瓶颈"与"上游触顶"。
- **超时**:real 模式客户端超时上调到 20s。

```bash
python -m bench.stress --upstream real --mode all --duration 120
python -m bench.stress --upstream real --real-hosts www.baidu.com,www.qq.com
```

## 隔离缓存层

HTTP 响应缓存会掩盖路由路径的真实性能(缓存命中后 TTFB 极低,测的是缓存而非代理)。对照跑法:

- `python -m bench.stress` —— 完整路径(含 HTTP + 域名缓存),测生产体感。
- `python -m bench.stress --no-http-cache` —— 禁用 HTTP 响应缓存,**测纯路由性能**(域名缓存仍生效,可单独观察 racing + 连接池)。

## 可比性

- 同一 mock 配置 + 同一 Router 代码,多次跑结果可重复(延迟确定性高)。
- JSON 报告带 git 版本,跨提交/跨优化可 diff。**注:本版报告结构已重新分组**(requests/throughput/latency/cache/racing/resources/correctness/attribution),与旧版 `bench_report_*.json` 不兼容,需用同结构报告对比。
- **多轮报告**:`--rounds N`(N>1)新增顶层 `rounds`/`round_results`/`aggregates`,`scenarios` 仍是跨轮均值视图(字段类型与单轮一致);`--rounds 1` 报告与旧版 schema 完全一致。

## 输出示例

```
■ 场景: mixed
  请求          : 客户端 2000 (成功 2000, 失败 0, 注入 2000, 预热 50)
  状态码分布    : {'200': 200, '404': 800, '302': 800, '301': 200}
  吞吐          : 完成 540.0 req/s  注入 540.0 req/s
  TTFB (ms)     : P50=37  P95=250  P99=460  mean=78
  缓存          : HTTP命中 100.0%  域名命中 0.0%  (hits=270 misses=0 条目末值=28)
  竞速          : 放大率 0.10x  上游尝试 30  竞速触发 0
  资源          : RSS=40MB  fd=9  池末值=2  服务端CPU=14%  loop-lag p95=0.50ms max=0.50ms
  正确性        : ✓ 校验 300 通过 / 0 失败
  归因          : 上游触顶=False  瓶颈=proxy
```

多轮(`--rounds 3`)时每个场景先打每轮紧凑表 + 均值±标准差,再打 round 0 详细块:

```
■ 场景: staircase
  轮次  完成rps  注入rps  TTFB p50   p95      p99    err%    缓存%
  [1/3]    151     151    8.5   214.1   342.8   0.00    89.2
  [2/3]    148     148    8.9   220.0   350.1   0.00    88.9
  [3/3]    153     153    8.1   209.9   340.2   0.00    89.5
  均值          150.7±2.5 req/s  TTFB p50 8.5±0.4  p95 214.7±5.1  p99 344.4±5.0  err 0.00%  缓存 89.2%±0.3
  请求          : 客户端 200 (成功 200, 失败 0, 注入 200, 预热 40)
  ...
```
