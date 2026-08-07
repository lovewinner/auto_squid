# auto_squid 代码分析与优化方案

> 分析日期：2026-08-07
> 分析对象：当前 master 分支（HEAD `c982e0c`）全部核心代码
> 目标：多 HTTP 代理上网 —— 通过多个上游代理，为不同资源选择合适代理，优化上网体验（降低延迟/失败率、提升稳定性）。

---

## 〇、一句话定位

auto_squid 是一个**正向 HTTP/HTTPS 代理**：监听一个端口，客户端（浏览器/应用）把流量交给它，它在一组上游 Squid 代理上做**并行竞速**（Happy Eyeballs 思路），谁先成功就用谁；并对"域名 → 最优代理"做缓存复用，兼顾"快"与"稳"。

**核心价值主张**：多个上游代理的可用性、速度、地区分布各不相同。竞速（race）能在**无需任何先验质量模型**的情况下自动选中最快的可用代理；域名缓存 + 会话粘性把竞速结果沉淀为"稳态最优路径"。

---

## 一、当前版本代码架构

### 1.1 模块划分（核心 2418 行）

| 模块 | 行数 | 职责 |
|---|---|---|
| `auto_squid/router.py` | 1572 | 全部转发引擎：竞速、域名/响应/粘性三级缓存、连接池、统计持久化 |
| `auto_squid/api.py` | 476 | FastAPI 管理 API + 单页 Web 仪表盘（含内联 HTML/JS） |
| `auto_squid/config_schema.py` | 109 | pydantic 配置模型（listen/api/router/logging + ProxyInfo） |
| `auto_squid/cli.py` | 127 | Typer 入口：加载配置 → 起 Router + uvicorn API → 优雅关闭 |
| `auto_squid/auth.py` | 66 | 客户端 HTTP Basic 认证 |
| `auto_squid/proxy_store.py` | 66 | 上游代理注册表（YAML 加载/保存 + 内存 CRUD） |

辅助：`bench/`（1824 行压测工具链）、`tests/test_end_to_end.py`（1755 行、38 用例）。

### 1.2 请求处理流程

```
客户端 ──HTTP/S──> :10808 handle_client
                      │  解析首行+头 → 可选认证(407) → 按 CONNECT/HTTP 分流
                      ▼
              _handle_http_request / _handle_connect
                      │
        ┌─────────────┴──────────────────────────────┐
        ▼                                            ▼
   HTTP: 响应缓存 → 会话粘性 → 域名缓存 → 竞速     CONNECT: 会话粘性 → 域名缓存 → 竞速
        │                                            │
        ▼                                            ▼
   _forward_single（流式转发+边收边缓冲缓存）      _relay_tunnel（双向裸管道透传）
        │
        ▼
   胜者回写 _record_win_meta（域名缓存）+ _record_sticky（粘性表）
```

### 1.3 三级缓存决策树（HTTP 路径）

| 优先级 | 机制 | 键 | 命中动作 | TTL |
|---|---|---|---|---|
| 1 | **HTTP 响应缓存** | `GET:<url>` | 整包回写（完全不经上游） | 60s（尊重 Cache-Control） |
| 1.5 | 在途 GET 去重聚合 | `GET:<url>` | 并发同 URL 请求 await 首请求结果（防 cache stampede） | 0.1s 等待超时 |
| 2 | **会话粘性** | `client_ip\|domain` | 该代理单发（不竞速），egress IP 稳定 | 1800s 滑动（命中刷新） |
| 3 | **域名缓存** | `domain` | 该代理单发 | 600s |
| 4 | 竞速 | — | 首批 `max_retries` 个并行，首字节判胜 | 每次请求 |

### 1.4 竞速机制（核心算法）

- **并行扇出**：`ProxySelector.ordered_proxies()` 返回**随机打乱**的 enabled 代理列表，取前 `max_retries` 个并行发请求（HTTP 用 httpx 流式，CONNECT 用裸 socket 隧道）。
- **首字节判胜**：HTTP 响应头到达即判胜，body 流式转发；败者取消（其流式 resp 后台 aclose，`_drain_losers` 不阻塞赢家 TTFB）。
- **兜底批**：首批全失败则对剩余代理再竞速。
- **赢家沉淀**：胜者 `_record_win_meta` 写域名缓存 + `_record_sticky` 写粘性表。

### 1.5 质量观测现状

**没有任何主动健康探测 / 质量模型 / 失败惩罚**：

- `ordered_proxies()` 均匀随机 shuffle，**不考虑**任何历史延迟、失败率、权重。
- 代理死了 = 每次冷请求都白白参与竞速（占竞速槽、拖长失败路径），直到某次请求恰好失败。
- 无 EWMA 延迟跟踪、无熔断器、无 in-flight 感知、无错峰启动。
- 唯一的"再评估"是粘性表的 `recheck_hits`（命中 N 次触发重竞速）——本质是**反应式**的，且只对粘性路径生效。

**已有但可迁移的经验**：`_flush_loop` 后台周期任务模式（每 5s 落盘 + 清理）可复用为探活/质量更新的后台循环。

---

## 二、可参考的技术/算法（带出处）

> 均为 2026-08 复核的一手文献；详见文末链接。

### 2.1 Happy Eyeballs 竞速排序与错峰（RFC 8305）

**当前**：auto_squid 同时全发，顺序随机。
**RFC 8305 的做法**：
- §4 排序：有状态客户端应维护每个候选的历史 RTT，**按 RTT 从低到高排序**后再竞速 → 先发的更可能先赢，减少扇出。
- §4 关键约束：**历史 RTT 数据不可跨网络接口/网络切换沿用**，换网络应清空。
- §5 错峰：连接尝试**不应同时发起**。默认连接尝试延迟 **250ms**，下限 **100ms**（绝对值下限 10ms，防丢包率高时拥塞崩溃），上限 **2s**；有历史 RTT 时可动态取"上次尝试 SYN 重传时点"。

**适配**：竞速首批从"前 max_retries 个随机代理同时发"改为"按 EWMA 排序后，先发最优 1~2 个，间隔 ~50-250ms 补发下一个"。

### 2.2 EWMA / peak-EWMA 质量模型（Finagle/Linkerd 实验）

**实验结论**（11 后端、单节点人为变慢到 2s 30 秒）：
| 算法 | 能扛住的尾延迟分位 | 1s 超时换算成功率 |
|---|---|---|
| round-robin（≈当前 auto_squid） | ~95 分位 | ~95% |
| least-loaded（在途最少） | ~99 分位 | ~99% |
| **peak-EWMA**（RTT 移动平均 × 在途数） | **~99.9 分位** | **~99.9%** |

结论：**"信息用的越多，尾延迟越短"**。EWMA 公式为对每次成功 RTT 做指数滑动平均，可加在途数加权（峰值 EWMA = 最近 RTT × 在途请求数，兼顾"快"与"不忙"）。

### 2.3 P2C（Power of Two Choices）+ 加权 least-request（Envoy/Dubbo）

- **P2C**：每次从候选集随机抽 2 个，选在途请求数更少的那个。复杂度 O(1)，最大失衡从 O(log n) 降到 O(log log n)，抗"羊群效应"（herding）。
- **加权 least-request**（Envoy `LeastRequest`）：权重按负载动态调，
  `有效权重 = 静态权重 / (在途数 + 1)^bias`（bias 默认 1.0）。在途多的代理权重被压低。
- **Dubbo**：默认就是加权随机，官方文档明确点名缺陷——**慢代理会堆积请求**（打到慢节点就卡住，久而久之全卡上去）。LeastActive / ShortestResponse（最近 30s 滑动窗平均响应时间）是改进。

**适配**：竞速选批时避开 in-flight 积压多的代理，保护慢代理不被打爆。

### 2.4 熔断 / 异常点检测 / 慢启动（Envoy outlier / Hystrix）

- **熔断器**：连续失败 N 次 → 打开电路，退避期内不参与竞速；后台探活恢复后放回（指数退避探测）。
- **outlier detection**：consecutive_5xx / success_rate / failure_percentage 等维度触发剔除。
- **slow-start**：新加入或熔断恢复的代理从低权重开始，随成功次数在窗口内爬升到满权重（Envoy 默认 60s 窗口）。防止新成员被首批流量打懵→误判劣质→又被熔断的恶性循环。

### 2.5 自适应并发限制（Netflix / Envoy 潮流）

- 不设固定超时，而是**动态调整每个上游允许的在途请求数**（加性增加、乘性减少，基于延迟梯度），保护慢代理同时最大化吞吐。Envoy 有自适应并发限制过滤器。

### 2.6 一致性哈希（Envoy ring hash / Maglev）

- 若希望"同一资源稳定映射到同一代理"（跨重启/跨进程稳定），用一致性哈希。**Maglev** 查表/建表比 Ketama 快约 5-10 倍（Envoy 基准），但增删节点扰动约 2 倍；表大小可调。
- 注意：与"追求最快"目标有张力，通常仅在需要 egress 稳定性时用。

### 2.7 域名/资源 → 代理子集的策略路由（PBR / SD-WAN rule）

- 若代理有地域差异（如部分代理在国内、部分在国外），可按**目标特征**收窄候选集：`域名模式 → 代理子集`，竞速只在该子集内进行。
- 例：国内域名只用国内代理（快且合规），被墙域名用海外代理，流媒体用特定出口。这与 SD-WAN 的 PBR / App-Aware Routing 同构。

---

## 三、现状诊断（与业界做法对照）

| 维度 | auto_squid 现状 | 业界最优做法 | 差距 |
|---|---|---|---|
| 选路 | 均匀随机竞速 | 按 EWMA 延迟/权重排序竞速 | 中（P1 可解） |
| 竞速扇出 | 首批同时全发 | 错峰启动（250ms 默认） | 中（省扇出/带宽） |
| 死代理 | 每请求仍参与竞速 | 熔断器 + 探活恢复 + slow-start | 高（拖长失败路径） |
| 负载均衡 | 无 in-flight 感知 | P2C / least-active / 自适应并发 | 高（慢代理被并发打爆） |
| 域名策略 | 全局单一域名缓存 | 按域名特征 → 代理子集（PBR） | 高（收益取决于代理地域分布） |
| 再评估 | 粘性 recheck_hits（反应式） | 主动周期探活（SD-WAN BFD/SLA） | 中 |
| egress 稳定 | 会话粘性（滑动 TTL） | 会话粘性 / 一致性哈希 | 低（已实现） |

---

## 四、优化方案（按价值/成本排序）

### P1 —— 低成本、高价值，建议直接做

**1. 每代理 EWMA 延迟跟踪 + 竞速排序** — ✅ **已落地（2026-08-07）**
- `ProxySelector` 加 `_quality: {pid: {ewma_ttfb: float}}`，`record_ttfb()` 更新（无历史取当前值，有历史 `ewma = (1-0.3)*old + 0.3*new`）。
- `_try_http`/`_try_tunnel` 用 `time.perf_counter()` 记录首字节耗时，成功后 `record_ttfb`（被竞速取消、未完成首字节的请求不记录，避免污染质量模型）。
- `ordered_proxies()` 按 EWMA 排序（快速者靠前；无观测的"未知质量"代理排后）；同段随机打乱均衡负载。
- **清空**：`reset_quality()`（网络切换/代理分组变化时调用，RFC 8305 §4）；经管理 API `POST /quality/reset` 暴露，`GET /quality` 可查看。
- 运行时验证：快代理（0.01s）EWMA≈0.057s，慢代理（0.30s 单独时）EWMA≈0.329s，reset 清空生效（bench mock 集群端到端）。

**2. 错峰启动（staggered start）** — ✅ **已落地（2026-08-07）**
- 竞速首批改为：先发最优 `stagger_initial` 个（默认 1，冷启动自动翻倍到 2），间隔 `stagger_interval_ms`（默认 250ms，钳制到 RFC 8305 [100, 2000]）补发下一个，首字节成功即取消其余。
- **实现**：`_race_staggered` 按 interval **定时补发**（RFC 8305 §5 关键——补发不等待上一候选失败，慢代理挂起时后发者仍能顶上）；候选**惰性创建**（真 task 只在补发时经 `_make_race_task` 创建，未发候选不启动）；败者清理复用 `_spawn_cleanup`/`_drain_losers`（软上限 + 后台排空）。非错峰路径 `_race` 保留作对比。
- **HTTP 5xx 不算胜出**：`_is_acceptable_win` 过滤——上游 5xx 不作竞速赢家（否则错峰首批单发时，坏的先应答即胜，吞掉好代理），继续补发/兜底找 200；单发路径（粘性/域名缓存）仍原样透传 5xx 由调用方驱逐。
- **配置贯通**：`config.yaml` + `config_schema`（`stagger_start/stagger_initial/stagger_interval_ms`）+ `cli.py` + `bench`（`--no-stagger` 对比开关）。
- **验证**（mock 2 台：快 10ms / 慢 300ms，每请求冷域名强制竞速，EWMA 已学习）：

  | 指标 | OFF 全发 | ON 错峰 |
  |---|---|---|
  | 每请求扇出 amplification | 2.0 | **1.0** |
  | upstream_attempts（40 req） | 80 | **40** |
  | 慢代理被打中次数 | 40 | **0** |
  | TTFB p50（HTTP） | 14.4ms | **13.7ms** |
  | CONNECT 慢隧道被打中 | 40 | **0** |
  | 正确性 | 40/40 FAST | 40/40 FAST |

  结论：扇出减半、慢代理根本不发（CONNECT 败者隧道"建好再关"的成本归零）、TTFB 不劣化。冷启动（无 EWMA）自动翻倍到 2，等价旧 `_race` 兜底能力。

**3. 全局熔断器 + 指数退避探活 + slow-start** — ✅ **已落地（2026-08-07）**
- **连续失败熔断**：`ProxySelector` 维护每代理 `consec_fail`。连续失败达 `circuit_threshold`(默认 3) → `circuit_open_until` 指数退避（1s→2s→4s，上限 `circuit_max_backoff` 300s），退避期内 `ordered_proxies()` **剔除**该代理、域名缓存/粘性单发路径也跳过（`_get_fresh_proxy`/`_get_sticky_proxy` 检查熔断），避免对已确认故障的代理持续单发。
- **失败信号同源**：真实请求失败（`_try_http`/`_try_tunnel` 连接失败/超时/5xx）与后台探活失败**共享同一连续失败计数**——不再重复实现。关键：**被竞速取消（CancelledError）不算失败**，健康慢代理每次竞速都会被快代理抢先取消，若计入会误熔断。收到响应头（HTTP）/CONNECT 200（隧道）即记成功、归零计数。
- **后台探活**（仿 `_flush_loop`）：每 `probe_interval_sec`（默认 30s，0=关闭）对 enabled 代理做轻量 CONNECT 到 canary（默认 `1.1.1.1:443`）+关闭，成功记 EWMA + 归零、失败累计连续失败（达阈值即熔断）。探活只在低流量期补全质量模型，域名级最终仍由竞速决定。
- **slow-start**：退避到期 → `started_at=now` → slow-start 恢复期（默认 60s 窗口），排序**垫底**（`_slow_start_rank` 档位 1），累计 `slow_start_success`(默认 3) 次成功即恢复完整权重，防"熔断恢复的代理一上来就被单发/首批抢打"。
- **可观测**：`/circuit` 返回每代理熔断状态（open/退避剩余/连续失败/slow-start 中）+ `probes_sent/ok` + `circuit_open_count`；`/metrics` counters 含 `circuit_state`；`POST /circuit/reset` 手动解熔断（不动 EWMA）。
- **配置贯通**：`config.yaml` `router.circuit` 块 + `config_schema.CircuitConfig` + `cli.py` + bench（`--probe-interval-sec` 等，压测默认关探活隔离该层）。
- **验证**（真实 socket 端到端 + 单测 68 全绿）：
  - 坏代理 `down`(端口无人监听) + 好代理 `up`：请求 1-2 `down` 失败累计 → 第 3 次起 `down` 不再被尝试（`attempted_counts={'up': N, 'down': 2}` 恒定）、请求全部 200；
  - 退避指数翻倍：首熔断 2s → 二次 4s → 三次 8s（`/circuit` 实读 `backoff` 字段）；
  - slow-start：退避到期 `slow_start=true` 垫底，成功 2 次后恢复首位；
  - 探活：`probe_interval_sec=3` 实跑，`up` 成功喂 EWMA（~0.96ms）、`down` 失败累计熔断（backoff 8s）。

### P2 —— 明显增益，值得纳入

**4. in-flight 计数 + P2C/least-active 选批** — ✅ **已落地（2026-08-07）**
- **在途计数**：`ProxySelector` 维护每代理 `_in_flight`。`_try_http`/`_try_tunnel` 在"发起→收到响应头/CONNECT 200/失败/被取消"的整个尝试生命周期 `_inflight_start`/`_inflight_finish`（finally 保证取消也释放，防计数泄漏）；`max_in_flight` 记录单代理在途高水位。
- **加权 least-request 选批**：`ordered_proxies()` 排序权重 = `ewma × (1 + active)^lb_bias`（默认 bias=1.0）——快而空闲的代理靠前，背上在途积压的代理即使延迟历史最快也被压低排序，竞速首批/补发天然避开积压代理，保护慢代理不被打爆（Envoy LeastRequest `weight/(active+1)^bias` 的对偶，即分析 2.2 的 peak-EWMA）。slow-start 垫底档不变；未知质量代理仍靠未知标记垫底；`bias=0` 退化为纯 EWMA 排序。
- **P2C 已在既有排序层覆盖**：分批候选来自 EWMA+least-active 排序后的有序列表，首批取最优、补发按序——比随机抽 2 的 P2C 更确定，同权重段随机打乱保留抗羊群。
- **可观测**：`/metrics` counters 含 `proxy_in_flight`（当前在途快照）+ `max_in_flight`（高水位）；bench 报告 `in_flight` 组（单代理在途峰值 + 末态在途，验证计数归零无泄漏）。
- **配置贯通**：`config.yaml` `router.circuit.lb_bias` + `config_schema` + `cli.py` + bench `--lb-bias`。
- **验证**（mock 2 台：快 0ms / 慢 200ms，100 并发全竞速端到端）：
  - 单测 74 全绿（含 6 个 in-flight 定向测试：积压挤位、释放恢复、bias=0 纯 EWMA、未知垫底、reset 清空、真实请求生命周期）；
  - 100/100 请求成功，`max_in_flight=100`（真实竞速双代理同时积压），请求结束后 `in_flight` 归零（**无泄漏**）；
  - 排序单元验证：fast 背 5 个在途时权重 0.02×6=0.12 > slow 0.05 → slow 排前；释放后 fast 回首位。

**5. 域名 → 代理子集策略路由（地域感知）**
- 配置 `域名规则 → 代理子集`，竞速只在该子集内进行。
- 收益取决于代理地域分布；若代理全在国内/全在国外则意义有限。

**6. 引入质量模型/权重到单发选择** — ✅ **已落地（2026-08-08）**
- **单发降级**：域名缓存/粘性命中单发时，若被钉住代理"最近失败率上升（连续失败）"或"EWMA 恶化（相对钉住时基线）"，主动降级回竞速——把确定性探路从 `recheck_hits` 的纯命中计数升级为 **EWMA 感知的"不稳定即重竞速"**（与 Envoy outlier 连续失败剔除 + 基线比对的思路同源）。
- **两条独立信号**（`Router._single_send_degraded`，与熔断解耦——熔断是"连续失败达阈值直接剔除"，降级是"尚未熔断但已开始变差，别再确定性单发，交给竞速选路"）：
  1. **连续失败**：`consec_fail ≥ single_send_degrade_fail`（默认 2，熔断阈值 3 的早告警）。被钉住代理最近在真实请求/探活中连续失败 → 单发命中它只会放大失败路径。
  2. **EWMA 恶化**：当前 EWMA ≥ 钉住时基线 × `single_send_degrade_ratio`（默认 3.0，且绝对差 > `single_send_degrade_slack_ms` 默认 10ms 防极低延迟误判）。obs≥2 才触发（单次观测无趋势可言）。
- **基线捕获**：`_record_win_meta` / `_record_sticky` 在钉住时刻捕获 `ref_ewma`；粘性命中（`_bump_sticky`）只滑动 TTL **不刷新基线**，保证"恶化"是相对钉住时的初始状态。
- **降级集合（可观测）**：降级命中 → 代理记入 `_degraded_single_send`（供 `/metrics` `/circuit` 展示）；真正的门控是每次选择时**实时重估** `_single_send_degraded`（代理恢复后立即重新可单发，无冷却）。新赢家经 `_record_win_meta` 清除标记，`reset_proxy_quality` 一并清空。
- **可观测**：`/metrics` counters 含 `single_send_degrades`；`/circuit` 含 `degraded_single_send` 集合；`/quality`、`/stickiness`、`/domain-meta` 均含 `ref_ewma`。
- **配置贯通**：`config.yaml` `router.circuit.single_send_degrade_{fail,ratio,slack_ms}` + `config_schema` + `cli.py` + bench `--single-send-degrade-*`。DB `domain_meta` 加 `ref_ewma` 列（PRAGMA 迁移，老库自动补列）。
- **验证**（单测 82 全绿，含 8 个单发降级定向测试；真实 socket 端到端）：
  - 单元：连续失败达阈值 → 域名缓存/粘性都降级 None（未熔断时）；EWMA ratio 恶化 → 降级；slack 防误判；obs=1 不触发；粘性命中不刷新基线；reset 清空；降级标记清除/重钉；
  - 端到端：fast 首次竞速胜出钉住 → 连续失败 2 次后单发降级、真实客户端请求由竞速换到 slow 应答（200）；fast 上游真实宕机后竞速也绕开（熔断器兜底）；恢复后重钉清除标记。

### P3 —— 视目标决定

**7. 切换阻尼（dampening）**
- 新代理须连续赢若干次才替换域名赢家，防 flapping。当前域名缓存 TTL 内固定，天然有阻尼，可留待观察。

**8. 一致性哈希稳定映射**
- 仅当希望跨重启/跨进程 egress 映射稳定时用；与"追求最快"略有张力。

**9. 自适应并发限制**
- 动态调整每代理在途上限（Netflix 思路），在无固定超时下保护慢代理。实现复杂，收益在中高负载场景。

**10. 置信度驱动的竞速 vs 单发**
- UCB/ε-greedy 形式化 `recheck_hits`：对不确定域多试、对稳定域少试。HTTP 败者首字节即取消，竞速边际成本本就不高，收益有限。

---

## 五、落地草案（伪代码）

```
Router.__init__ 增加:
  proxy_quality: {pid: {
    ewma_ttfb,        # 成功请求的 EWMA 首字节延迟（可选 × 在途 = peak EWMA）
    consecutive_fail, # 连续失败计数 → 熔断触发
    in_flight,        # 当前在途请求数（least-loaded/P2C 选批用）
    probed_at,        # 上次探活结果
    started_at,       # 加入/熔断恢复时刻 → slow-start 计算爬升权重
    circuit_until}}   # 指数退避开闸时间

  后台探活 task（仿 _flush_loop）: 每 PROBE_INTERVAL 对 enabled 代理做轻量
  CONNECT 到 canary + 关闭, 计延迟/成败 → 更新 quality;
  连续失败 → 熔断（指数退避）; 恢复后 started_at=now → slow-start 低权重爬升

ProxySelector.ordered_proxies() 改为:
  1) 过滤 circuit_until 未到的熔断代理
  2) 按 slow-start 权重 × EWMA 排序/加权随机; in_flight 参与选批（P2C/least-active）
  3) 网络/代理分组切换 → 重置全部 EWMA/stats（RFC 8305 §4）

_build_racing_tasks_* 改为:
  取前 max_retries 个质量最优, 错峰启动（默认 250ms·下限 100ms/10ms·上限 2s）
  首个首字节成功 → 取消其余

每次 _try_http/_try_tunnel 成败更新 quality:
  成功 → EWMA 更新、consecutive_fail 归零、slow-start 推进、in_flight-1
  失败 → consecutive_fail+1（达阈值熔断）、in_flight-1

配置新增（config_schema）:
  router.quality:
    probe_interval: 30        # 秒; 0=关闭主动探活
    circuit_threshold: 3      # 连续失败熔断阈值
    circuit_max_backoff: 300  # 指数退避上限（秒）
    slow_start_window: 60     # slow-start 爬升窗口（秒）

  错峰（已落地,见 P1-2）:
  router.stagger_start: true        # 启用错峰启动
  router.stagger_initial: 1         # 首批并发数(冷启动自动翻倍到 2)
  router.stagger_interval_ms: 250   # 启动间隔(毫秒),钳制到 [100, 2000]

  单发降级（已落地,见 P2-6）:
  router.circuit.single_send_degrade_fail: 2     # 连续失败阈值(熔断早告警);0=关闭
  router.circuit.single_send_degrade_ratio: 3.0  # EWMA 相对钉住基线恶化倍数;0=关闭
  router.circuit.single_send_degrade_slack_ms: 10  # 降级绝对下限(ms),防极低延迟误判
```

**边界与成本**：
- 探活消耗上游流量/配额 → 间隔可配、可关闭。
- canary 延迟 ≠ 目标站延迟 → 探活只用于**存活 + 粗排序**，域名级最终仍由竞速决定。
- EWMA 不可跨网络沿用（RFC 8305 §4），换网络清空重学。
- 竞速本身已有"自适应"特性：即使排序/权重不准，首字节判胜仍兜底选中最快可用者。所有优化都是**减少扇出、缩短失败路径、提升 P99**，而非改变正确性。

---

## 六、参考链接

- RFC 8305 Happy Eyeballs v2（§4 RTT 排序/换网络清空、§5 错峰 250ms·100ms·2s）: https://www.rfc-editor.org/rfc/rfc8305.html
- Envoy Load Balancers（P2C/加权 least-request/slow-start/outlier）: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancers
- Envoy slow-start 配置（60s 窗口）: https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/load_balancing_policies/least_request/v3/least_request.proto
- Linkerd/Finagle「Beyond Round Robin」（RR vs least-loaded vs peak-EWMA 实验，95/99/99.9 分位）: https://linkerd.io/2016/03/16/beyond-round-robin-load-balancing-for-latency/
- Dubbo 负载均衡（加权随机慢代理堆积缺陷、LeastActive、ShortestResponse 30s 窗、P2C、Adaptive）: https://dubbo.apache.org/en/overview/what/core-features/load-balance/
- Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing"（2001）: https://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf
- Google Maglev 论文（一致性哈希）: https://research.google/pubs/maglev-a-fast-and-reliable-software-network-load-balancer/
- 姊妹篇《出口路由器负载均衡策略调研》分析文档: docs/出口路由器负载均衡调研.md
