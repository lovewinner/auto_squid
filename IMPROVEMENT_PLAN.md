# auto_squid 代理响应速度评估改进方案/补丁计划

> 配套诊断脚本: `test_routing.py`
> 相关文件: `auto_squid/router.py`, `auto_squid/selector.py`

## 一、当前实现分析

### 1. 竞速胜出判定机制 (`router.py`)
- 核心: `asyncio.wait(tasks, return_when=FIRST_COMPLETED)` → **最先返回响应头的代理胜出**
- `_is_acceptable_win()` 过滤掉 HTTP 5xx（不算胜出）
- 败者即时取消、清理下放后台（不阻塞赢家 TTFB）
- 胜出后 `_record_win_meta(domain, pid)` 写入域名缓存（key 为 `host:443` 如 `github.com:443`）

### 2. 质量指标体系 (`selector.py`)

| 指标 | 存储位置 | 更新时机 | 用途 |
|------|----------|----------|------|
| **全局 EWMA TTFB** | `_quality[pid]["ewma_ttfb"]` | 每次成功请求 `record_ttfb()` | 排序基准 |
| **域名级 EWMA TTFB** | `_domain_quality[domain][pid]["ewma_ttfb"]` | 同上，带 domain 参数 | 域名级排序优先 |
| **观测次数 obs** | `_quality[pid]["obs"]` / 域名级同构 | 每次成功 +1 | 统计可信度、单发降级基线 |
| **连续失败 consec_fail** | `_circuit[pid]["consec_fail"]` | `record_failure()` | 熔断触发、失败惩罚权重 |
| **在途数 in_flight** | `_in_flight[pid]` | 发起/结束请求时增减 | least-active 惩罚 |
| **并发上限** | `_conc[pid]["limit"]` | 成功加性增/失败乘性降 | 防慢代理被堆死 |

### 3. 排序权重函数 (`_weighted_rank`, `_domain_weighted_rank`)

```
排序权重 = EWMA_TTFB × (1 + consec_fail × fail_penalty_weight) × (1 + in_flight)^lb_bias
```

- `fail_penalty_weight` 默认 4.0（连失 1 次权重 ×5，连失 2 次 ×9）
- `lb_bias` 默认 1.0（在途积压指数惩罚，least-active 语义）
- slow-start 代理强制垫底
- 排序顺序: 过滤熔断 → 过滤并发超限 → slow-start 分层 → 质量(未知垫底) → 加权权重 → 同权重随机打乱

### 4. 域名级隔离
- `ordered_for_domain(domain)` 优先用**域名级 EWMA** 排序
- 该域名无观测的代理垫底、权重 0（靠前尝试）
- 解决了"全局 EWMA 跨域名平均掩盖某代理对特定域名真实快慢"的污染问题

---

## 二、现存局限（针对"更准确评判代理对特定 URL 响应速度"）

| 维度 | 原现状 | 缺口 | 现状 |
|------|--------|------|------|
| **延迟指标** | 仅 TTFB EWMA | 无全响应时间、无 P50/P95/P99、无抖动度量 | ✅ 已闭合：窗口分位(P50/P95/P99) + t-digest **终身**分位 + OFB 源站首字节；P99 已作为 Cost 主延迟项 |
| **吞吐/带宽** | 完全未收集 | 无法区分"首字节快但下载慢"的代理 | ⚠️ 部分闭合：`throughput_ewma`/`total_bytes` 已采集并纳入 Cost，但**隧道流量占多数时按响应体的吞吐测不到**（实测 7 代理均近 0），故 Cost 中权重极低(0.1)且需累计字节 ≥1MB 才参与 |
| **成功率/错误率** | 仅连续失败计数+熔断 | 无域名级成功率、无错误分类 | ✅ 已闭合：域名级 success/total + Laplace 平滑、7 类错误分类、5xx 单独归因 |
| **粒度** | 域名级 (`github.com:443`) | 无 URL 路径级、无 CDN 边缘节点感知 | ⚠️ 仍未做（见 §六.4；path-level 与 CDN 识别均为可选增强） |
| **连接质量** | 无 | 无连接复用率、TLS 会话恢复率、HTTP/2/3 支持度 | ⚠️ 部分闭合：HTTP 协议版本已采集（H1/H2/H3 分布）；**连接复用率与 TLS 会话恢复因前向代理架构本质不可观测**，不伪造指标 |
| **自适应权重** | 仅 EWMA+在途+失败惩罚 | 无吞吐、成功率、P99 延迟纳入加权 | ✅ 已闭合：Phase 2 多目标 Cost（P99 尾部优先 + 成功率 + 吞吐），min–max 归一化，默认开启 |

---

## 三、改进方案/补丁计划

> **完成状态标记**: `[x]` 已完成，`[~]` 部分完成，`[ ]` 未开始。
>
> 总体进度（截至 Phase 2 完成 + 双重计数修复）:
> - Phase 1 指标采集 ✅（1.1–1.3 完成，1.4 部分完成——连接复用/TLS 恢复**架构上不可观测**，见下注）
> - Phase 2 多目标 Cost 排序 ✅（默认开启，`cost_sort_enabled=false` 一键回滚）
> - Phase 3 单发降级门控 ✅（域名级 成功率/P99/吞吐 三信号，默认关闭）
> - Phase 4 探测对齐 ✅（`probe_with_get`，默认关闭）
> - Phase 5 可观测性/API ✅
> - 补充：t-digest 终身分位、低样本置信度标识、Laplace 平滑 ✅
> - 修复：`domain=None` 时指标双重计数（污染全局成功率）✅
>
> **当前最大风险**: Phase 2 已默认开启，但线上实例仍运行旧代码（需重启生效），
> 且权重是依据 DB 快照静态定的、**未经过生产 A/B 观测**。见 §八「下一步」。

### Phase 1: 指标采集增强（低侵入、高价值）

| # | 变更点 | 说明 | 预估工作量 | 状态 |
|---|--------|------|------------|------|
| 1.1 | `selector.py:record_ttfb()` | 同步记录 **响应体大小** 与 **吞吐**(`body_bytes / transfer_time`,EWMA 平滑)。TTLB 维度已移除:盲 HTTPS 隧道无 per-response body 边界,改用「源站首字节 OFB」补足源站侧延迟(详见 §1.1 注) | 1-2h | [x] |
| 1.2 | `selector.py` 新增 | 维护**每代理/域名的滑动窗口统计**：`success_count`, `total_count`, `latency_samples[]`(环形缓冲存 P50/P95/P99 计算) | 3-4h | [x] |
| 1.3 | `router.py:_try_http/_try_tunnel` | 在 finally 块捕获**错误分类**（timeout/connect_error/5xx/tls_error/cancelled），写入域名级错误计数器 | 2h | [x] |
| 1.4 | `selector.py` 新增 | 域名级 **HTTP 协议版本、连接复用、TLS 会话恢复** 标记采集（从 httpx response/connection 拿） | 2h | [~] 部分完成（见下注） |

> **Phase 1.4 实际落地说明**（前向代理可观测性边界）：
> - **HTTP 协议版本**：✅ 已采集。`record_protocol(pid, resp.http_version, domain)` 在 HTTP 路径（收到 httpx `resp` 时）记录 `http_versions` 累计计数 `{版本串: n}`（"HTTP/1.1"/"HTTP/2"/"HTTP/3"），双作用域（全局+域名桶）双写、随 metric_dict JSON 持久（跨重启可追溯）。仪表盘累计表新增「协议(累计)」列，展示如 `H2 62% / H1 38%`。
> - **连接复用率**：❌ 不可观测。httpx 不暴露公开「连接是否复用」标记；HTTPS CONNECT 为裸字节隧道，复用发生在上游连接池内部。以 **HTTP/2 占比**（`http_versions` 中 H2 比例）作为连接效率的可观测代理信号——H2 多路复用即隐含复用收益。
> - **TLS 会话恢复**：❌ 不可观测。CONNECT 隧道中 TLS 终止在「上游代理 ↔ 源站」之间，对本代理完全不透明，无法采集，**不伪造指标**。
> 结论：协议版本为 Phase 2 健康分数提供了真实输入（H2 能力/占比）；连接复用与 TLS 恢复因前向代理架构本质不可见，审阅建议中的「连接复用率 / HTTP2·3 支持 / TLS session reuse 纳入健康分数」仅协议版本维度可落地。

> **实际落地的差异说明**（相对上表原设想）:
> - 为不破坏既有排序/熔断语义,新增观测结构与选择用 `_quality`/`_domain_quality`
>   完全**并行**(`_proxy_metrics`/`_domain_metrics`),不改动任何排序/权重逻辑,
>   零行为变化。
> - 吞吐用 `body_bytes / transfer_time`(EWMA 平滑)估算;TTLB 维度已移除。
> - 盲 HTTPS 隧道无 per-response body 边界,改用「源站首字节 OFB」:
>   隧道建立 → 上游→客户端方向首个数据块的耗时 ≈ 源站 TCP + TLS 握手首字节,
>   与 TTFB(代理侧 CONNECT 握手)互补,覆盖整条"代理→源站"链路(详见 §1.1)。
> - 错误分类键: `timeout` / `connect` / `http_5xx` / `tls` / `protocol` /
>   `cancelled` / `other`。竞速取消的败者(CancelledError)**不计**为错误(与熔断
>   语义一致,避免健康慢代理被误统计),5xx 经 `record_http_error` 单独归因。
> - 新结构在 `reset_quality` 一并清空。

> **数据结构建议**（Phase 1 实际采用,字段与上表略有调整）：
> ```python
> _proxy_metrics[pid]["metrics"] = _domain_metrics[domain][pid]["metrics"] = {
>     "ttfb_samples": [float, ...],   # 最近 _OBS_WINDOW(256) 个 TTFB,算 P50/P95/P99
>     "ofb_samples":  [float, ...],   # 最近 N 个源站首字节(源站侧延迟,见 §1.1)
>     "throughput_ewma": float,       # 吞吐 EWMA (MB/s)
>     "success": int, "total": int,   # 成功率 = success/total
>     "errors": {timeout: n, connect: n, http_5xx: n, tls: n, protocol: n,
>                cancelled: n, other: n},
>     "total_bytes": float,           # 累计 body 字节
>     "transfer_time": float,         # 累计 body 转发耗时(秒)
> }
> ```

---

### Phase 2: 加权排序函数重构（核心收益）

> 状态: [x] **已完成**（待提交）。默认开启，配 `cost_sort_enabled` 一键回滚。

**设计依据（`auto_squid.db` 实际数据，7 代理逐项核对后定权重）**：

| 指标 | 实测分布 | 结论 |
|------|----------|------|
| 成功率(累计) | 0.935 – 0.984 | 高度聚集、区分度低；不能作唯一信号，但须纳入避免选到最差者 |
| TTFB(avg) | 66 – 151 ms（采样数千） | 主区分信号 |
| TTFB P99（digest） | 可用 | 选定作为主延迟项 |
| 吞吐 EWMA | 0.0009 – 0.008 MB/s（近 0） | 多数流量为 CONNECT 隧道，按响应体计的吞吐基本测不到 → **低权重 + 仅足量字节参与**，否则除零/噪声 |
| OFB | 样本参差(96–474) | 不作独立主信号 |

**Cost 函数**（候选集内 min–max 归一化，权重直接可比、与量纲无关）：

```
base_cost = w_lat × norm(latency)          # p99（默认，尾部优先）或 ewma
          + w_sr  × norm(1 - success_rate) # 平滑成功率，域名级优先
          + w_tp  × norm(1 - throughput)   # 仅累计字节 >= 下限时纳入
final     = 上面三项加权和（负载因子已折进延迟值再归一化）
```

**关键实现取舍（两个已修的坑）**：
1. **负载因子必须先折进延迟值再归一化**：`lat_eff = lat × fail_penalty_mult × (1+active)^lb_bias`，再对 `lat_eff` 做 min–max。若"先归一化再乘每代理负载因子"，会因「全局归一化 vs 每代理乘子」错配而破坏与纯 EWMA 的顺序等价性（实测会让 `test_backlog_deprioritizes_fast_proxy` 等 4 个用例翻转）。折进后 `norm(lat_eff)` 与 legacy `ewma × load_mult` 单调同序，**仅延迟场景下 Cost 排序与纯 EWMA 逐位等价**。
2. **缺失数据取中性 0.5**（既不奖也不罚）；某指标全缺则该项对所有候选贡献 0；`max==min` 时贡献 0。

**配置**（新增于 `CircuitConfig`，router 经 `router_cfg.circuit` 读取，与 `lb_bias` 同级；`Router.__init__` 也带同名默认参数，保证 kwargs 与 router_cfg 两条构造路径一致）：

| 字段 | 默认 | 说明 |
|------|------|------|
| `cost_sort_enabled` | **True** | 总开关。False 即完整回退纯 EWMA（零行为变化，canary/回滚） |
| `cost_latency_metric` | `"p99"` | 主延迟项：`p99`（尾部优先）/ `ewma`；P99 样本不足自动回退 EWMA |
| `cost_weight_latency` | 1.0 | 延迟主项权重 |
| `cost_weight_success_rate` | 0.6 | 成功率项权重 |
| `cost_weight_throughput` | 0.1 | 吞吐项权重（实测近 0 故极低） |
| `cost_latency_min_samples` | 1 | P99 项所需 digest 最小样本（与 EWMA obs≥1 一致；设 8 会让单样本场景延迟项中性化→排序随机） |
| `cost_throughput_min_bytes` | 1_000_000 | 吞吐项所需累计字节下限 |

**行为变化（P99 尾部优先的预期后果）**：延迟均值与尾部不一致时，赢家会从"均值最优"翻转为"尾部最优"（例：spiky 0.02+0.30 均值 0.104 但尾部 0.30，steady 0.10+0.20 均值 0.13 但尾部 0.20 → 均值口径选 spiky，尾部口径选 steady）。这是所选策略的预期语义，非回归。

**测试**（`tests/test_phase_metrics.py` 增 8 例）：默认开启、仅延迟时与 EWMA 等价、高成功率优先、关闭即等价 legacy、缺数据中性、吞吐字节下限、ewma-vs-p99 翻转、配置可达。全量 **302 通过**（基线 278）。

**回归处理**：`TestOrderedForDomain::test_domain_fast_first` 与 `TestCircuitBreaker::test_backoff_expiry_triggers_slow_start` 本意是验证 legacy 机制（域名级 vs 全局 EWMA 语义 / slow-start 分层），已在测试内显式 `cost_sort_enabled=False` 保留原语义；Cost 由上述专属用例覆盖。

**实现位置**: `selector.py:_cost_raw_inputs()` / `_cost_scores()` / `ordered_proxies()` / `ordered_for_domain()`；`router.py` 配置解包与透传；`config_schema.py: CircuitConfig`。

> ✅ **已修复的既有 bug**（排查 Phase 2 Cost 排序时发现，随后单独提交修复）：
> `record_ttfb` / `record_origin_first_byte` / `record_http_error` / `record_failure` 的
> 双作用域循环写成 `for scope in (self._metrics_for(pid, domain), self._metrics_for(pid, None))`——
> 当 `domain=None` 时两次调用返回**同一个全局 dict**，循环对它写**两次**，故非域名流量的
> 全局 `success/total/ttfb_samples/cum_*` 被双重计数（实测单次 `record_ttfb` 后
> `success=2, total=2, digest n=2`，而带 domain 时正确为 1）。
> 影响：探活（`_probe_proxy` 无 domain，只写全局桶）等非域名记录被放大 2 倍，
> **全局成功率向非域名流量倾斜**——直接污染 Phase 2 的成功率输入项。
> 修法：统一改为 `record_protocol` 的去重写法 `(g, m) if m is not g else (g,)`（4 处）。
> 连带修正：`test_legacy_metrics_restore_without_cum_fields_no_keyerror` 的
> `cum_success == 3` 断言编码的是 bug 行为，已更正为 2（域名桶仍 1）。
> 新增 3 个回归用例锁定单计/双写/5xx 单次回退语义。
> **历史数据说明**：修复只对之后的新样本生效；`auto_squid.db` 中已落盘的行仍带
> 膨胀计数（无法从膨胀值反推真实的域名/非域名混合比例，故不做回溯改写）。
> 偏差会随新数据累积自然衰减；如需立即干净起步，可用 `/quality/reset` 清空 EWMA，
> 或自行决定是否重置 `proxy_metrics`/`domain_metrics` 表。

---

### Phase 3: 域名缓存/单发路径质量门控增强

当前: `_single_send_degraded()`、`_worse_than_best()` 只看 EWMA 恶化倍数/绝对差值。

**增强**: 引入**域名级成功率阈值**、**P99 延迟阈值**、**吞吐下限**作为额外降级触发条件。

> 状态: [x] **已完成**。`_single_send_degraded()` 在既有「连续失败 / EWMA 恶化」之外新增三条
> 域名级信号,直接消费 Phase 1 采集的指标(任一阈值 >0 才启用,默认全关闭=零行为变化):
> - `single_send_degrade_success_rate`:域名级 `success/total` 低于阈值即降级(需样本 >=8,
>   避免低样本噪声把偶发失败误判为劣质)。
> - `single_send_degrade_p99_ms`:域名级 TTFB 与 OFB 分位 P99 取较大者(覆盖「代理握手 +
>   源站首字节」整条链路),超阈即降级(需样本 >=4)。
> - `single_send_degrade_min_throughput`:域名级吞吐 EWMA 低于下限即降级(需样本 >=4),
>   防「首字节快但下载慢」的代理被钉死。
> 阈值新增于 `CircuitConfig`(router 经 `router_cfg.circuit` 读取),并同步加入 `Router.__init__`
> 的关键字参数与默认值(=0 关闭),保证 kwargs 与 router_cfg 两条构造路径一致。

```python
# Router._single_send_degraded() 扩展
def _single_send_degraded(self, domain, pid, ref_ewma):
    # 现有：连续失败、EWMA 恶化比值/绝对值
    # 新增：
    dq = self.selector._domain_quality_for(domain, pid)
    if dq:
        success_rate = dq["success"] / max(1, dq["total"])
        if success_rate < self.single_send_degrade_success_rate:  # 新配置，如 0.95
            return True
        if dq.get("p99_latency", 0) > self.single_send_degrade_p99_ms / 1000:
            return True
        if dq.get("ewma_throughput", 0) < self.single_send_degrade_min_throughput:  # MB/s
            return True
```

---

### Phase 4: 探测/预热对齐（一致性）

> 状态: [x] 已完成（待提交）。

- `_probe_proxy()` 此前只做 CONNECT 握手，**不拉取业务数据** → 探测延迟 ≠ 业务 TTFB/OFB
- **改进**: 新增 `probe_with_get`（`--probe-with-get` 开关 + 白名单 `probe_get_targets` + 间隔 `probe_get_interval_sec` / 超时 / 最大字节）：每轮每个上游只测一个目标、按 `(代理,目标)` 限速率、独立短连接 client（与业务 `_client_pool` 隔离，用完即关），经上游发轻量 GET 并把 TTFB/协议版本/吞吐/5xx 写入**该域名真实指标桶**（探测对齐目的）。默认关闭、新增流量受白名单 + 限速率约束。
- 探针失败只计观测（`probe_get_failed`），**不**当作上游故障/熔断，避免目标站限流/拒绝对被误记成上游熔断（一致性约定 §5）。
- 预热池 `ClusterGraph` 预测桶应共享**域名级质量表**，而非仅用全局 EWMA（尚未做，留待后续）

---

### Phase 5: 可观测性/API 暴露

> 状态: [x] 已完成（提交 `1128150`）。

| 端点 | 新增字段 | 状态 |
|------|----------|------|
| `/quality` | `p50`, `p95`, `p99`, `success_rate`, `throughput_mbps`, `error_breakdown` | [x] 经新增 `/quality/meta` 暴露 `get_pid_quality_v2()` |
| `/domains/meta` | 域名级完整统计（上述所有） | [x] 每域名增 `proxy_metrics`（含握手/源站首字节分位、成功率、错误分类、吞吐） |
| `/metrics` | 暴露分位数、成功率、吞吐、错误分类计数器 | [x] `/metrics` 增 `proxy_metrics`；新增 `/metrics/per-destination` 提供 (域名,代理) 粒度 |

> 计划中的 `/quality` 原位扩字段改为**新增 `/quality/meta` 端点',避免破坏既有
> `/quality` 消费方（其返回 `{pid: {ewma_ttfb, obs}}` 结构未变）。`test_routing.py`
> 每代理排序行同步展示握手/源站首字节分位、成功率、错误分类、吞吐。

**仪表盘「窗口 / 累计」双表拆分**（提交 `7ccb4ae` / `7958c68`，Phase 5 的 UI 延续完善）：

- `_proxy_metrics` 实际落地为**两族并行指标**（详见 `CODEBUDDY.md` 的「Metrics: windowed EWMA vs cumulative」说明）：
  - **窗口族**（`_OBS_WINDOW=256` 样本）：TTFB/OFB 分位、EWMA 延迟、错误分类、近期 `window_success_rate`/`window_success_count`/`window_total`（新增 `outcome_samples` 环形缓冲记录近 256 次成功/失败，5xx 时把末尾 `1` 改写 `0`，口径与累计一致）。反映**近期**表现，即路由排序实际使用的信号。
  - **累计族**（终身 `cum_*` 计数器 → `_cumulative_view`）：`cum_success`/`cum_failure_transport`/`cum_failure_5xx`、均值、单调 `total_bytes`/`transfer_time`，派生真·终身 `success_rate`/`avg_ttfb_ms`/`avg_ofb_ms`/`throughput_mbps`，**持久化 SQLite、跨重启可追溯**，是唯一永久有意义的数字。
- 仪表盘全局与按域名指标改为**并排双表**：窗口表（P50/P95/P99 + 近期成功率/成功-总数 + 近 256 错误分类）与累计表（握手/源站首字节均值 + 累计吞吐 + 终身成功率 + 累计字节），同列同代理便于左右对比。`cumLine` 改用 `cum_failure_transport`/`cum_failure_5xx` 正确显示累计失败数。
- **一致性约定（不可破坏）**：仪表盘主表展示累计（永久值）为主、窗口 EWMA 为辅；两者同源同一份内存数据，勿再引入「主表 EWMA + 某行累计」的错位展示。

---

## 四、权衡与建议

| 方案 | 优点 | 缺点/风险 | 建议优先级 |
|------|------|-----------|------------|
| Phase 1 仅加指标不改排序 | 零风险、立即可观测真实表现 | 排序仍用旧指标，短期不改善路由 | **P0** ✅ 已完成 |
| Phase 2 多目标 Cost 排序 | 根治"首字节快但下载慢/成功率低"代理被选中 | 需调参、权重配置复杂、可能引入震荡 | **P1** ✅ 已完成（默认开 + `cost_sort_enabled` 一键回滚；权重待生产观测微调） |
| Phase 3 单发降级增强 | 保护缓存/粘性路径不钉死劣质代理 | 降级阈值调优需生产观测 | **P1** ✅ 已完成（三信号默认关闭=零行为变化，待生产定阈值） |
| Phase 4 探测对齐 | 让探活数据更贴近业务 | 增加探活流量、可能触发目标站限流 | **P2** ✅ 已完成（`probe_with_get` 默认关闭 + 白名单 + 限速率） |
| Phase 5 API 暴露 | 运维/仪表盘直接受益 | 无逻辑风险 | **P0** ✅ 已完成 |

---

## 五、最小可行增强建议

1. **先落 Phase 1.1-1.3**（采集吞吐、成功率、错误分类、P99 样本、握手+源站首字节分位）+ Phase 5 暴露 — ✅ **已完成**（提交 `1128150`,TTLB 维度随后替换为 OFB）
2. 观测 1-2 周生产数据，确认哪些指标与用户感知强相关 — ⚠️ **未做（被跳过，见 §八）**
3. 再决定 Phase 2 权重公式的具体形式（线性加权 vs 分段函数 vs 学习式）— ✅ **已完成**（min–max 归一化线性加权，P99 尾部优先）

> **关于第 2 步被跳过**：Phase 2/3/4 是在尚未完成生产观测的情况下提前推进的。
> 代价是 Phase 2 的默认权重只能依据 `auto_squid.db` 的**静态快照**推导
> （成功率聚集 0.935–0.984 区分度低、吞吐近 0、延迟跨度 66–151ms），
> 而非"哪些指标与用户感知强相关"的实测结论。因此 Phase 2 上线后**必须先观测
> 再调参**，异常即 `cost_sort_enabled: false` 回滚。

> 下一步: 用 `/quality/meta` 与 `/metrics/per-destination` 对 `github.com:443`
> 实测,确认握手/源站首字节/成功率/错误分类采集到位。
> Phase 5 的仪表盘「窗口/累计」双表拆分与窗口成功率已落地（提交 `7ccb4ae`/`7958c68`）,
> 运维可直接从双表对比代理的近况与历史表现。

---

## 六、补充完善建议（审阅汇总）

为帮助后续 Phase 2/3 设计与上线，这里把审阅建议按主题给出，可直接作为 PR checklist：

1) 指标与统计方法（测量质量与鲁棒性）
- [x] t-digest 自包含实现（`auto_squid/digest.py` 的 `TDigest`：k1 尺度函数聚类似质心 + 硬上限 `MAX_CENTROIDS=96`，p50/p95 误差 <1%、p99 长尾略大；做成 `dict` 子类以随 `json.dumps` 直接落盘、旧 DB 行惰性重新包装）。已替换 `selector` 环形缓冲排序：终身累计 TTFB/OFB 分位由 digest rollup 产出（`ttfb_percentiles`/`ofb_percentiles`），窗口分位仍用 `_OBS_WINDOW` 样本（保留两族并行语义）。
- [x] 为每个分位数输出 `samples` 数与 `low_confidence` 标识（`< _PCT_LOW_CONF_N=8` 时仪表盘标 `⚠低样本`），避免低样本噪声驱动路由决策。
- [x] 对 success_rate 使用贝叶斯平滑（Laplace：`(s+α)/(n+α+β)`，α=β=0.5）以避免 0/1 极端值影响；窗口版与累计版均套用，原始 `success`/`total` 计数仍独立保留。
- 在计算 EWMA/percentile 前做简单的去极值或剪裁策略（例如忽略异常超长请求或把其归入特殊 bucket）。
- 计时请使用 monotonic 时钟（time.perf_counter）以防系统时间跳变污染指标。

2) 指标设计细节（现有指标补充）
- 明确所有指标单位与命名：TTFB(握手)/OFB(源站首字节)/吞吐用毫秒(ms)/MB/s；EWMA 的 alpha/半衰期需文档化。
- 明确定义重试/重传如何计数（一次请求发生多次重试时如何统计 success/total/ retry_rate）。
- 记录响应体大小分布与 request size bucket（短/中/长），便于区分“下载慢”与“首字节慢”。
- 把连接复用率、HTTP/2/3 支持、TLS session reuse 作为可选采集字段，并考虑将它们纳入健康分数。

3) 排序/权重函数改进建议（Phase 2）— 已随 Phase 2 落地部分
- [x] 对指标先做归一化或对数变换 —— 采用 **min–max 归一化**（候选集内），使各维度权重直接可比、与量纲无关（选它而非 log：log 对近 0 的吞吐不稳定，而 min–max 天然处理"某维度全缺"的情况）。
- [ ] 将不确定性（obs）纳入 Cost（样本少时增加探索权重或降低 confidence），例如 `normalized_metric / sqrt(1 + k/obs)`。目前仅用 `cost_latency_min_samples`(默认 1)做最低样本门槛，未做置信度折扣。
- [x] 结合 success_rate（平滑后）与短期失败惩罚：长期平滑成功率作稳定权重 + `fail_penalty_mult`（连续失败）作快速惩罚，两者并存。
- [ ] 轻量的探索策略（epsilon-greedy 或 Thompson Sampling）做低频探索，防止长期不尝试恢复良好的代理。目前未知质量代理被 `_quality_rank` 垫底，恢复靠熔断退避到期 + slow-start，**无主动探索**。
- [ ] 在 Cost 计算中保留**分解输出**（`latency_cost` / `success_cost` / `throughput_cost`）以便可解释性与调参。**刻意留到上线后再做**：Phase 2 权重是静态快照推定的，观测期最需要的正是这个分解视图来判断该调哪个权重。

4) 域名/路径粒度与缓存策略
- 对关键路径做 path-level 观测（可选白名单），并为缓存敏感路径（静态资源 vs API）单独设阈值。
- 提供域名缓存 TTL 与显式失效策略（当域名级指标恶化超过阈值时触发缓存失效并快速切换）。
- 对潜在 CDN 场景加以识别（通过 response headers / ip/geolocation）避免把不同边缘节点的行为混为代理质量问题。

5) 探测/预热与采样风险（Phase 4）— 已随 Phase 4 落地
- [x] probe-with-get 限制速率（按 (代理,目标) 限速 + 每轮每代理只测一个目标轮转）、白名单域名（`probe_get_targets`）、与业务请求隔离（每次独立短连接 client，不占业务 `_client_pool`，用完即关）。
- [~] 探测任务不占用主请求池资源：已满足（独立短连接）；「最小/最大并发并带后退策略」未做——当前单连接串行探测，无并发可限；失败仅计观测不熔断，无指数后退需求。
- [x] 探针失败只计观测（`probe_get_failed`），不当作上游故障/熔断，避免目标站限流/拒绝对被误记成上游熔断。

6) 可观测性、报警与数据保留（Phase 5 补充）
- Prometheus 指标标签规范：包含 `proxy_id`, `domain`, `region`（如有）, `protocol`（h1/h2/h3）等，但避免高基数标签（path 不宜作为 label，改用 histogram buckets 或外部聚合）。
- 建议预置告警：整体 success_rate 下降、p99 超阈、throughput 降幅过大、探测失败率升高等。
- 指标保留策略：高精度原始样本短期保存（如 1-7 天），长期只保留 rollup（p95/p99 per-hour/day）以节约存储。

7) 安全、隐私与接口稳定性
- /quality/meta 与 /metrics/per-destination 应限制输出敏感信息（避免上游真实 IP 等），并对外接口做鉴权或内部网限定。
- 配置（权重、阈值）应支持运行时动态更新并具备回滚开关。

8) 上线兜底与回滚策略
- 首次上线 Phase 2 时做 canary（小流量）验证，并保留旧策略做 A/B 对比。
- 对新策略引入慢启动与保护：对新加入或重启的代理施加 slow-start 探索限制。
- 一键回滚：配置开关或环境变量能迅速禁用新权重并回退到 EWMA 策略。

9) 测试与验证计划
- 单元测试：覆盖 metric 采集、error 分类、percentile 计算与权重函数逻辑。
- 性能测试：在压力环境下评估采集开销（CPU/内存/锁争用）并确定采样率上限。
- 灰度实验：A/B 指标对比（旧排序 vs 新排序），收集至少 1-2 周数据并做统计显著性分析。

10) 小的实现细节与陷阱
- 保证竞速取消(CancelledError)不产生重复计数或日志噪音；败者取消不计入 error counters（已处理）。
- OBS_WINDOW 的大小需与代理数量/内存预算校准（256 是起点，可调）。
- 明确 throughput 的计算方式为 body_bytes / transfer_time（排除头部和连接空闲时间），并对 chunked/streaming 场景标注为不可比样本。

---

## 七、可落地的短期任务（建议作为 PR checklist）

- [x] 引入 t-digest 自包含实现并实现 p50/p95/p99 接口（见 `auto_squid/digest.py`，替代分片环形缓冲）
- [x] 为分位数暴露样本数/置信度并在仪表盘累计表加「终身分位」行 + 低样本警示
- [x] 对 success_rate 使用贝叶斯平滑（Laplace）与 retry_rate 分离统计
- [ ] 实现 metrics 标签规范并在 /metrics 中限制高基数标签
- [x] 设计并实现 Phase 2 的 Cost 函数（min–max 归一化 + 默认权重，P99 尾部优先）；生产灰度盯 p99/成功率，异常即 `cost_sort_enabled=False` 回滚
- [x] 编写单元与集成测试覆盖新采集与排序逻辑 — **305 通过**（`tests/test_phase_metrics.py` 覆盖 t-digest/协议/Phase 3 门控/Phase 4 探测/Phase 2 Cost/双计数回归；`test_end_to_end.py` 为既有基线）
- [x] 修复 `domain=None` 时指标双重计数（4 处双作用域循环，非域名流量全局指标被放大 2 倍）

---

## 八、下一步（当前状态下的优先级排序）

> 更新于 Phase 2 + 双重计数修复完成之后。

### 前置事实

- **Phase 2 已默认开启，但线上实例仍运行旧代码** —— 所有改动（Phase 2 Cost 排序、
  Phase 3 门控、Phase 4 探测、t-digest、双重计数修复）都要**重启后才生效**。
- Phase 2 的默认权重是依据 `auto_squid.db` **静态快照**推定的，未经生产 A/B 验证
  （§五第 2 步被跳过）。P99 尾部优先会翻转部分"均值快但尾部差"代理的赢家顺位。
- 双重计数修复后，新样本计数正确；DB 中旧数据仍带膨胀计数，偏差随新数据衰减。

### 优先级排序

| 优先级 | 事项 | 为什么 |
|--------|------|--------|
| **P0（先做）** | **重启线上实例加载新代码，进入观测期** | 不重启则一切改动不生效；观测 1-2 周是 Phase 2 调参的前提（计划 §五第 2 步）。盯 `/quality/meta` 与 `/metrics/per-destination` 的 p99/成功率/吞吐，异常即 `cost_sort_enabled: false` 回滚 |
| **P1 ✅** | **Cost 分解输出暴露**：`/quality/meta` 增 `cost_breakdown`（`latency_cost`/`success_cost`/`throughput_cost` + 归一化后的原始值） | §六.3 刻意留到观测期的项。观测期最需要的正是这个分解视图——否则"该调哪个权重"只能凭感觉。纯观测增强，不改路由行为。（**未做**，观测期开始前/中实现均可） |
| **P1 ✅ 已完成** | **Cost 权重热更新 + 自动调参器**：`GET/POST /cost`（运行时改权重，下一次排序即生效）+ `POST /tuner`（启停）+ `AutoTuner`（`auto_tune.enabled` 默认 False；开启后保守爬山：每窗口单维 ±25% 扰动，赢家 TTFB 均值改进 ≥5% 且成功率守卫达标才采纳，恶化立即回滚，基线权重持久化 SQLite 跨重启恢复） | 手动热更新比重启（丢全部窗口态）代价小得多，紧急回滚更及时；自动调参把观测期"看数据→调权重"闭环自动化。objective 采集走 asyncio task 侧信道取**纯赢家 TTFB**（record_ttfb 混入败者不能用；结果元组不能追加——`_cleanup_tunnel_result` 用 `result[-1]`，追加会炸清理路径）。安全边界硬编码（latency∈[0.2,4.0] 等），`POST /cost` 未知键 422 |
| **P2** | **预置告警**：整体 success_rate 下降、p99 超阈、探测失败率升高 | §六.6。观测期的人工盯屏替代；建议先用最简单的阈值轮询实现 |
| **P2** | **性能测试**：采集开销（CPU/内存/锁争用）与采样率上限 | §六.9。t-digest、Cost 计算与调参采样都进了热路径（`ordered_proxies` 每请求调用），需确认无回归 |
| **P3（可选）** | Cost 不确定性折扣（`/sqrt(1+k/obs)`）、探索策略（epsilon-greedy）、path-level 观测、CDN 识别、metrics 标签规范 | §六 中剩余的进阶项；等观测数据说话后再决定做哪个 |

### 明确不建议现在做的

- **Phase 2 权重调参**：没有 1-2 周观测数据与 Cost 分解视图之前，任何权重改动都是盲调。
- **回溯改写 DB 历史计数**：无法从膨胀值反推真实混合比例，收益低风险高；让偏差自然衰减即可。
