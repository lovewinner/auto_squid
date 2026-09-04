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

| 维度 | 现状 | 缺口 |
|------|------|------|
| **延迟指标** | 仅 TTFB EWMA | 无全响应时间、无 P50/P95/P99、无抖动度量 |
| **吞吐/带宽** | 完全未收集 | 无法区分"首字节快但下载慢"的代理 |
| **成功率/错误率** | 仅连续失败计数+熔断 | 无域名级成功率、无错误分类(超时/5xx/证书/连接重置) |
| **粒度** | 域名级 (`github.com:443`) | 无 URL 路径级(`/user/repo` vs `/api`)、无 CDN 边缘节点感知 |
| **连接质量** | 无 | 无连接复用率、TLS 会话恢复率、HTTP/2/3 支持度 |
| **自适应权重** | 仅 EWMA+在途+失败惩罚 | 无吞吐、成功率、P99 延迟纳入加权 |

---

## 三、改进方案/补丁计划

> **完成状态标记**: `[x]` 已完成（提交 `1128150`），`[ ]` 未开始。

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

> 状态: [ ] 未开始。依赖 Phase 1 观测数据稳定后再定权重形式。

**目标**: 把排序权重从单一 `EWMA_TTFB` 升级为**多目标综合 Cost**：

```
Cost = w1 × Latency_P99 + w2 × (1 - Success_Rate) + w3 × (1/Throughput) + w4 × Retry_Rate
```

- 可配置权重（`selector.py` 构造参数或 `RouterConfig` 字段）
- 保留 least-active 惩罚 `(1 + in_flight)^bias`
- 失败惩罚改用 `1 / Success_Rate` 而非线性 `consec_fail`

**实现位置**: `selector.py:_weighted_rank()`, `_domain_weighted_rank()`, `ordered_for_domain()`

**兼容性**: 旧配置（仅 EWMA）作为默认回退，新权重字段默认 0 不生效。

---

### Phase 3: 域名缓存/单发路径质量门控增强

当前: `_single_send_degraded()`、`_worse_than_best()` 只看 EWMA 恶化倍数/绝对差值。

**增强**: 引入**域名级成功率阈值**、**P99 延迟阈值**、**吞吐下限**作为额外降级触发条件。

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

- `_probe_proxy()` 目前只做 CONNECT 握手，**不拉取业务数据** → 探测延迟 ≠ 业务 TTFB/OFB
- **改进**: 探活可选模式 `--probe-with-get` 对关键域名（如 `api.github.com`）做轻量 GET，记录完整指标
- 预热池 `ClusterGraph` 预测桶应共享**域名级质量表**，而非仅用全局 EWMA

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
| Phase 2 多目标 Cost 排序 | 根治"首字节快但下载慢/成功率低"代理被选中 | 需调参、权重配置复杂、可能引入震荡 | **P1（Phase1稳定后）** |
| Phase 3 单发降级增强 | 保护缓存/粘性路径不钉死劣质代理 | 降级阈值调优需生产观测 | **P1** |
| Phase 4 探测对齐 | 让探活数据更贴近业务 | 增加探活流量、可能触发目标站限流 | **P2（可选）** |
| Phase 5 API 暴露 | 运维/仪表盘直接受益 | 无逻辑风险 | **P0** ✅ 已完成 |

---

## 五、最小可行增强建议

1. **先落 Phase 1.1-1.3**（采集吞吐、成功率、错误分类、P99 样本、握手+源站首字节分位）+ Phase 5 暴露 — ✅ **已完成**（提交 `1128150`,TTLB 维度随后替换为 OFB）
2. 观测 1-2 周生产数据，确认哪些指标与用户感知强相关
3. 再决定 Phase 2 权重公式的具体形式（线性加权 vs 分段函数 vs 学习式）

> 下一步（可选）: 线上重启加载新代码后,用 `/quality/meta` 与
> `/metrics/per-destination` 对 `github.com:443` 实测,确认握手/源站首字节/成功率/
> 错误分类采集到位;随后据此推进 Phase 2 加权排序与 Phase 1.4 协议/复用标记采集。
> Phase 5 的仪表盘「窗口/累计」双表拆分与窗口成功率已落地（提交 `7ccb4ae`/`7958c68`）,
> 运维可直接从双表对比代理的近况与历史表现。

---

## 六、补充完善建议（审阅汇总）

为帮助后续 Phase 2/3 设计与上线，这里把审阅建议按主题给出，可直接作为 PR checklist：

1) 指标与统计方法（测量质量与鲁棒性）
- 建议接入 t-digest 或 HDRHistogram 来计算 p50/p95/p99，比分片环形缓冲更稳健且内存友好，尤其对 p99 有明显好处。
- 为每个分位数输出观测样本数与置信度标识（如 obs < N 标注低置信度），避免低样本噪声驱动路由决策。
- 对 success_rate 使用贝叶斯平滑（Laplace：(s+α)/(n+α+β)，α=1/2/2）以避免 0/1 极端值影响。
- 在计算 EWMA/percentile 前做简单的去极值或剪裁策略（例如忽略异常超长请求或把其归入特殊 bucket）。
- 计时请使用 monotonic 时钟（time.perf_counter）以防系统时间跳变污染指标。

2) 指标设计细节（现有指标补充）
- 明确所有指标单位与命名：TTFB(握手)/OFB(源站首字节)/吞吐用毫秒(ms)/MB/s；EWMA 的 alpha/半衰期需文档化。
- 明确定义重试/重传如何计数（一次请求发生多次重试时如何统计 success/total/ retry_rate）。
- 记录响应体大小分布与 request size bucket（短/中/长），便于区分“下载慢”与“首字节慢”。
- 把连接复用率、HTTP/2/3 支持、TLS session reuse 作为可选采集字段，并考虑将它们纳入健康分数。

3) 排序/权重函数改进建议（Phase 2）
- 对指标先做归一化或对数变换（如 log(latency)），避免不同量级直接相加导致权重难以调。
- 将不确定性（obs）纳入 Cost（样本少时增加探索权重或降低confidence），例如 normalized_metric / sqrt(1 + k/obs)。
- 建议结合 success_rate（平滑后）与短期失败惩罚：既用长期 success_rate 做稳定权重，又用最近 N 次失败触发快速惩罚。
- 考虑轻量的探索策略（epsilon-greedy 或 Thompson Sampling）做低频探索，防止长期不尝试恢复良好的代理。
- 在 Cost 计算中保留分解输出（latency_cost、success_cost、throughput_cost）以便可解释性与调参。

4) 域名/路径粒度与缓存策略
- 对关键路径做 path-level 观测（可选白名单），并为缓存敏感路径（静态资源 vs API）单独设阈值。
- 提供域名缓存 TTL 与显式失效策略（当域名级指标恶化超过阈值时触发缓存失效并快速切换）。
- 对潜在 CDN 场景加以识别（通过 response headers / ip/geolocation）避免把不同边缘节点的行为混为代理质量问题。

5) 探测/预热与采样风险（Phase 4）
- probe-with-get 必须限制速率、白名单域名，并与业务请求隔离（独立的连接/线程池）。
- 探测任务不能占用主请求池资源，建议设置最小/最大并发并带后退策略。

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

- [ ] 引入 t-digest/HDRHistogram 库并实现 p50/p95/p99 接口（Phase 1 补充）
- [ ] 为分位数暴露样本数/置信度并在 `/quality/meta` 上加标识
- [ ] 对 success_rate 使用贝叶斯平滑与 retry_rate 分离统计
- [ ] 实现 metrics 标签规范并在 /metrics 中限制高基数标签
- [ ] 设计 Phase 2 的 Cost 函数草案（含归一化/对数变换 & 默认权重）并在小流量 canary 上 A/B 验证
- [ ] 编写单元与集成测试覆盖新采集与排序逻辑

---

我已经把审阅建议直接合并到文档末尾（新增“六、补充完善建议” 与“七、可落地的短期任务”），以便团队在现有计划上按优先级拆分 PR。

下一步我可以：
- （A）给出 `t-digest` 的接入示例和代码片段并帮你实现一个小 PR；或
- （B）草拟 Phase 2 的 Cost 函数实现（含归一化/对数变换、观测不确定性处理和默认权重）；或
- （C）把上面的短期任务拆成 Issue 模板并创建到仓库（如果你希望我直接创建 issue 我可以继续）。

你希望我接着做哪项？
