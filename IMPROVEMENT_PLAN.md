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
| 1.1 | `selector.py:record_ttfb()` | 同步记录 **完整请求耗时**（TTLB）与 **响应体大小**，计算吞吐 | 1-2h | [x] |
| 1.2 | `selector.py` 新增 | 维护**每代理/域名的滑动窗口统计**：`success_count`, `total_count`, `latency_samples[]`(环形缓冲存 P50/P95/P99 计算) | 3-4h | [x] |
| 1.3 | `router.py:_try_http/_try_tunnel` | 在 finally 块捕获**错误分类**（timeout/connect_error/5xx/tls_error/cancelled），写入域名级错误计数器 | 2h | [x] |
| 1.4 | `selector.py` 新增 | 域名级 **HTTP 协议版本、连接复用、TLS 会话恢复** 标记采集（从 httpx response/connection 拿） | 2h | [ ] |

> **实际落地的差异说明**（相对上表原设想）:
> - 为不破坏既有排序/熔断语义,新增观测结构与选择用 `_quality`/`_domain_quality`
>   完全**并行**(`_proxy_metrics`/`_domain_metrics`),不改动任何排序/权重逻辑,
>   零行为变化。
> - TTLB 用"body 转发耗时"表达(即 TTLB−TTFB 增量),同时算吞吐
>   (`body_bytes / transfer_time`,EWMA 平滑)。
> - 错误分类键: `timeout` / `connect` / `http_5xx` / `tls` / `protocol` /
>   `cancelled` / `other`。竞速取消的败者(CancelledError)**不计**为错误(与熔断
>   语义一致,避免健康慢代理被误统计),5xx 经 `record_http_error` 单独归因。
> - 新结构在 `reset_quality` 一并清空。

> **数据结构建议**（Phase 1 实际采用,字段与上表略有调整）：
> ```python
> _proxy_metrics[pid]["metrics"] = _domain_metrics[domain][pid]["metrics"] = {
>     "ttfb_samples": [float, ...],   # 最近 _OBS_WINDOW(256) 个 TTFB,算 P50/P95/P99
>     "ttlb_samples": [float, ...],   # 最近 N 个 TTLB(body 转发耗时)
>     "ttlb_ewma": float,             # TTLB EWMA
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

- `_probe_proxy()` 目前只做 CONNECT 握手，**不拉取业务数据** → 探测延迟 ≠ 业务 TTFB/TTLB
- **改进**: 探活可选模式 `--probe-with-get` 对关键域名（如 `api.github.com`）做轻量 GET，记录完整指标
- 预热池 `ClusterGraph` 预测桶应共享**域名级质量表**，而非仅用全局 EWMA

---

### Phase 5: 可观测性/API 暴露

> 状态: [x] 已完成（提交 `1128150`）。

| 端点 | 新增字段 | 状态 |
|------|----------|------|
| `/quality` | `p50`, `p95`, `p99`, `success_rate`, `throughput_mbps`, `error_breakdown` | [x] 经新增 `/quality/meta` 暴露 `get_pid_quality_v2()` |
| `/domains/meta` | 域名级完整统计（上述所有） | [x] 每域名增 `proxy_metrics`（含 TTFB/TTLB 分位、成功率、错误分类、吞吐） |
| `/metrics` | 暴露分位数、成功率、吞吐、错误分类计数器 | [x] `/metrics` 增 `proxy_metrics`；新增 `/metrics/per-destination` 提供 (域名,代理) 粒度 |

> 计划中的 `/quality` 原位扩字段改为**新增 `/quality/meta` 端点**,避免破坏既有
> `/quality` 消费方（其返回 `{pid: {ewma_ttfb, obs}}` 结构未变）。`test_routing.py`
> 每代理排序行同步展示 TTFB/TTLB 分位、成功率、错误分类、吞吐。

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

1. **先落 Phase 1.1-1.3**（采集 TTLB、吞吐、成功率、错误分类、P99 样本）+ Phase 5 暴露 — ✅ **已完成**（提交 `1128150`）
2. 观测 1-2 周生产数据，确认哪些指标与用户感知强相关
3. 再决定 Phase 2 权重公式的具体形式（线性加权 vs 分段函数 vs 学习式）

> 下一步（可选）: 线上重启加载新代码后,用 `/quality/meta` 与
> `/metrics/per-destination` 对 `github.com:443` 实测,确认 TTFB/TTLB/成功率/错误
> 分类采集到位;随后据此推进 Phase 2 加权排序与 Phase 1.4 协议/复用标记采集。
