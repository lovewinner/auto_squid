# auto_squid 性能压测工具

`bench/` 提供一套**可控、可重复、可归因**的压测,用于准确评价 auto_squid 的性能。

## 文件

- `mock_upstream.py` — 受控上游代理集群。模拟真实 HTTP/HTTPS 代理(绝对 URL 请求 + CONNECT 回显隧道),延迟/响应大小/chunked/失败率由配置决定,排除真实网络抖动。每实例带**命中计数器**,据此算出真实缓存命中率与 racing 放大率。
- `stress.py` — 压测主驱动。启动 mock 集群(或读真实 `proxies.yaml`)→ 启动 Router → 按模式跑负载 → 输出终端表格 + 结构化 JSON。

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

# cProfile 覆盖(定位瓶颈,输出 bench_profile.txt)
python -m bench.stress --profile
```

## 压测模式

| 模式 | 负载形态 | 测什么 |
|------|---------|--------|
| `staircase` | 并发数 1→200 阶梯,每级固定请求数 | 吞吐/延迟随并发的变化,**找饱和点** |
| `rate` | 目标 RPS 100→2000 阶梯,持续发 | 延迟/错误率随负载的变化,**找容量上限** |
| `mixed` | 30%热+20%大响应+20%chunked+20%冷+10%CONNECT | 贴近真实流量的**混合画像** |
| `soak` | 固定并发长时持续(默认 60s) | **稳定性与资源泄漏**(周期打印 RSS/fd/连接池/缓存) |
| `all` | 依次跑上述四种 | 全面评价 |

## 关键指标(准确性设计)

- **吞吐** (req/s)、**TTFB** 与 **total** 的 P50/P95/P99 —— 客户端用 raw socket 精确到读到状态行的时间。
- **错误率与分类** (conn / timeout / `http:<状态码>` / echo-mismatch)。`http:` 类按上游实际状态码细分(如 `{'503': 12}`),一眼看出 DNS 失败(503)还是别的根因。
- **状态码分布** —— 所有结果(含成功)的状态码分布。real 模式下尤其关键:揭示"成功"背后的真实状态码(如全是 503 = 上游 DNS 失败,数据无效)。
- **缓存命中率** = 被缓存吸收(未触达上游)的请求占比。用 mock 命中计数器测,**区分于上游自身缓存**。
- **racing 放大率** = 上游命中 / 客户端请求。>1 表示竞速扇出超过单发(冷请求竞速);<1 表示缓存吸收了部分请求。
- **资源采样**:进程 RSS、文件描述符数、Router 连接池大小、HTTP 缓存条目数(峰值 + 末值)。

> mock 模式可测缓存命中率/放大率;`--upstream real` 模式无法测(上游不计命中),记为 N/A。

## 真实上游模式(`--upstream real`)

指向 `proxies.yaml` 里的真实上游代理,贴近生产。与 mock 模式有几处关键差异:

- **主机名**:真实代理会真正解析主机名,故压测打向**内置默认大站池**(www.baidu.com 等,可被 `--real-hosts host1,host2,...` 覆盖)。主机名必须可被上游代理解析,否则全部 503(看状态码分布即可发现)。
- **成功判定**:真实站点对压测用的路径(`/p0` 等)常返回 3xx/4xx,但这不代表代理失败——代理已成功转发,源站状态码与代理性能无关。故 real 模式把"收到任何 HTTP 响应"都记为成功(仅 conn/timeout 算真失败)。**务必看状态码分布**确认响应不是全 503。
- **CONNECT**:真实 TLS 隧道会加密 payload,无法原样回显,故 real 模式 CONNECT"建隧道即成功"(不做 echo 校验),只测隧道建立。
- **缓存指标**:真实上游无命中计数器,缓存命中率/放大率记 N/A。要测缓存收益请用 mock 模式。
- **超时**:real 模式客户端超时上调到 20s(上游延迟高),避免把"慢"误判为 timeout。

```bash
python -m bench.stress --upstream real --mode all --duration 120
python -m bench.stress --upstream real --real-hosts www.baidu.com,www.qq.com
```

## 隔离缓存层

HTTP 响应缓存会掩盖路由路径的真实性能(缓存命中后 TTFB 仅 ~0.6ms,测的是缓存而非代理)。两个对照跑法:

- `python -m bench.stress` —— 完整路径(含 HTTP + 域名缓存),测生产体感。
- `python -m bench.stress --no-http-cache` —— 禁用 HTTP 响应缓存,**测纯路由性能**(域名缓存仍生效,可单独观察 racing + 连接池)。

对照二者的吞吐与延迟差,即 HTTP 响应缓存的收益。

## 可比性

- 同一 mock 配置 + 同一 Router 代码,多次跑结果可重复(延迟确定性高)。
- JSON 报告带 git 版本,跨提交/跨优化可 diff:
  ```bash
  python -m bench.stress --mode all --output before.json   # 优化前
  python -m bench.stress --mode all --output after.json    # 优化后
  diff <(jq -S . after.json) <(jq -S . before.json)
  ```

## 输出示例

```
■ 场景: mixed
  请求数        : 2000  (成功 2000, 失败 0)
  吞吐          : 560.1 req/s
  TTFB (ms)     : P50=14.0  P95=212.6  P99=212.8  mean=35.5
  缓存命中率    : 78.0%  (上游命中 66)
  racing 放大率 : 0.22x
  资源峰值      : RSS=46MB  fd=11  连接池末值=2  HTTP缓存条目末值=12
```
