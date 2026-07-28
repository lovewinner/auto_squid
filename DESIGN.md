# Squid 多代理智能路由系统 — 程序设计文档

## 1. 需求分析

### 1.1 问题陈述

用户拥有多个分布在远端机器上的 Squid 代理服务器，需要一种机制：

- **自动探测**每个代理的当前延迟和吞吐量
- **基于性能数据**为每次上网请求选择最优代理
- **减少人工维护成本**——代理增减、网络抖动时自动适应

### 1.2 核心指标

| 指标 | 说明 | 采集方式 |
|------|------|----------|
| TCP 连接延迟 | 到代理:3128 的 RTT | 主动探测 |
| HTTP 响应延迟 | 通过代理请求一个参考 URL 的完整时间 | 主动探测 |
| 下载吞吐量 | 参考 URL 的文件大小 / 耗时 | 主动探测 |
| 历史成功率 | 近期请求成功/失败比例 | 日志统计 |
| 综合评分 | 加权后的可排序分数 | 算法计算 |

### 1.3 约束条件

- 代理均在远端，无 syslog 直接访问
- 依赖代理暴露的标准 HTTP/SOCKS 端口
- 探活流量应保持最小化（不影响代理正常使用）
- 单点架构，不做分布式多节点

---

## 2. 总体架构

```
┌──────────────┐     CONNECT/HTTP     ┌──────────────────┐
│   本地应用    │ ──────────────────▶  │   Auto-Squid     │
│ (浏览器/curl) │     透明转发        │   本地监听端口    │
└──────────────┘                     │   10808 HTTP     │
                                     └────────┬─────────┘
                                              │
                                              ├── 路由决策（选分最高的可用代理）
                                              │
                              ┌───────────────┼───────────────┐
                              │               │               │
                        ┌─────▼──────┐ ┌──────▼──────┐ ┌─────▼──────┐
                        │  Squid #1  │ │  Squid #2  │ │  Squid #3  │
                        │  1.2.3.4   │ │  5.6.7.8   │ │  9.10.11.12│
                        │  :3128     │ │  :3128     │ │  :3128     │
                        └────────────┘ └────────────┘ └────────────┘
                                     ▲
                                     │ 定时探测（异步）
                              ┌─────┴──────┐
                              │  探测管理器  │
                              │  (Probe)   │
                              └────────────┘
                                     ▲
                                     │ 读/写
                              ┌─────┴──────────────────┐
                              │   性能数据库            │
                              │   SQLite / JSON-Lines  │
                              └────────────────────────┘
```

### 组件拆分

| 模块 | 职责 |
|------|------|
| `proxy_store` | 管理代理列表（增删改查、持久化） |
| `probe_engine` | 异步探活 → 计算分 → 写入数据库 |
| `router` | 接收本地请求、选代理、转发流量 |
| `api_server` | HTTP API（查询列表/分数/健康状态） |
| `cli` | 命令行入口 |

---

## 3. 模块详细设计

### 3.1 proxy_store — 代理存储

```
数据模型（JSON 文件 / SQLite）

ProxyInfo {
    id: str              # 用户给定或生成 UUID
    name: str            # 人类可读名称
    host: str            # IP 或域名
    port: int            # 默认 3128
    protocol: str        # "http" | "https" | "socks5"
    auth: Auth | null    # { username, password? }
    enabled: bool        # 是否参与路由
    tags: dict           # 自定义标签（地域/运营商/备注）
    created_at: str      # ISO8601
    updated_at: str
}
```

操作接口：

```python
class ProxyStore:
    def add(self, info: ProxyInfo) -> None
    def remove(self, proxy_id: str) -> None
    def get(self, proxy_id: str) -> ProxyInfo | None
    def list_all(self) -> list[ProxyInfo]
    def update(self, proxy_id: str, **kwargs) -> None
    def set_enabled(self, proxy_id: str, enabled: bool) -> None
```

### 3.2 probe_engine — 探测引擎

**策略：异步并发 + 指数退避 + 滑动窗口**

```python
class ProbeEngine:
    """定时探测所有可用代理，计算综合评分。"""

    def __init__(self, store, db, probe_url="http://www.gstatic.com/generate_204", interval=30):
        ...

    async def probe_one(self, proxy: ProxyInfo) -> ProbeResult:
        """
        对单个代理执行探测：
        1. TCP 握手时间
        2. 发送 HTTP GET 到 probe_url（通过该代理）
        3. 记录状态码、body 大小、总耗时
        返回 ProbeResult
        """

    async def probe_all(self) -> list[ProbeResult]:
        """并发探测所有 enabled 代理（asyncio.gather + 超时控制）"""

    def compute_score(self, history: list[ProbeResult]) -> float:
        """
        综合评分算法（见 3.2.1）
        """

    async def run_once(self) -> None:
        """执行一轮探测 → 写数据库"""
```

#### 3.2.1 评分算法

综合评分采用**加权衰减模型**：

```
Score = α * L_score + β * T_score + γ * R_score

各分量映射到 [0, 100]：

L_score (延迟分) = clamp(100 - (latency_ms / latency_max) * 100, 0, 100)
T_score (吞吐分) = clamp((throughput_kbps / throughput_max) * 100, 0, 100)
R_score (可靠分) = (最近 N 分钟成功次数 / 总探测次数) * 100

默认权重: α=0.5, β=0.3, γ=0.2  (可配置)

指数时间衰减（半衰期 5 分钟）：
weight = exp(-ln(2) * age_minutes / 5)

最终 Score = Σ(probe.weight * probe.component_score) / Σ(probe.weight)
```

**设计理由**：延迟对用户体验影响最直接（50%），吞吐量影响大文件下载（30%），可靠性防止把流量调度到已宕机的代理（20%）。

#### 3.2.2 探活目标 URL 选择

推荐使用以下之一（体积小、全球 CDN、稳定）：

1. `http://www.gstatic.com/generate_204`（204 响应，~0 字节，最适合测延迟）
2. `http://clients3.google.com/generate_204`（同上）
3. 自建探活端点（最高可控性）

用户可通过配置指定自定义 URL。

### 3.3 router — 路由转发

```
请求流程：

1. 本地流量 → 监听端口 10808
2. iptables/透明代理 将目标流量重定向到 10808
   或用户显式将浏览器代理设为 127.0.0.1:10808
3. Router 解析目标地址
4. 从数据库选出 Score 最高且 enabled 的代理
5. 建立到目标代理的 CONNECT 隧道（HTTPS）
6. 或转发 HTTP 请求（如果目标已是 HTTP）
7. 转发响应回客户端
```

**关键设计选择**：

- **透明代理模式**（推荐）：通过 iptables REDIRECT 将本地流量截获，用户无感知
- **显式代理模式**：用户手动配置系统/浏览器代理为 `127.0.0.1:10808`

路由决策在 **每次新连接** 时执行，而非维持会话绑定。这意味着：
- 同一用户的不同连接可能走不同代理（动态负载更均衡）
- 若需要会话粘滞，可后续增加"同一源IP+目标在 N 秒内锁定代理"的选项

```python
class Router:
    def __init__(self, store, db, listen_host="127.0.0.1", listen_port=10808):
        self.server = asyncio.start_server(self.handle, listen_host, listen_port)

    async def handle(self, reader, writer):
        # 1. 读取客户端请求首行
        # 2. 判断 CONNECT vs 普通 HTTP
        # 3. 选最优代理
        # 4. 双向转发，不做内容解析
```

### 3.4 api_server — HTTP 管理接口

基于 FastAPI + uvicorn，监听 `127.0.0.1:18080`。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/proxies` | 列出所有代理及实时评分 |
| GET | `/api/v1/proxies/{id}` | 单个代理详情+历史 |
| POST | `/api/v1/proxies` | 添加代理 |
| PUT | `/api/v1/proxies/{id}` | 更新代理 |
| DELETE | `/api/v1/proxies/{id}` | 删除代理 |
| GET | `/api/v1/health` | 系统健康 |
| GET | `/api/v1/stats` | 汇总统计（平均分、在线数） |

### 3.5 CLI 入口

```
auto-squid 命令                    说明
──────────────────────────────────────────────────────
proxy list                        列出所有代理及评分
proxy add <name> <host> [port]    添加代理
proxy remove <id>                 删除代理
proxy test <id>                   立即单次探测
status                            系统状态总览
```

---

## 4. 数据存储

### 4.1 目录结构

```
~/.config/auto-squid/
├── config.yaml            # 主配置
├── proxies.yaml           # 代理列表（用户编辑）
├── perf.db                # SQLite 性能数据库
└── cache/                 # 可选缓存文件
```

### 4.2 性能数据库 Schema

```sql
-- 单次探测记录
CREATE TABLE probes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    proxy_id    TEXT NOT NULL,
    timestamp   REAL NOT NULL,          -- unix timestamp
    success     INTEGER NOT NULL,
    latency_ms  REAL,                  -- TCP + HTTP 响应总时间
    status_code INTEGER,
    body_bytes  INTEGER,
    error       TEXT
);

CREATE INDEX idx_probes_proxy_ts ON probes(proxy_id, timestamp DESC);

-- 实时最佳分（缓存，避免每次查历史）
CREATE TABLE scores (
    proxy_id    TEXT PRIMARY KEY,
    score       REAL NOT NULL,
    l_score     REAL,
    t_score     REAL,
    r_score     REAL,
    computed_at REAL NOT NULL
);
```

---

## 5. 配置

```yaml
# config.yaml
listen:
  host: 127.0.0.1
  port: 10808          # 透明代理监听端口

api:
  host: 127.0.0.1
  port: 18080          # 管理接口端口

probe:
  url: http://www.gstatic.com/generate_204
  interval: 30         # 探测间隔（秒）
  timeout: 10          # 单次探测超时（秒）
  concurrency: 20      # 并发探测数
  history_minutes: 10  # 评分滑动窗口（分钟）
  score:
    latency_weight: 0.5
    throughput_weight: 0.3
    reliability_weight: 0.2
    half_life_minutes: 5

logging:
  level: INFO
  file: ~/.config/auto-squid/logs/app.log
```

---

## 6. 错误处理与容错

| 场景 | 处理策略 |
|------|----------|
| 单个代理探测超时 | 标记失败，降其可靠分；不中断其他探测 |
| 批量代理全部不可达 | Router 返回 503 给客户端；日志告警 |
| 探测 URL 无法访问 | 降级：仅用 TCP 握手延迟评分；告警 |
| 代理认证失败 | 重复失败 3 次后自动禁用，通知用户 |
| 本地端口占用 | 启动时检测，报错退出，建议换端口 |
| 数据库损坏 | 备份+重建 probes 表；保留 proxies.yaml |

### 超时策略细节

```
per-probe 超时: config.timeout (默认 10s)
per-probe TCP 超时: 3s（在 HTTP 超时内）
全量探测总耗时上限: interval 秒（超时用 asyncio.wait_for 包
```

---

## 7. 性能考量

### 7.1 转发延迟

- `/etc/hosts` 或 **DNAT (iptables)** 截获：增加 <1ms
- 代理转发增加 = 代理延迟（无法避免，这是路由系统价值的来源）

### 7.2 资源占用预估

| 场景 | CPU | 内存 | 磁盘 |
|------|-----|------|------|
| 空闲（等待探测） | < 0.5% | ~20 MB | ~0（SQLite 只追加） |
| 探测中（50个代理并发） | ~5% | ~30 MB | ~0.5 KB/轮 |
| 转发中（100 并发连接） | ~10% | ~40 MB | ~0 |

---

## 8. 依赖与选型

### 8.1 主依赖

```
Python >= 3.10
asyncio               # 标准库，异步转发和探测
aiohttp               # 异步 HTTP 探活（提高并发效率）
fastapi + uvicorn      # 管理 API
click                 # CLI 框架
pyyaml                # 配置解析
pydantic              # 数据校验
```

### 8.2 操作系统要求

- Linux（路由功能依赖 iptables/nftables）
- macOS（显式代理模式可用，透明代理需额外配置）
- Windows（显式代理模式可用；路由部分需自行适配）

---

## 9. 部署与使用

### 9.1 安装启动

```bash
# 克隆/拷贝项目
pip install -r requirements.txt

# state 初始化
auto-squid proxy add "北京节点" 1.2.3.4 3128
auto-squid proxy add "上海节点" 5.6.7.8 3128
auto-squid proxy add "广州节点" 9.10.11.12 3128

# 启动路由 + 探测
auto-squid start
  → 启动监听 10808（路由）+ 18080（API）+ 自动探测循环

# 设置系统代理（二选一）
# 方案 A - 显式代理：
#     浏览器/环境变量设置 socks5://127.0.0.1:10808
# 方案 B - 透明代理（Linux）：
#     sudo iptables -t nat -A OUTPUT -p tcp --dport 80 -j REDIRECT --to-port 10808
#     sudo iptables -t nat -A OUTPUT -p tcp --dport 443 -j REDIRECT --to-port 10808
```

### 9.2 日常使用

```bash
# 查看所有代理状态
auto-squid proxy list

# 立即手动探测单个代理
auto-squid proxy test "北京节点"

# 查看管理 API 返回
curl http://127.0.0.1:18080/api/v1/proxies | jq .

# 停止
auto-squid stop

# 后台运行（默认）
auto-squid start --daemon
```

### 9.3 与现有 Squid 的配合

远端 Squid 不需要做任何改动。auto-squid 作为 **客户端侧路由层**：
- 所有互联网请求先经 auto-squid
- auto-squid 根据评分选一个 Squid 后端
- 流量经选中的 Squid 出去

如果远端 Squid 需要身份验证，在 `proxy add` 时提供 `-u user -p pass` 即可。

---

## 10. 何时扩展

| 条件 | 扩展方向 |
|------|----------|
| 代理数 > 100 | 分片探测，每轮按批次并发 |
| 需要故障自动切换 | 在 Router 层增加"失败 N 次后临时冷却 Q 秒" |
| 需要持久会话粘滞 | 增加"源IP+目标在窗口内锁定代理"模式 |
| 多用户共享 | 增加 per-user 代理分配策略库 |
| 遥测看板 | Grafana + Prometheus 导出指标 |
| 远端 Squid 有 SNMP | 直接从 Squid SNMP 读指标替代 HTTP 探活 |

---

## 11. 总结

| 维度 | 决策 |
|------|------|
| 探测策略 | 异步并发，30s 间隔，指数衰减评分 |
| 路由粒度 | 每连接决策，无会话粘滞（可配置） |
| 转发方式 | asyncio 双向流式转发，零拷贝无缓冲 |
| 数据存储 | SQLite + 滑动窗口 |
| 管理接口 | FastAPI + CLI Click |
| 代理发现 | 用户手动添加（可扩展为自动发现） |
| 透明代理 | iptables REDIRECT 用户无感知 |

---

*文档版本：v1.0 | 日期：2026-07-28*