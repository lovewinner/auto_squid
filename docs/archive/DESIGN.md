# Squid 多代理智能路由系统 — 程序设计文档

## 1. 需求分析

### 1.1 问题陈述

用户拥有多个分布在远端机器上的 Squid 代理服务器，需要一种机制：

- **自动探测**每个代理对特定域名的访问延迟和吞吐量
- **基于域名维度**的性能数据为每次请求选择最优代理
- **减少人工维护成本**——代理增减、网络抖动时自动适应

### 1.2 核心指标

| 指标 | 说明 | 采集方式 |
|------|------|----------|
| TCP 连接延迟 | 到代理的 RTT | 探活时测量 |
| 域名可达延迟 | 「某代理 → 某域名」的完整 HTTP 响应时间 | 探活时测量 |
| 域名吞吐量 | 「某代理 → 某域名」的下载速度 | 探活时测量 |
| 历史成功率 | 近期请求成功/失败比例 | 日志统计 |
| 综合评分 | 加权后的可排序分数 | 算法计算 |

> **关键变化**：评分不再针对代理整体，而是针对 **(域名, 代理)** 对。同一个代理访问 A 域名可能很快，访问 B 域名可能很慢。

### 1.3 约束条件

- 代理均在远端，无 syslog 直接访问
- 依赖代理暴露的标准 HTTP/SOCKS 端口
- 探活流量应保持最小化（不影响代理正常使用）
- 单点架构，不做分布式多节点
- 域名空间可能非常大（用户访问过的所有域名），需要高效存储和查询

---

## 2. 总体架构

```
┌──────────────┐ CONNECT/HTTP ┌──────────────────┐
│ 本地应用     │ ──────────────────▶ │ Auto-Squid     │
│ (浏览器/curl) │ 透明转发          │ 本地监听端口    │
└──────────────┘ │ 10808 HTTP       └────────┬─────────┘
                 └──────────────────────────┘
                                              │
                         ┌────────────────────┤
                         │ 路由决策            │
                         │ (按域名选最优代理)  │
                         └─────────┬──────────┘
                                   │
              ┌────────┬───────────┼───────────┐
              │        │           │           │
        ┌─────▼─────┐ ┌───▼────┐ ┌───▼────┐ ┌───▼────┐
        │ Squid #1  │ │ Squid  │ │ Squid  │ │ Squid  │
        │ 1.2.3.4   │ │ #2     │ │ #3     │ │ #N     │
        │ :3128     │ │ 5.6.7.8│ │ ...    │ │ ...    │
        └───────────┘ └────────┘ └────────┘ └────────┘
                           ▲
                           │ 定时探测（异步）
                    ┌──────┴──────┐
                    │ 探测管理器  │
                    │ (Probe)     │
                    └──────▲──────┘
                           │ 读/写
                    ┌──────┴──────────────────┐
                    │ 性能数据库               │
                    │ SQLite / JSON-Lines     │
                    │ key: (domain, proxy_id) │
                    └─────────────────────────┘
```

### 组件拆分

| 模块 | 职责 |
|------|------|
| `proxy_store` | 管理代理列表（增删改查、持久化） |
| `probe_engine` | 异步探活「域名×代理」对 → 计算分 → 写入数据库 |
| `domain_index` | 域名索引：维护域名-代理评分映射，域名缓存管理 |
| `router` | 接收本地请求、按目标域名查最佳代理、转发流量 |
| `api_server` | HTTP API（查询列表/分数/健康状态） |
| `cli` | 命令行入口 |

---

## 3. 模块详细设计

### 3.1 proxy_store — 代理存储

```
数据模型（JSON 文件 / SQLite）

ProxyInfo {
id: str # 用户给定或生成 UUID
name: str # 人类可读名称
host: str # IP 或域名
port: int # 默认 3128
protocol: str # "http" | "https" | "socks5"
auth: Auth | null # { username, password? }
enabled: bool # 是否参与路由
tags: dict # 自定义标签（地域/运营商/备注）
created_at: str # ISO8601
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

**策略：按域名分组并发探测 + 指数衰减 + 滑动窗口**

```
更新：探测粒度从「代理」变为「域名×代理」对。

路由时根据目标域名，查询该域名下各代理的历史表现，选最优者。
```

```python
class ProbeEngine:
    """定时探测「域名 × 代理」对，计算综合评分。"""

    def __init__(self, store, domain_index, db,
                 probe_url="http://www.gstatic.com/generate_204",
                 interval=60,
                 batch_size=20):
        self.store = store          # ProxyStore
        self.domain_index = domain_index  # DomainIndex
        self.db = db
        self.probe_url = probe_url
        self.interval = interval    # 探测间隔（秒）
        self.batch_size = batch_size  # 每轮并发探测数

    async def probe_pair(self, domain: str, proxy: ProxyInfo) -> ProbeResult:
        """
        对单个「域名×代理」对执行探测：
        1. TCP 握手时间
        2. 通过代理发送 HTTP GET 到 probe_url
        3. 记录状态码、body 大小、总耗时
        返回 ProbeResult
        """

    async def probe_domain(self, domain: str) -> list[ProbeResult]:
        """并发探测 domain 关联的所有 enabled 代理"""

    async def run_once(self) -> None:
        """
        执行一轮探测：
        1. 从 domain_index 获取需要更新的域名列表
        2. 分批并发探测
        3. 写数据库
        """

    def schedule_domain(self, domain: str):
        """将新发现的域名加入探测队列（热插拔）"""
```

#### 3.2.1 探测触发机制

域名探测有三种触发来源，按优先级执行：

| 触发源 | 说明 |
|--------|------|
| **路由日志**（主动） | Router 转发时记录目标域名，标记为"待探测" |
| **定时轮询**（被动） | 每隔 interval 秒扫描缓存，优先探测最近访问过的域名 |
| **手动触发** | CLI `auto-squid probe <domain>` 立即探测某域名 |

**探测策略**：

```
1. 新域名首次出现 → 立即加入探测队列（高优先级）
2. 已有域名 → 按 interval 间隔定期更新
3. 每个域名关联全部 enabled 代理 → 并发探测（限流 batch_size）
4. 探测目标 URL：使用参考 URL（通过代理），非真实用户域名
```

#### 3.2.2 探活目标 URL 选择

推荐使用以下（体积小、全球 CDN、稳定）：

1. `http://www.gstatic.com/generate_204`（204 响应，~0 字节，最适合测延迟）
2. `http://clients3.google.com/generate_204`（同上）
3. 自建探活端点（最高可控性）

> 注意：探测时访问的是参考 URL 而非真实用户域名（避免触发安全告警），但通过不同代理访问同一 URL 的延迟差异，反映了各代理到互联网的整体质量。

用户可通过配置指定自定义 URL。

#### 3.2.3 评分算法

综合评分采用**加权衰减模型**，计算的是「某个代理访问某个域名」的得分：

```
Score = α * L_score + β * T_score + γ * R_score

各分量映射到 [0, 100]（按域名分组统计）：

L_score (延迟分) = clamp(100 - (latency_ms / latency_max) * 100, 0, 100)
T_score (吞吐分) = clamp((throughput_kbps / throughput_max) * 100, 0, 100)
R_score (可靠分) = (最近 N 分钟成功次数 / 总探测次数) * 100

默认权重: α=0.5, β=0.3, γ=0.2 (可配置)

指数时间衰减（半衰期 5 分钟）：
weight = exp(-ln(2) * age_minutes / 5)

最终 Score = Σ(probe.weight * probe.component_score) / Σ(probe.weight)
```

**设计理由**：延迟对用户体验影响最直接（50%），吞吐量影响大文件下载（30%），可靠性防止把流量调度到已宕机的代理（20%）。

### 3.3 domain_index — 域名索引

```
新增模块：维护域名空间和域名-代理评分映射。

DomainIndex {
  domains: dict[str, DomainInfo]

  DomainInfo {
    domain: str                  # 域名
    first_seen: float            # 首次出现时间（unix ts）
    last_seen: float             # 最后出现时间
    probe_priority: int          # 探测优先级（越大越优先）
    proxy_scores: dict[str, ScoreCache]  # proxy_id → ScoreCache

    ScoreCache {
      scores: list[ScoredProbe]  # 滑动窗口
      current_score: float       # 缓存的最新综合分
      last_probe: float          # 最后探测时间
    }

    ScoredProbe {
      timestamp: float
      latency_ms: float
      throughput_kbps: float
      success: bool
      component_scores: dict     # {l_score, t_score, r_score}
      weighted_score: float
    }
  }
}
```

操作接口：

```python
class DomainIndex:
    def register_domain(self, domain: str) -> None:
        """新域名出现时注册（首次使用/路由命中）"""

    def get_best_proxy(self, domain: str, enabled_only: bool = True) -> tuple[ProxyInfo, float] | None:
        """返回某域名下得分最高的可用代理及其分数"""

    def update_score(self, domain: str, proxy_id: str, probe_result: ProbeResult) -> None:
        """将探测结果写入索引，更新缓存的当前分数"""

    def get_domains_needing_probe(self, max_age: float) -> list[str]:
        """返回需要重新探测的域名列表（按 last_probe 时间排序）"""

    def get_all_domains(self) -> list[str]:
        """返回所有已注册域名"""
```

### 3.4 router — 路由转发

```
请求流程：

1. 本地流量 → 监听端口 10808
2. iptables/透明代理 将目标流量重定向到 10808
   或用户显式将浏览器代理设为 127.0.0.1:10808
3. Router 解析目标域名
4. 从 domain_index 查询该域名下 Score 最高的代理
5. 建立到目标代理的 CONNECT 隧道（HTTPS）或转发 HTTP 请求
6. 双向转发流量
7. 记录路由决策日志（域名 → 代理选择）
```

**关键设计选择**：

- **透明代理模式**（推荐）：通过 iptables REDIRECT 将本地流量截获，用户无感知
- **显式代理模式**：用户手动配置系统/浏览器代理为 `127.0.0.1:10808`
- **路由粒度**：每次连接决策，基于域名查 domain_index
- **域名解析**：从 CONNECT 请求或 HTTP 请求中提取目标主机名

```python
class Router:
    def __init__(self, store, domain_index, db,
                 listen_host="127.0.0.1", listen_port=10808):
        self.store = store
        self.domain_index = domain_index
        self.db = db

    async def handle(self, reader, writer):
        # 1. 读取客户端请求首行
        # 2. 提取目标 host（CONNECT: 解析 CONNECT 行; HTTP: 解析 Host header）
        # 3. 提取域名部分
        # 4. 从 domain_index 选最优代理
        # 5. 双向转发，不做内容解析
        # 6. 记录路由决策到 db

    def _extract_domain(self, target: str) -> str:
        """从 host:port 或 URL 中提取域名"""
        pass
```

### 3.5 api_server — HTTP 管理接口

基于 FastAPI + uvicorn，监听 `127.0.0.1:18080`。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/proxies` | 列出所有代理 |
| GET | `/api/v1/proxies/{id}` | 单个代理详情 |
| POST | `/api/v1/proxies` | 添加代理 |
| PUT | `/api/v1/proxies/{id}` | 更新代理 |
| DELETE | `/api/v1/proxies/{id}` | 删除代理 |
| GET | `/api/v1/domains` | 列出所有已发现域名及其各代理评分 |
| GET | `/api/v1/domains/{domain}/ranking` | 某域名下各代理评分排名 |
| POST | `/api/v1/domains/{domain}/probe` | 立即探测某域名 |
| GET | `/api/v1/health` | 系统健康 |
| GET | `/api/v1/stats` | 汇总统计 |

### 3.6 CLI 入口

```bash
auto-squid 命令                         说明
──────────────────────────────────────────────────────
proxy list                             列出所有代理及评分
proxy add <name> <host> [port]         添加代理
proxy remove <id>                      删除代理
proxy test <id>                        立即单次代理健康检测
status                                 系统状态总览
domain list                            列出所有已发现域名
domain ranking <domain>                查看某域名各代理评分排名
domain probe <domain>                  立即探测某域名（所有代理）
```

---

## 4. 数据存储

### 4.1 目录结构

```
~/.config/auto-squid/
├── config.yaml     # 主配置
├── proxies.yaml    # 代理列表（用户编辑）
├── perf.db         # SQLite 性能数据库
└── cache/          # 可选缓存文件
```

### 4.2 性能数据库 Schema

```sql
-- 单次探测记录：key = (domain, proxy_id, timestamp)
CREATE TABLE probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,           -- 目标域名
    proxy_id TEXT NOT NULL,         -- 代理 ID
    timestamp REAL NOT NULL,        -- unix timestamp
    success INTEGER NOT NULL,       -- 是否成功
    latency_ms REAL,               -- 响应总时间
    status_code INTEGER,           -- HTTP 状态码
    body_bytes INTEGER,            -- 响应体大小
    error TEXT,                     -- 错误信息
    l_score REAL,                   -- 延迟分 [0,100]
    t_score REAL,                   -- 吞吐分 [0,100]
    r_score REAL,                   -- 可靠分 [0,100]
    weighted_score REAL             -- 加权综合分
);

CREATE INDEX idx_probes_domain_proxy_ts
    ON probes(domain, proxy_id, timestamp DESC);

-- 域名-代理最新评分缓存（快速查询用）
CREATE TABLE domain_scores (
    domain TEXT NOT NULL,
    proxy_id TEXT NOT NULL,
    score REAL NOT NULL,
    l_score REAL,
    t_score REAL,
    r_score REAL,
    probe_count INTEGER DEFAULT 0,
    computed_at REAL NOT NULL,
    PRIMARY KEY (domain, proxy_id)
);

-- 路由日志：记录每次转发的域名和选中的代理
CREATE TABLE route_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    proxy_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    client_addr TEXT
);
```

---

## 5. 配置

```yaml
# config.yaml
listen:
  host: 127.0.0.1
  port: 10808    # 透明代理监听端口

api:
  host: 127.0.0.1
  port: 18080    # 管理接口端口

probe:
  url: http://www.gstatic.com/generate_204  # 探活参考 URL
  interval: 60            # 探测间隔（秒）
  timeout: 10             # 单次探测超时（秒）
  concurrency: 20         # 每域名并发探测数
  history_minutes: 10     # 评分滑动窗口（分钟）
  batch_domains: 10       # 每轮探测域名数

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
| 某域名无可用代理 | 回退到全局评分最高的代理（兜底策略） |

### 兜底策略

当某域名没有任何代理的历史数据时，Router 按以下优先级选择：

1. 该域名下所有 enabled 代理中 TCP 连接延迟最低的
2. 如果连 TCP 探测都未完成，回退到全局延迟最低的代理
3. 标记该代理为该域名的"默认路由"，下次探测后自动更新

### 超时策略细节

```
per-probe 超时: config.timeout (默认 10s)
per-probe TCP 超时: 3s（在 HTTP 超时内）
全量探测总耗时上限: interval 秒（超时用 asyncio.wait_for 包裹）
```

---

## 7. 性能考量

### 7.1 转发延迟

- `/etc/hosts` 或 **DNAT (iptables)** 截获：增加 <1ms
- 代理转发增加 = 代理延迟（无法避免，这是路由系统价值的来源）

### 7.2 资源占用预估

| 场景 | CPU | 内存 | 磁盘 |
|------|-----|------|------|
| 空闲（等待探测） | < 0.5% | ~20 MB | ~0 |
| 探测中（10域名×20代理并发） | ~5% | ~30 MB | ~0.5 KB/轮 |
| 转发中（100 并发连接） | ~10% | ~40 MB | ~0 |

### 7.3 域名空间增长

- 每天新增假设 500 个独特域名 × 20 个代理 = 10,000 条记录/天
- 每条约 200 字节 → 2 MB/天
- 保留 30 天 → 60 MB，SQLite 轻松处理
- 定期清理：`DELETE FROM probes WHERE timestamp < cutoff`

---

## 8. 依赖与选型

### 8.1 主依赖

```
Python >= 3.10
asyncio          # 标准库，异步转发和探测
aiohttp          # 异步 HTTP 探活（提高并发效率）
fastapi + uvicorn # 管理 API
click            # CLI 框架
pyyaml           # 配置解析
pydantic         # 数据校验
aiosqlite         # 异步 SQLite
```

### 8.2 操作系统要求

- Linux（路由功能依赖 iptables/nftables）
- macOS（显式代理模式可用，透明代理需额外配置）
- Windows（显式代理模式可用；路由部分需自行适配）

---

## 9. 部署与使用

### 9.1 安装启动

```bash
# 安装依赖
pip install -r requirements.txt

# 初始化配置
auto-squid proxy add "北京节点" 1.2.3.4 3128
auto-squid proxy add "上海节点" 5.6.7.8 3128
auto-squid proxy add "广州节点" 9.10.11.12 3128

# 启动路由 + 探测
auto-squid start
→ 启动监听 10808（路由）+ 18080（API）+ 自动探测循环

# 设置系统代理（二选一）
# 方案 A - 显式代理：
# 浏览器/环境变量设置 http://127.0.0.1:10808
# 方案 B - 透明代理（Linux）：
# sudo iptables -t nat -A OUTPUT -p tcp --dport 80 -j REDIRECT --to-port 10808
# sudo iptables -t nat -A OUTPUT -p tcp --dport 443 -j REDIRECT --to-port 10808
```

### 9.2 日常使用

```bash
# 查看所有代理状态
auto-squid proxy list

# 查看某域名下各代理评分
auto-squid domain ranking google.com

# 立即手动探测某域名（所有代理）
auto-squid domain probe google.com

# 查看管理 API 返回
curl http://127.0.0.1:18080/api/v1/domains | jq .

# 查看系统状态
auto-squid status

# 停止
auto-squid stop

# 后台运行
auto-squid start --daemon
```

### 9.3 与现有 Squid 的配合

远端 Squid 不需要做任何改动。auto-squid 作为 **客户端侧路由层**：

- 所有互联网请求先经 auto-squid
- auto-squid 根据域名查 domain_index，选最优 Squid 后端
- 流量经选中的 Squid 出去

如果远端 Squid 需要身份验证，在 `proxy add` 时提供 `-u user -p pass` 即可。

---

## 10. 何时扩展

| 条件 | 扩展方向 |
|------|----------|
| 域名数 > 10,000 | 按热度分片探测，低频域名降低探测频率 |
| 代理数 > 100 | 增加每域名并发探测数，分批调度 |
| 新域名无数据 | 预热机制：新域名建立时立即全量探活一次 |
| 需要故障自动切换 | Router 层增加"失败 N 次后临时冷却 Q 秒" |
| 需要会话粘滞 | 增加"同一源IP+目标在窗口内锁定代理"模式 |
| 多用户共享 | 增加 per-user 代理分配策略库 |
| 遥测看板 | Grafana + Prometheus 导出指标 |
| 远端 Squid 有 SNMP | 直接从 Squid SNMP 读指标替代 HTTP 探活 |

---

## 11. 总结

| 维度 | 决策 |
|------|------|
| 探测粒度 | **域名×代理** 对，而非代理全局 |
| 探测触发 | 路由日志驱动 + 定时轮询双通道 |
| 路由策略 | 按目标域名查 domain_index → 选最高分代理 |
| 兜底策略 | 新域名无数据时回退到全局最低延迟代理 |
| 路由粒度 | 每连接决策，无会话粘滞（可配置） |
| 转发方式 | asyncio 双向流式转发，零拷贝无缓冲 |
| 数据存储 | SQLite + domain_index 内存索引 |
| 管理接口 | FastAPI + CLI Click |
| 代理发现 | 用户手动添加（可扩展为自动发现） |
| 透明代理 | iptables REDIRECT 用户无感知 |

---

*文档版本：v2.0 | 日期：2026-07-28*
