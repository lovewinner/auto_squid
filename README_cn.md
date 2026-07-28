# auto_squid（中文说明）

轻量级正向代理，支持**并行竞速**、域名统计、SQLite 持久化。

## 概述

- 在网关主机运行，接受 HTTP/HTTPS 代理流量，将每个请求转发到上游代理
- **并行竞速**：每次请求同时发往所有上游代理，使用最先返回的成功响应
- 探测引擎周期性测试代理延迟、吞吐、可靠性，计算评分辅助选择
- 域名级胜出统计持久化到 SQLite，重启不丢失

## 功能

- HTTP 与 HTTPS（CONNECT）转发，**并行竞速所有上游代理**
- 加权随机代理排序（基于探测评分）
- Hop-by-hop 响应头过滤（`transfer-encoding`、`content-encoding` 等）+ `Content-Length` 重写
- 运行时 ProxyStore（YAML 加载/保存），管理 API CRUD
- 探测引擎：TCP 连接 + HTTP GET，吞吐测量、IQR 异常过滤、时间衰减打分
- 域名级胜出统计持久化到 SQLite（`auto_squid.db`）
- 管理 API：`/health`、`/proxies`、`/score`、`/probe/*`、`/stats`、`/domains`、`/metrics`
- CLI：同时启动代理、探测循环和管理 API

## 快速开始

1. 创建虚拟环境：

   ```bash
   uv venv .venv --seed && uv sync
   ```

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
   ```

   可选参数：`--proxies ./proxies.yaml` `--db ./auto_squid.db` `--config ./config.yaml`

4. 验证：

   ```bash
   curl http://127.0.0.1:18080/health
   curl http://127.0.0.1:18080/proxies
   curl http://127.0.0.1:18080/stats
   curl http://127.0.0.1:18080/domains
   ```

5. 作为代理使用：

   ```bash
   curl -x http://127.0.0.1:10808 http://www.baidu.com
   curl -x http://127.0.0.1:10808 https://www.baidu.com
   ```

## 架构

```
客户端 ──HTTP/S──> auto_squid (B:10808)
                      │
                      ├── 并行 ──> 上游代理 1 (squid)
                      ├── 并行 ──> 上游代理 2 (squid)
                      └── 并行 ──> 上游代理 3 (squid)
                      │
                      ▼ 最快的响应被使用，其余取消
```

- **HTTP 请求**：通过 `httpx.AsyncClient` 并行竞速（每个请求独立 client）
- **CONNECT 请求**：通过 `asyncio.open_connection` 并行竞速隧道
- **评分**：探测引擎定时测试；`ProxySelector.ordered_proxies()` 加权随机排序（竞速模式会尝试所有代理，排序不影响结果）
- **统计**：`request_counts` 各代理胜出次数，`attempted_counts` 各代理尝试次数；`domain_stats` 各域名下各代理胜出次数（SQLite 持久化）

## API 端点

| 端点 | 说明 |
|------|------|
| `GET /health` | 健康检查 |
| `GET /proxies` | 列出已配置代理 |
| `POST /proxies` | 添加代理（JSON body） |
| `GET /score` | 各代理当前评分 |
| `GET /probe/status` | 探测循环状态 |
| `GET /probe/history` | 探测历史样本 |
| `GET /probe/states` | 代理状态（warming/normal/degraded） |
| `GET /stats` | `request_counts` + `attempted_counts` |
| `GET /domains` | 从 SQLite 读取的域名胜出统计 |
| `GET /metrics` | 合并评分、状态、计数、域名统计 |

## 配置

通过 `--config` 传入 YAML 配置文件，结构见 `config_schema.py`：

```yaml
listen:
  host: "0.0.0.0"
  port: 10808
api:
  host: "0.0.0.0"
  port: 18080
probe:
  url: "http://www.baidu.com"
  interval: 60
  timeout: 10
  concurrency: 5
  history_minutes: 60
  min_samples: 10
logging:
  file: "auto_squid.log"
```

## 限制

- HTTP 解析为 MVP 级别，大流量流式响应可能有边界情况
- 管理 API 无鉴权，生产环境请用防火墙保护端口 18080
- CONNECT 隧道使用原始管道（无 TLS 拦截）

## 许可证

MIT
