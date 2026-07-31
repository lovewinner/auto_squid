# auto_squid（中文说明）

轻量级正向代理，支持**并行竞速**、域名缓存、HTTP 响应缓存，以及 SQLite 持久化的统计。

> [English →](README.md)

## 概述

- 在网关主机运行，接受 HTTP/HTTPS 代理流量，将每个请求转发到上游代理
- **并行竞速**：每次请求同时发往多个上游代理，使用最先返回的成功响应，其余取消并释放连接
- **域名缓存**：某个代理为某域名竞速胜出后，在 `cache_ttl` 有效期内复用该代理，避免每个请求都竞速
- **HTTP 响应缓存**：幂等 `GET` 响应在内存中缓存（TTL 60s，遵循 `Cache-Control`）
- **本机竞速**：可选，让网关主机自身作为代理节点直接参与竞速（不走上游）
- **域名统计**：各域名胜出次数持久化到 SQLite，重启不丢失
- **Web 界面**：内置仪表盘 `/`，可浏览域名统计、默认代理、胜出次数，支持自动刷新

## 功能

- HTTP 与 HTTPS（`CONNECT`）转发，**并行竞速多个上游代理**
- 域名级缓存（`cache_ttl`），按域名复用胜出代理
- 内存级 HTTP `GET` 响应缓存，遵循 `Cache-Control`
- 可选本机竞速节点（网关与上游一同竞速）
- Hop-by-hop 响应头过滤（`transfer-encoding`、`content-encoding`、`content-length` 等），并按实际 body 长度重写 `Content-Length`
- 请求体处理设有 10 MB 上限（超限返回 `413`）；`Content-Length: 0` 处理正确（不会卡死）
- CONNECT 隧道设有连接/读取超时，挂死的上游不会永久占用竞速槽位
- SQLite 访问加锁串行化，在 FastAPI/uvicorn 线程池下安全
- 优雅关闭：先取消并排空在途连接，再关闭数据库
- 运行时 ProxyStore（YAML 加载/保存），管理 API CRUD
- 域名级胜出统计持久化到 SQLite（`auto_squid.db`）
- 管理 API + 单页 Web 界面

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

- **HTTP 请求**：通过 `httpx.AsyncClient` 并行竞速（每次尝试独立 client），胜出响应回写，落败 client 关闭
- **CONNECT 请求**：通过 `asyncio.open_connection` 隧道并行竞速，带连接/读取超时
- **选择**：`ProxySelector.ordered_proxies()` 返回随机打乱的已启用代理列表；前 `max_retries` 个先竞速，失败后再对剩余代理竞速兜底
- **域名缓存**：胜出后把代理记入 `domain_meta`，在 `cache_ttl` 有效期内对该域名复用
- **统计**：`request_counts` 各代理胜出次数，`attempted_counts` 各代理尝试次数；`domain_stats` 各域名下各代理胜出次数（SQLite 持久化）

## API 端点

| 端点 | 说明 |
|------|------|
| `GET /` | Web 仪表盘（域名统计、默认代理、自动刷新） |
| `GET /health` | 健康检查 |
| `GET /proxies` | 列出已配置代理 |
| `POST /proxies` | 添加代理（JSON body） |
| `GET /stats` | `request_counts` + `attempted_counts` |
| `GET /metrics` | `request_counts`、`attempted_counts` 与域名统计 |
| `GET /config` | 路由配置（`enable_local_racing`） |
| `GET /domains` | 从 SQLite 读取的域名胜出统计 |
| `GET /domains/meta` | 各域名默认代理 + 最近更新时间 |

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
logging:
  file: "auto_squid.log"
```

## 测试

```bash
.venv/bin/python -m pytest -q
```

测试套件覆盖 HTTP/CONNECT 转发、HTTP 响应缓存、域名缓存、本机竞速、`ProxyStore` CRUD、API，以及二进制安全的请求体处理。

## 限制

- HTTP 解析为 MVP 级别，大流量流式响应可能有边界情况
- 管理 API 无鉴权，生产环境请用防火墙保护端口 18080
- CONNECT 隧道使用原始管道（无 TLS 拦截）

## 许可证

MIT
