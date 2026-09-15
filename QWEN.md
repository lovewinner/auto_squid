# QWEN.md — auto_squid 项目上下文

## 项目概述

auto_squid 是一个轻量级正向代理，核心能力是**并行竞速**从多个上游代理中选最快者转发 HTTP/HTTPS 流量，并叠加域名缓存、会话粘性、HTTP 响应缓存、SQLite 持久化统计、管理 API 与 Web 仪表盘。

- **语言/框架**: Python 3.10+, FastAPI + uvicorn + httpx + pydantic v2 + typer + pyyaml
- **入口**: `python -m auto_squid.cli`（或 `auto-squid`），同时启动代理端口与管理 API
- **双端口**: 代理 `:10808`，管理 API `:18080`
- **状态**: master 分支；当前测试基线 342 用例，CI 覆盖 3.10/3.11/3.12

## 架构

`Router`（`router.py`）是薄编排层，真实状态在协作模块里，通过白名单 `__getattr__/__setattr__` 转发：

| 文件 | 职责 |
|------|------|
| `auto_squid/router.py` | 客户端处理、竞速循环、域名缓存、单发降级、策略路由、SQLite 持久化、转发 shim |
| `auto_squid/selector.py` | 每代理 EWMA 延迟、熔断 + slow-start、自适应并发、多目标 Cost 排序 + 指标（窗口 + 累计） |
| `auto_squid/pools.py` | 三层 CONNECT 预热池（通用池 / target 半预连接 / 已建握手复用） |
| `auto_squid/sticky.py` | per-client+domain 会话粘性表 |
| `auto_squid/http_cache.py` | GET 响应缓存（LRU、在途聚合、写方法失效） |
| `auto_squid/cluster.py` | 请求簇共现图 → 预测并预热 co-target |
| `auto_squid/tuner.py` | Cost 权重保守爬山自动调参器（默认关闭） |
| `auto_squid/config_schema.py` | pydantic 配置模型（`extra="forbid"` + 跨字段校验） |
| `auto_squid/api.py` / `cli.py` | 管理 API + 仪表盘；Typer 入口（uvloop、配置加载、uvicorn） |
| `auto_squid/proxy_store.py` | 上游代理列表 YAML 持久化 + CRUD |

请求处理链条：连接/认证 → 短路检查（直连白名单 / 在途 GET 聚合）→ 分发决策（粘性 → 域名缓存 → 竞速）→ 降级反馈（单发失败立即降级回竞速）→ 失败计数（竞速败者累加熔断器）→ 三层 TCP 预热池。

## 构建与运行

```bash
# 环境初始化
uv venv .venv --seed && uv sync

# 启动代理 + API（默认读 ./config.yaml 与 ./proxies.yaml）
uv run python -m auto_squid.cli

# 指定配置
uv run python -m auto_squid.cli --config examples/config.yaml --proxies examples/proxies.yaml --db auto_squid.db

# 验证
curl http://127.0.0.1:18080/health
curl http://127.0.0.1:18080/proxies
curl http://127.0.0.1:18080/stats
curl http://127.0.0.1:18080/domains
curl http://127.0.0.1:18080/quality/meta
curl http://127.0.0.1:18080/metrics/per-destination

# 作为代理使用
curl -x http://127.0.0.1:10808 http://www.baidu.com
curl -x http://127.0.0.1:10808 https://www.baidu.com
```

## 测试

```bash
# 全量
uv run pytest -q --timeout=60

# 单用例
uv run pytest tests/test_end_to_end.py::TestFoo::test_bar -q

# 关键词过滤
uv run pytest -q -k "circuit"
```

- `asyncio_mode = "auto"`，无需 `@pytest.mark.asyncio`
- 无 linter/formatter 配置；测试是 gate
- 单测超时 60s（CI 挂起经验值）

## 开发约定

- **注释风格**: 中文注释，解释 *why* 而非 *what*；新增代码保持该风格
- **配置模型**: `extra="forbid"`，拼错配置键启动即硬报错，不会静默落默认
- **指标双族**: 窗口族（近期 256 样本，用于路由排序）与累计族（终身 t-digest，SQLite 持久化）。仪表盘/CLI 主表展示累计为主、窗口为辅，不要混用
- **路由转发**: `Router` 热路径属性名白名单转发到协作对象；调用方继续用 `router.stickiness_enabled` 等同名属性，无需感知 delegation
- **持久化**: `domain_stats`（域名胜出统计）与 `domain_meta`（域名默认代理）持久化到 SQLite；后台 `_flush_loop` 每 5s 批量落盘，访问加 `_db_lock`
- **配置文件**: `config.yaml` / `proxies.yaml` 在 `.gitignore`，使用 `examples/config.yaml` 与 `examples/proxies.yaml` 作为模板
- **bench**: `bench/` 是压测工具目录，不纳入 `auto_squid` 发行包

## 关键配置项速查

| 配置 | 作用位置 | 默认 | 说明 |
|------|----------|------|------|
| `cache_ttl` | 域名缓存 | 600s | 竞速胜出代理复用时长 |
| `stickiness.enabled` | 粘性查找 | false | per-client+domain 钉住同一出口 |
| `max_retries` | 竞速扇出 | 3 | 初始并发候选数 |
| `cost_sort_enabled` | 候选排序 | true | 多目标 Cost 排序；false 回滚纯 EWMA |
| `circuit_threshold` | 熔断 | 3 | 连续失败几次后开路 |
| `single_send_degrade_fail` | 单发降级 | 2 | 粘性/域缓存单发连续失败降级阈值 |
| `api.auth` / `router.auth` | 认证 | 关闭 | 管理 API / 代理端口 HTTP Basic |

## 管理 API 端点（:18080）

| 端点 | 用途 |
|------|------|
| `GET /` | Web 仪表盘 |
| `GET /health` | 健康检查（始终开放） |
| `GET /proxies` / `POST /proxies` | 上游代理列表 CRUD |
| `GET /stats` | 胜出次数 + 总尝试次数 |
| `GET /domains` / `GET /domains/meta` | 域名级胜出统计 / 默认代理 |
| `GET /stickiness` | 粘性表 |
| `GET /quality` / `GET /quality/meta` | 每代理 EWMA / 增强指标（分位、成功率、Cost 分解） |
| `GET /metrics` / `GET /metrics/per-destination` | 全局指标 / 按 (域名, 代理) 指标 |
| `GET /circuit` / `POST /circuit/reset` | 熔断 + 探活状态 / 手动复位 |
| `GET /cost` / `POST /cost` | Cost 参数热更新 |
| `POST /tuner` | 自动调参器启停 |
| `GET /config` | 路由配置快照 |

## 压测

```bash
python -m bench.stress --quick          # ~10s 冒烟
python -m bench.stress                  # 默认 staircase
python -m bench.stress --mode all       # 四种模式
python -m bench.stress --rounds 3       # 多轮均值±标准差
```

## 已知文档

- `README.md` / `README_CN.md`：项目说明、特性、配置、API、Cost 排序与自动调参
- `AGENTS.md`：项目级代理上下文（测试、架构、约定）
- `CODEBUDDY.md`：开发导航（常用命令、指标双族说明、协作模块职责）
- `docs/待办事项_路线图.md`：路线图与已完成里程碑
- `docs/改进方案_响应速度评估.md`：代理响应速度评估改进方案（Phase 1–5 落地记录）
- `docs/代码审计报告.md`：代码审计发现与 2026-09-05 修复状态

## 待办（运维）

- systemd unit 示例缺失（当前仅 docker-compose）
- 与真实 Squid 的集成测试（bench 已支持 `--upstream real`，缺 CI 用例）
