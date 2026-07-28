# auto_squid

智能的 Squid 多代理路由（设计仓库）

状态
- 当前仓库包含详细的设计文档 `DESIGN.md`（架构、数据模型、API、评分与探测策略等）。
- 该仓库目前为设计规范：尚无可运行的实现、无 requirements.txt、无源码目录。

项目简介
- 目标：为分散在远端的多个 Squid 代理提供基于域名的智能路由，根据探测到的延迟、吞吐与可靠性为每次连接选择最优代理。
- 受众：需要按域名路由到最佳出站代理以提升访问体验与稳定性的高级桌面或服务器用户。

Quick start（当前为设计草案）

1. 阅读设计：
   - 主设计文档：DESIGN.md

2. 要实现并运行本项目，你需要：
   - 完整实现源码（router, probe_engine, domain_index, proxy_store, api_server, cli）
   - requirements.txt / pyproject.toml
   - 示例配置文件： `~/.config/auto-squid/config.yaml`, `~/.config/auto-squid/proxies.yaml`

3. 期望运行命令（示例，需实现）：

```bash
# 安装依赖（当 requirements.txt 可用时）
pip install -r requirements.txt

# 添加代理示例（实现后）
auto-squid proxy add "北京节点" 1.2.3.4 3128

auto-squid start
# -> 启动监听 127.0.0.1:10808（路由）和 127.0.0.1:18080（管理 API），并启动探测循环
```

贡献者指南（如何开始实现）
- 我可以帮助 scaffold 初始骨架：pyproject/requirements、包结构、基本的 FastAPI stub、proxy_store 和 domain_index 的最小实现。告诉我你希望我先实现哪个组件（router、probe_engine、api 或 CLI）。

更多信息
- 详见 DESIGN.md（包含架构图、数据模型、评分算法草案、数据库 schema 与配置示例）。

