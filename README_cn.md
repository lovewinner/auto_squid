# auto_squid（中文说明）

轻量级 MVP：基于域名的出站代理选择工具。

概述
- 在网关主机（B）运行，接受来自客户端（A）的 HTTP/HTTPS 代理流量，并将每次请求转发到按域名选出的最优出站代理（可部署在 B/C/D）。
- 周期性探测配置的代理，基于延迟、吞吐与可靠性计算分数以驱动选择。

功能
- 支持 HTTP 与 HTTPS（CONNECT）转发，通过选定的上游代理进行访问
- 探测引擎：TCP 连接 + HTTP GET，测量吞吐、IQR 异常过滤、时间衰减打分
- 运行时 ProxyStore（内存 + 可选 YAML 加载/保存），提供管理 API CRUD
- 管理 API：/health、/proxies、/score、/probe/status、/probe/history、/probe/states、/metrics
- CLI：启动路由器、探测循环和管理 API

快速开始（本机）
1. 创建虚拟环境（推荐使用 uv）：
   uv venv .venv --seed && uv sync

2. 准备 proxies YAML（示例）：

```yaml
- id: squid-beijing-01
  name: beijing-01
  host: 10.14.25.86
  port: 3128
  protocol: http
  enabled: true
```

3. 启动服务：
   python -m auto_squid.cli start --config ./config.yaml --proxies ./proxies.yaml

4. 验证：
   curl http://127.0.0.1:18080/health
   curl http://127.0.0.1:18080/score

使用说明
- 在客户端 A 配置 B:10808 为 HTTP/HTTPS 代理（或将流量重定向到该端口）。
- Router 将请求转发到所选上游代理；上游代理必须支持 CONNECT（用于 HTTPS）。

限制与注意
- HTTP 解析为 MVP 级别，未支持完整持久连接或复杂分块传输；适合原型和测试。
- 管理 API 未提供鉴权，请用防火墙或在可信网络中使用，或在投入生产前添加认证。
- 打分与探测为启发式实现，需根据生产环境进行调整与扩展。

开发
- 测试：pytest（包含 end-to-end mock 测试）
- 主要模块：auto_squid/router.py、probe_engine.py、proxy_store.py、api.py、cli.py

后续计划
- 完善 HTTP 流式与持久连接支持
- 为管理 API 增加鉴权并导出 Prometheus 指标
- 增加与 Squid 的集成测试与容器部署示例

许可证: MIT
