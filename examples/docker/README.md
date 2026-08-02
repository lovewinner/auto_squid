# auto_squid 容器化示例（Docker / docker compose）

将 auto_squid 打包为镜像,可一键启动、挂载数据卷、接入真实上游。本目录文件:

| 文件 | 说明 |
|------|------|
| `../Dockerfile` | 多阶段镜像(uv 构建依赖 → slim 运行时,非 root 运行) |
| `docker-compose.yml` | compose v2 编排:端口、数据卷、健康检查、重启策略 |
| `../.dockerignore` | 构建上下文排除(数据/日志/配置/venv 不进镜像) |

## 快速开始

在仓库根目录执行:

```bash
docker compose -f examples/docker/docker-compose.yml build
docker compose -f examples/docker/docker-compose.yml up -d
```

验证:

```bash
curl http://127.0.0.1:18080/health          # → {"status":"ok"}
curl -x http://127.0.0.1:10808 http://www.baidu.com   # 走占位上游(会失败,见下)
```

浏览器打开仪表盘: <http://127.0.0.1:18080/>

## 接入真实上游代理

默认镜像内是占位上游(仅供自举验证)。两种方式接入真实代理:

**方式 A(推荐)——挂载你自己的 `proxies.yaml`:**

```bash
mkdir -p ~/auto_squid
cp examples/proxies.yaml ~/auto_squid/proxies.yaml   # 填入真实地址/凭据
```

编辑 `examples/docker/docker-compose.yml`,启用挂载并让 CLI 指向它:

```yaml
volumes:
  - ~/auto_squid/proxies.yaml:/proxies/proxies.yaml:ro
command: ["--proxies", "/proxies/proxies.yaml", "--db", "/data/auto_squid.db"]
```

再 `docker compose up -d --build` 重建容器。无需重新构建镜像即可改上游。

**方式 B——构建时注入(id 列表,无认证):**

```yaml
build:
  args:
    AUTO_SQUID_PROXY_IDS: squid-01,squid-02
```

> 注意:方式 B 生成的代理文件不含 auth 凭据(仅 id/name/host/port/
> protocol/enabled),且改变节点后需 `--build` 重新构建;需要上游认证或
> 频繁改节点时用方式 A。

## 数据与日志

- SQLite 统计(`auto_squid.db`)持久化在 `./data` 挂载卷,`docker compose down`
  不会删除;`docker compose down -v` 才会清空。
- 容器内日志走 stdout/stderr,`docker compose logs -f` 查看。

## 与真实 Squid 集成

把镜像里的 `host: 10.14.25.86` 等占位上游换成你的 Squid 节点后,请求会按
域名竞速转发到最优上游,`/metrics` 服务端计数器(缓存命中率/竞速放大率)在
容器内同样可用。压测请用仓库的 `bench/`(见 `README_CN.md` 性能压测一节),
镜像本身不含 bench 依赖。

## 停止

```bash
docker compose -f examples/docker/docker-compose.yml down        # 停止,保留数据
docker compose -f examples/docker/docker-compose.yml down -v     # 停止并删数据卷
```
