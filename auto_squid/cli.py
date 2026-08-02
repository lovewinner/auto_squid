"""命令行入口(Typer)。

默认行为(无子命令)是启动代理路由器 + 管理 API 服务。支持通过可选参数
指定 `config.yaml`(路由/认证/日志等配置)与 `proxies.yaml`(上游代理列表)。

入口点在 `pyproject.toml` 注册为 `auto-squid` 脚本,也可用
`python -m auto_squid.cli` 运行。
"""

import asyncio
import logging
import sys
from pathlib import Path

import typer
import uvicorn
import yaml

# uvloop 以约 2× 加速 asyncio 事件循环(任务调度、socket I/O),显著降低
# CPU 开销与 P99 延时。已随 uvicorn[standard] 间接安装,此处用 try 兜底。
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from .proxy_store import ProxyStore
from .router import Router
from .api import mount as mount_api
from .config_schema import Config


def _load_config(config_path: str) -> Config:
    """加载配置:显式 --config > 当前目录 config.yaml > 全默认 Config()。

    用 YAML 文件时按顶层键构造 Config(缺省字段走默认值)。
    """
    if config_path:
        return Config(**yaml.safe_load(Path(config_path).read_text()))
    default_yaml = Path("config.yaml")
    if default_yaml.exists():
        return Config(**yaml.safe_load(default_yaml.read_text()))
    return Config()


def setup_logging(cfg: Config):
    """配置根日志:控制台只输出 WARNING 及以上,文件输出 INFO 及以上。

    之所以压低控制台级别:代理转发量大,INFO 会刷屏;但文件保留 INFO
    便于事后排查(竞速命中、认证拒绝等)。
    """
    root = logging.getLogger()
    # 根 logger 设为 DEBUG,让各 handler 各自按级别过滤。
    root.setLevel(logging.DEBUG)

    # 控制台:WARNING 起,精简格式,只看错误与警告。
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.WARNING)
    console.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    root.addHandler(console)

    # 文件:INFO 起,带时间戳,追加模式。cfg.logging.file 为 None 时用默认文件名。
    log_path = cfg.logging.file or "auto_squid.log"
    fh = logging.FileHandler(log_path, mode="a")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(name)s:%(message)s"))
    root.addHandler(fh)

    # 压制 httpx/uvicorn 等依赖库的嘈杂输出(它们默认 INFO 会刷屏)。
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.ERROR)
    # router 的每请求日志(client connected / cache hit / racing win)已降级为
    # DEBUG。把 auto_squid logger 置于 INFO,DEBUG 在 logger 层即被短路,不再
    # 构造格式化参数——250 rps 下每秒数百次 log 调用的隐藏成本由此消除。
    # 启动(Router listening)与认证拒绝仍为 INFO,文件里保留审计轨迹。
    logging.getLogger("auto_squid").setLevel(logging.INFO)


app = typer.Typer()


@app.callback(invoke_without_command=True)
def start(config: str = "", proxies: str = "", db: str = "auto_squid.db"):
    """启动代理路由器和 API 服务。支持可选的 config.yaml / proxies.yaml 参数。"""
    cfg = _load_config(config)
    setup_logging(cfg)
    # 加载上游代理列表(未指定 --proxies 时用当前目录 proxies.yaml)。
    proxy_store = ProxyStore(proxies if proxies else "proxies.yaml")
    # 构造 Router 并注入配置;客户端认证参数从 cfg.router.auth 取。
    router = Router(
        proxy_store,
        listen_host=cfg.listen.host, listen_port=cfg.listen.port,
        max_retries=cfg.router.max_retries, db_path=db,
        cache_ttl=cfg.router.cache_ttl,
        enable_local_racing=cfg.router.enable_local_racing,
        auth_enabled=cfg.router.auth.enabled,
        auth_username=cfg.router.auth.username,
        auth_password=cfg.router.auth.password,
    )
    # 把 store/router 注入 FastAPI app 的模块级全局,供各端点使用。
    mount_api(proxy_store, router)

    async def _main():
        # 先启动代理端口(接受客户端 HTTP/CONNECT),再后台跑管理 API。
        await router.start()
        config_uv = uvicorn.Config("auto_squid.api:app", host=cfg.api.host, port=cfg.api.port, log_level="info")
        server = uvicorn.Server(config_uv)
        api_task = asyncio.create_task(server.serve())
        try:
            # 阻塞在 API 服务上;API 退出(被信号中断等)后才停止路由器。
            await api_task
        finally:
            # 优雅关闭:停止接受新连接、取消在途连接、关闭 DB(见 router.stop)。
            await router.stop()

    asyncio.run(_main())


if __name__ == '__main__':
    app()
