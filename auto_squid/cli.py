import asyncio
import uvicorn
import typer
import logging
import sys
from pathlib import Path

from .proxy_store import ProxyStore
from .router import Router
from .api import mount as mount_api
from .config_schema import Config


def setup_logging(cfg: Config):
    """控制台只输出 WARNING 级别，文件日志输出 INFO 级别"""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.WARNING)
    console.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    root.addHandler(console)

    log_path = cfg.logging.file or "auto_squid.log"
    fh = logging.FileHandler(log_path, mode="a")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(name)s:%(message)s"))
    root.addHandler(fh)

    # 压制 httpx/uvicorn 的控制台输出
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.ERROR)


app = typer.Typer()


@app.callback(invoke_without_command=True)
def start(config: str = "", proxies: str = "", db: str = "auto_squid.db"):
    """启动代理路由器和 API 服务。支持可选的 config.yaml / proxies.yaml 参数。"""
    cfg = None
    # 优先使用命令行指定 config 路径，其次尝试当前目录 config.yaml
    if config:
        import yaml
        cfg = Config(**yaml.safe_load(Path(config).read_text()))
    elif Path("config.yaml").exists():
        import yaml
        cfg = Config(**yaml.safe_load(Path("config.yaml").read_text()))
    else:
        cfg = Config()
    setup_logging(cfg)
    proxy_store = ProxyStore(proxies if proxies else "proxies.yaml")
    router = Router(proxy_store, listen_host=cfg.listen.host, listen_port=cfg.listen.port, db_path=db, cache_ttl=cfg.router.cache_ttl, enable_local_racing=cfg.router.enable_local_racing)
    mount_api(proxy_store, router)

    async def _main():
        await router.start()
        # 后台启动 FastAPI 服务
        config_uv = uvicorn.Config("auto_squid.api:app", host=cfg.api.host, port=cfg.api.port, log_level="info")
        server = uvicorn.Server(config_uv)
        api_task = asyncio.create_task(server.serve())
        try:
            await api_task
        finally:
            await router.stop()

    asyncio.run(_main())


if __name__ == '__main__':
    app()
