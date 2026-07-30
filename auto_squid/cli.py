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

    # keep httpx quiet on console
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.ERROR)


app = typer.Typer()


@app.callback(invoke_without_command=True)
def start(config: str = "", proxies: str = "", db: str = "auto_squid.db"):
    """Start API and proxy router. Optionally pass config YAML and proxies YAML paths."""
    cfg = None
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
        # start API server in background
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
