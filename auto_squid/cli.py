import asyncio
import uvicorn
import typer
import logging
from pathlib import Path

from .proxy_store import ProxyStore
from .probe_engine import ProbeEngine
from .router import Router
from .api import mount as mount_api
from .config_schema import Config

app = typer.Typer()


@app.callback(invoke_without_command=True)
def start(config: str = "", proxies: str = ""):
    """Start API and proxy router. Optionally pass config YAML and proxies YAML paths."""
    logging.basicConfig(level=logging.INFO)
    cfg = None
    if config:
        import yaml
        cfg = Config(**yaml.safe_load(Path(config).read_text()))
    else:
        cfg = Config()
    proxy_store = ProxyStore(proxies if proxies else "proxies.yaml")
    probe_engine = ProbeEngine(proxy_store, probe_cfg=cfg.probe, score_cfg=cfg.score)
    mount_api(proxy_store, probe_engine)
    router = Router(proxy_store, probe_engine, listen_host=cfg.listen.host, listen_port=cfg.listen.port)

    async def _main():
        # start probe loop
        probe_task = asyncio.create_task(probe_engine.run_loop())
        await router.start()
        # start API server in background
        config_uv = uvicorn.Config("auto_squid.api:app", host=cfg.api.host, port=cfg.api.port, log_level="info")
        server = uvicorn.Server(config_uv)
        api_task = asyncio.create_task(server.serve())
        try:
            await api_task
        finally:
            probe_engine.stop()
            probe_task.cancel()
            await asyncio.gather(probe_task, return_exceptions=True)
            await router.stop()

    asyncio.run(_main())


if __name__ == '__main__':
    app()
