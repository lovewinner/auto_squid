import asyncio
import uvicorn
import typer
from typing import Optional

from .api import app as fastapi_app
from .probe_engine import ProbeEngine

app = typer.Typer()


@app.command()
def start(host: str = "127.0.0.1", port: int = 10808, api_host: str = "127.0.0.1", api_port: int = 18080, reload: bool = False):
    """Start router and API (minimal scaffold)."""
    probe = ProbeEngine()

    async def _main():
        # start probe loop in background
        asyncio.create_task(probe.run_loop())
        # start API server (uvicorn)
        config = uvicorn.Config(fastapi_app, host=api_host, port=api_port, log_level="info", reload=reload)
        server = uvicorn.Server(config)
        await server.serve()

    asyncio.run(_main())


if __name__ == "__main__":
    app()
