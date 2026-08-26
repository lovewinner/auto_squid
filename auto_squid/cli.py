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

    #13:YAML 缺失/语法错/键位错配(#12 的 extra="forbid")统一打印可读的
    错误并退出码 2 —— 不再抛裸 traceback(运维面对的是配置文件,不是 Python)。
    """
    try:
        if config_path:
            return Config(**yaml.safe_load(Path(config_path).read_text()))
        default_yaml = Path("config.yaml")
        if default_yaml.exists():
            return Config(**yaml.safe_load(default_yaml.read_text()))
        return Config()
    except FileNotFoundError as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(2)
    except yaml.YAMLError as e:
        print(f"config error: bad YAML: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        # pydantic ValidationError(extra 键 / 跨字段校验失败)也落到这里。
        _ = str(e).splitlines()[:12]
        print("config error: invalid configuration:", file=sys.stderr)
        for line in _:
            print(f"  {line}", file=sys.stderr)
        sys.exit(2)


def setup_logging(cfg: Config):
    """配置根日志:控制台只输出 WARNING 及以上,文件按 cfg.logging.level。

    控制台级别刻意压低(代理转发量大,INFO 会刷屏);文件 handler 级别由
    `cfg.logging.level` 控制(#13:此前硬编码 INFO,设 DEBUG 没反应)。默认
    INFO,行为不变;想开 per-request 调试日志,配置 logging.level: DEBUG 即
    会同时提升文件级别与 auto_squid logger,两者一致。
    """
    root = logging.getLogger()
    # 根 logger 设为 DEBUG,让各 handler 各自按级别过滤。
    root.setLevel(logging.DEBUG)

    # 控制台:WARNING 起(不受 logging.level 影响,转发量大 INFO 会刷屏),精简格式。
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.WARNING)
    console.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    root.addHandler(console)

    # 文件:级别随 cfg.logging.level(#13),带时间戳,追加模式。default INFO。
    level = getattr(logging, cfg.logging.level.upper(), logging.INFO)
    log_path = cfg.logging.file or "auto_squid.log"
    fh = logging.FileHandler(log_path, mode="a")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(name)s:%(message)s"))
    root.addHandler(fh)

    # 压制 httpx/uvicorn 等依赖库的嘈杂输出(它们默认 INFO 会刷屏)。
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.ERROR)
    # router 的每请求日志(client connected / cache hit / racing win)已降级为
    # DEBUG。auto_squid logger 级别跟随 cfg.logging.level:默认 INFO 时 DEBUG
    # 在 logger 层即被短路,不构造格式化参数(250 rps 下每秒数百次 log 调用的
    # 隐藏成本由此消除);配 DEBUG 则打开 per-request 调试,与文件级别一致。
    logging.getLogger("auto_squid").setLevel(level)


app = typer.Typer()


@app.callback(invoke_without_command=True)
def start(config: str = "", proxies: str = "", db: str = "auto_squid.db"):
    """启动代理路由器和 API 服务。支持可选的 config.yaml / proxies.yaml 参数。"""
    cfg = _load_config(config)
    setup_logging(cfg)
    # 加载上游代理列表(未指定 --proxies 时用当前目录 proxies.yaml)。
    proxy_store = ProxyStore(proxies if proxies else "proxies.yaml")
    # 构造 Router 并注入配置(#15:整块 RouterConfig 传入,内部读取,删掉手工映射)。
    router = Router(
        proxy_store,
        listen_host=cfg.listen.host, listen_port=cfg.listen.port,
        db_path=db,
        router_cfg=cfg.router,
    )
    # 把 store/router 注入 FastAPI app 的模块级全局,供各端点使用。
    # api.auth 控制管理 API 的 HTTP Basic 认证(默认关闭)。
    mount_api(proxy_store, router, api_auth=cfg.api.auth)

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
