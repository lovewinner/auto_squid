"""上游建连的 DNS 安全路径:先自己解析、再连数值地址。

Router 与 ConnectionPools 都要连"可能写成域名"的地址:代理节点的
`ProxyInfo.host` 允许是域名,本地直连路径的目标更是客户端给的任意域名。
两处若各自 `asyncio.open_connection(host, ...)`,在生产用的 uvloop 上都会
踩同一个坑(见 `open_resolved` 的 docstring),所以统一到这一个函数上。
"""

import asyncio
import ipaddress
import logging
import socket

logger = logging.getLogger(__name__)


def is_ip_literal(host: str) -> bool:
    """host 是否已是数值地址(IPv4/IPv6 字面量,允许 IPv6 的方括号写法)。"""
    candidate = host.partition('[')[2].partition(']')[0] if host.startswith('[') else host
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return False
    return True


async def open_resolved(host: str, port: int, timeout: float,
                        *, phase: str, pid: str = '-', target: str = ''):
    """自己解析 DNS 再连数值地址(避免 uvloop 内部 DNS future 变孤儿)。

    背景:生产用 uvloop,其 `create_connection()` 对**域名**走内部
    `_getaddrinfo` future(uvloop loop.pyx:1747);外层 task 被取消(竞速败者
    / `wait_for` 超时 / stop() 取消预热)时该 future 与等待者解绑 → 稍后 DNS 失败
    回调 `set_exception(gaierror)` → 无人 await → asyncio 在 `Future.__del__`
    报 "Future exception was never retrieved"。实测 12.5 天 231 条,
    全部 gaierror。IP 字面量走 `__static_getaddrinfo` 快路径(loop.pyx:1940)
    不做 DNS,故不受影响。

    修法:改用 `loop.getaddrinfo` **公共 API** —— 它直接 await 内部 future
    (loop.pyx:1529),取消可传播到该 future → future 被 cancel → 回调里
    `if not fut.cancelled()` 跳过 set_exception → 不产生孤儿。随后连已解析
    的数值地址,uvloop 走静态快路径,结构上不可能再产生孤儿。

    语义保持不变:
    - 总超时预算不变(DNS + 连接共用同一 deadline);
    - 多地址回退保留(遍历 getaddrinfo 全部结果,顺序同 libc);
    - DNS 失败原样抛出 —— `socket.gaierror` 是 `OSError` 子类,故调用方
      既有的 `except (asyncio.TimeoutError, OSError, ConnectionError)`
      分支与 `ERROR_CONNECT` 错误归类完全不变;但此处**补一条带上下文的
      日志**(此前该失败完全不可见,只能看到匿名的 asyncio 告警)。
    """
    if is_ip_literal(host):
        # 数值地址:直接连。uvloop 对它走静态 getaddrinfo 快路径(不做 DNS 查询),
        # 结构上不可能产生孤儿;省掉一次线程池 getaddrinfo 往返,预热/竞速热路径
        # 的建连延迟与改动前一致。
        return await asyncio.wait_for(asyncio.open_connection(host, port),
                                      timeout=timeout)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        logger.info("dns FAILED phase=%s host=%s:%s pid=%s target=%s err=%s: %s",
                    phase, host, port, pid, target, type(e).__name__, e)
        raise
    last = None
    for _family, _type, _proto, _canon, sockaddr in infos:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise asyncio.TimeoutError()
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(sockaddr[0], sockaddr[1]),
                timeout=remaining)
        except (asyncio.TimeoutError, OSError, ConnectionError) as e:
            last = e          # 多地址回退:换下一个地址重试
    if last is not None:
        raise last
    raise OSError("getaddrinfo() returned empty list")
