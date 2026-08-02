"""压测用的受控上游代理集群(mock upstream proxies)。

本模块提供一组"行为像真实 HTTP/HTTPS 代理"的本地 asyncio 服务器,作为
auto_squid 的上游。目的是让压测**可控、可重复、可归因**:延迟、响应大小、
传输方式(chunked/分块间隔)、失败率都由配置决定,排除真实网络抖动。

每个上游实例还带一个**命中计数器**(hit counter):记录它实际收到并处理的
请求数。压测据此算出两个关键准确性指标:

- **真实缓存命中率** = (客户端请求数 - 上游总命中数) / 客户端请求数
  (域名缓存 + 响应缓存命中的请求根本不触达上游)
- **racing 放大率** = 上游总命中数 / 客户端请求数
  (竞速会把一个客户端请求扇出到多个上游;放大率反映竞速开销)

设计要点(为求可靠,采用"一连接一请求",不复用连接):
- HTTP 路径:解析绝对 URL 请求行(代理语义),按 host 套用 response profile,
  返回 200 + body;读完后即关闭连接。连接复用由 auto_squid 的 httpx 连接池
  在它那一侧处理,mock 侧只需忠实响应与计数。
- CONNECT 路径:回 200 + 回显隧道(echo),用于测隧道吞吐。
- response profile 按 host 匹配(前缀),支持:首字节延迟、body 大小、
  是否 chunked、分块间隔、失败率(返回 502)。
- 每个实例有 base_delay 偏移,模拟"快/中/慢/不稳定"代理,让 racing
  的"首字节判胜"真正有区分度(快的恒先到)。
"""

import asyncio
import logging
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ResponseProfile:
    """按 host 前缀匹配的响应画像。

    Attributes:
        host_prefix: 匹配 host 的前缀(小写)。如 "hot" 匹配 hot1.example.com。
        first_byte_delay: 发送响应头前的延迟(秒),模拟上游处理 + 网络首字节延迟。
        body_size: 响应 body 字节数。
        chunked: True 则用 Transfer-Encoding: chunked(无 Content-Length),
            否则发 Content-Length(测流式两条定界路径)。
        chunk_delay: 分块发送间隔(秒);0 表示一次性发出。
        fail_rate: 0~1,以该概率失败(返回 502);测竞速容错与兜底批次。
    """
    host_prefix: str
    first_byte_delay: float = 0.05
    body_size: int = 1024
    chunked: bool = False
    chunk_delay: float = 0.0
    fail_rate: float = 0.0


def _default_profile() -> ResponseProfile:
    return ResponseProfile(host_prefix="", first_byte_delay=0.05,
                           body_size=1024, chunked=False)


@dataclass
class MockUpstream:
    """单个 mock 上游代理实例。

    base_delay 叠加到每个响应的 first_byte_delay 上,让不同实例有稳定
    的快慢差异——这对"首字节判胜"压测至关重要:慢代理恒后到,竞速结果可预测。
    hit_count 累计该实例成功响应的请求数;connect_count 累计 CONNECT 隧道数。
    供压测算放大率/命中率。
    """
    host: str
    port: int
    base_delay: float = 0.0
    profiles: list = field(default_factory=list)
    server: Optional[asyncio.AbstractServer] = None
    hit_count: int = 0
    connect_count: int = 0
    # 新建连接计数(每接受一个连接 +1)。压测连接复用场景据此算 keepalive
    # 复用率 = (请求数 - 新建连接数) / 请求数;>0 表示连接被复用。
    new_conn_count: int = 0

    def profile_for(self, host_header: str) -> ResponseProfile:
        """按 host 选响应画像;无匹配则用默认。"""
        h = (host_header or "").lower()
        for p in self.profiles:
            if h.startswith(p.host_prefix):
                return p
        return _default_profile()


async def _read_request_head(reader) -> tuple[str, dict, bytes]:
    """读请求首行 + 头部,返回 (first_line, headers_dict_lower, leftover_body_indicator)。

    leftover 不在此读(对压测足够:GET 无 body,POST body 也很小且我们不校验)。
    """
    line = await reader.readline()
    if not line:
        return "", {}, b""
    first = line.decode('latin-1').strip()
    headers = {}
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
        try:
            k, v = h.decode('latin-1').split(':', 1)
            headers[k.strip().lower()] = v.strip()
        except ValueError:
            pass
    return first, headers, b""


def _host_from(first: str, headers: dict) -> str:
    """从绝对 URL 或 Host 头取 host。"""
    parts = first.split(' ')
    target = parts[1] if len(parts) > 1 else ""
    if target.lower().startswith('http'):
        return urllib.parse.urlparse(target).hostname or headers.get('host', '')
    return headers.get('host', '')


async def _send_body(writer, body: bytes, chunked: bool, chunk_delay: float):
    """按 chunked 或 content-length 发送 body(分块 + 间隔)。"""
    if chunked:
        step = 65536
        for i in range(0, len(body), step):
            piece = body[i:i + step]
            writer.write(f"{len(piece):X}\r\n".encode())
            writer.write(piece)
            writer.write(b"\r\n")
            await writer.drain()
            if chunk_delay:
                await asyncio.sleep(chunk_delay)
        writer.write(b"0\r\n\r\n")
        await writer.drain()
    else:
        step = 65536
        for i in range(0, len(body), step):
            writer.write(body[i:i + step])
            await writer.drain()
            if chunk_delay:
                await asyncio.sleep(chunk_delay)


async def _handle(reader, writer, upstream: MockUpstream):
    """单连接处理:CONNECT → 隧道回显;否则 → HTTP 代理响应。一连接一请求。"""
    # 每接受一个新连接 +1,供连接复用压测算 keepalive 复用率。
    upstream.new_conn_count += 1
    try:
        first, headers, _ = await _read_request_head(reader)
        if not first:
            return
        if first.upper().startswith('CONNECT'):
            upstream.connect_count += 1
            await asyncio.sleep(upstream.base_delay)
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            try:
                while True:
                    data = await asyncio.wait_for(reader.read(4096), timeout=10)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
            except (asyncio.TimeoutError, Exception):
                pass
            return
        # HTTP 代理路径
        host = _host_from(first, headers)
        prof = upstream.profile_for(host)
        total_delay = prof.first_byte_delay + upstream.base_delay
        if total_delay:
            await asyncio.sleep(total_delay)
        if prof.fail_rate > 0:
            import random
            if random.random() < prof.fail_rate:
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                await writer.drain()
                return
        upstream.hit_count += 1
        body = b"x" * prof.body_size
        writer.write(b"HTTP/1.1 200 OK\r\n")
        writer.write(b"Content-Type: application/octet-stream\r\n")
        if prof.chunked:
            writer.write(b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
            await writer.drain()
            await _send_body(writer, body, True, prof.chunk_delay)
        else:
            writer.write(f"Content-Length: {len(body)}\r\n".encode())
            writer.write(b"Connection: close\r\n\r\n")
            await writer.drain()
            await _send_body(writer, body, False, prof.chunk_delay)
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    except Exception:
        logger.debug("mock upstream handler error", exc_info=True)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


class UpstreamCluster:
    """管理一组 mock 上游的生命周期。

    start() 启动所有实例,start_port 起连续分配端口;stop() 关闭并等待。
    total_hits()/total_connects() 汇总计数,供压测算命中率/放大率。
    """

    def __init__(self, specs: list, start_port: int = 31000):
        """specs: [(base_delay, [ResponseProfile, ...]), ...]"""
        self.upstreams: list[MockUpstream] = []
        port = start_port
        for base_delay, profiles in specs:
            self.upstreams.append(MockUpstream(
                host="127.0.0.1", port=port, base_delay=base_delay,
                profiles=profiles))
            port += 1

    async def start(self):
        for u in self.upstreams:
            u.server = await asyncio.start_server(
                lambda r, w, uu=u: _handle(r, w, uu), host=u.host, port=u.port)
        return self

    async def stop(self):
        for u in self.upstreams:
            if u.server:
                u.server.close()
                try:
                    await u.server.wait_closed()
                except Exception:
                    pass

    def total_hits(self) -> int:
        return sum(u.hit_count for u in self.upstreams)

    def total_connects(self) -> int:
        return sum(u.connect_count for u in self.upstreams)

    def total_new_conns(self) -> int:
        return sum(u.new_conn_count for u in self.upstreams)

    def reset_counts(self):
        for u in self.upstreams:
            u.hit_count = 0
            u.connect_count = 0
            u.new_conn_count = 0
