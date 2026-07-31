"""配置模型定义(基于 pydantic v2)。

所有模型均带默认值,因此缺失配置也能以合理默认行为启动。配置可通过
`config.yaml`(`--config` 传入)加载,结构为顶层各子配置块:

    listen:  代理监听地址/端口
    api:     管理 API 监听地址/端口
    router:  路由行为(竞速、缓存、客户端认证)
    logging: 日志级别/文件

`ProxyInfo` 是上游代理节点定义,由 `proxies.yaml` 加载(见 ProxyStore)。
"""

from typing import Dict, Optional
from pydantic import BaseModel, Field


class ListenConfig(BaseModel):
    """代理端口的监听配置(面向客户端的 HTTP/CONNECT 代理端口)。"""
    host: str = Field("0.0.0.0")
    port: int = Field(10808)


class APIConfig(BaseModel):
    """管理 API 的监听配置(独立于代理端口,默认 18080)。

    注意:管理 API 不受客户端认证保护,生产环境需用防火墙限制访问。
    """
    host: str = Field("0.0.0.0")
    port: int = Field(18080)


class LoggingConfig(BaseModel):
    """日志配置。`file` 为 None 时写默认文件 `auto_squid.log`。"""
    level: str = Field("INFO")
    file: Optional[str] = Field(None)


class ProxyInfo(BaseModel):
    """单个上游代理节点的配置。

    `auth` 是访问**该上游代理**所需的凭据(上游认证),会被拼进 httpx 的
    代理 URL 或 CONNECT 的 Proxy-Authorization;与客户端认证(auth.py)
    是两套独立机制。`tags` 预留给策略引擎使用(当前未启用)。
    """
    id: str
    name: Optional[str] = None
    host: str
    port: int = Field(3128)
    protocol: str = Field("http")
    auth: Optional[Dict[str, str]] = None
    enabled: bool = Field(True)
    tags: Optional[Dict[str, str]] = None


class AuthConfig(BaseModel):
    """客户端访问代理端口所需的 HTTP Basic 认证配置。

    默认 `enabled=False`(开放代理),开启后客户端每个请求都需带
    `Proxy-Authorization` 头(见 auth.check_auth)。
    """
    enabled: bool = Field(False, description="要求客户端通过 HTTP Basic 认证")
    username: str = Field("")
    password: str = Field("")


class RouterConfig(BaseModel):
    """路由行为配置。

    cache_ttl:           域名缓存有效期(秒)。某代理为某域名竞速胜出后,
                         在该有效期内复用同一代理,避免每请求都竞速。
    enable_local_racing: 让网关主机自身作为代理节点直接参与竞速(不走上游)。
    max_retries:         竞速首批并行的代理数量;全失败后对剩余代理再竞速兜底。
    auth:                客户端认证配置(AuthConfig)。
    """
    cache_ttl: int = Field(600, description="域名缓存有效期(秒)，过期后重新竞速")
    enable_local_racing: bool = Field(False, description="将本机作为代理节点参与竞速")
    max_retries: int = Field(3, description="竞速首批并行的代理数量")
    auth: AuthConfig = Field(default_factory=AuthConfig, description="客户端认证配置")


class Config(BaseModel):
    """顶层配置,各字段均有默认值。"""
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
