from typing import Dict, Optional, List
from pydantic import BaseModel, Field


class ListenConfig(BaseModel):
    host: str = Field("0.0.0.0")
    port: int = Field(10808)


class APIConfig(BaseModel):
    host: str = Field("0.0.0.0")
    port: int = Field(18080)
    bind_remote: bool = Field(True)


class LoggingConfig(BaseModel):
    level: str = Field("INFO")
    file: Optional[str] = Field(None)


class ProxyInfo(BaseModel):
    """单个上游代理节点的配置"""
    id: str
    name: Optional[str]
    host: str
    port: int = Field(3128)
    protocol: str = Field("http")
    auth: Optional[Dict[str, str]] = None
    enabled: bool = Field(True)
    tags: Optional[Dict[str, str]] = None


class RouterConfig(BaseModel):
    cache_ttl: int = Field(600, description="域名缓存有效期(秒)，过期后重新竞速")
    enable_local_racing: bool = Field(False, description="将本机作为代理节点参与竞速")

class Config(BaseModel):
    """顶层配置，各字段均有默认值"""
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    proxies: Optional[List[ProxyInfo]] = None
