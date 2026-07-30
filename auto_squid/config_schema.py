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
    id: str
    name: Optional[str]
    host: str
    port: int = Field(3128)
    protocol: str = Field("http")
    auth: Optional[Dict[str, str]] = None
    enabled: bool = Field(True)
    tags: Optional[Dict[str, str]] = None


class RouterConfig(BaseModel):
    cache_ttl: int = Field(600, description="seconds before domain cache expires (default 10 min)")
    enable_local_racing: bool = Field(False, description="include local machine as a racing proxy node")

class Config(BaseModel):
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    proxies: Optional[List[ProxyInfo]] = None
