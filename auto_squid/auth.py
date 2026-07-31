"""客户端访问代理端口的 HTTP Basic 认证。

本模块只负责"客户端 → 本代理"这一跳的凭据校验,与上游代理自身的认证
(见 router._build_proxy_url / _try_tunnel)是两套独立机制。

校验流程:
1. 读取请求头中的 `Proxy-Authorization`(代理标准头),缺失时回退到
   `Authorization`(兼容部分客户端)。
2. 解析 `Basic base64(用户名:密码)` 格式,base64 解码后拆出用户名与密码。
3. 用 `hmac.compare_digest` 做常量时间比较,避免计时侧信道泄漏凭据差异。
4. 任何格式异常(base64 解码失败、缺冒号等)统一返回失败并记录日志。
"""

from __future__ import annotations

import base64
import hmac
import logging

logger = logging.getLogger(__name__)


def check_auth(headers: dict[str, str], auth_enabled: bool,
               expected_username: str, expected_password: str) -> tuple[bool, str | None]:
    """校验客户端的 HTTP Basic 凭据是否匹配预期值。

    参数:
        headers:          客户端请求头的字典(键保留原始大小写,本函数用
                          固定键名 `Proxy-Authorization` / `Authorization` 读取,
                          因此调用方需保证这两个键的大小写与客户端一致)。
        auth_enabled:     是否启用认证。为 False 时直接放行(开放代理)。
        expected_username:预期的用户名。
        expected_password:预期的密码。

    返回:
        (ok, reason)。ok 为 True 表示通过,reason 为 None;
        ok 为 False 时 reason 是简短失败原因,可回写给客户端作为 407 body。
    """
    # 未启用认证 → 放行。这让 auth_enabled=False 时行为与历史完全一致。
    if not auth_enabled:
        return True, None

    # 代理场景的标准头是 Proxy-Authorization;部分客户端(或普通 HTTP 请求)
    # 只带 Authorization,作为回退也接受。
    auth_header = headers.get('Proxy-Authorization') or headers.get('Authorization')
    if not auth_header:
        # 没有任何认证头 → 提示需要认证(触发客户端补发凭据)。
        return False, "Authentication required"
    try:
        # 格式: "Basic base64(username:password)",按首个空格拆出方案与凭据。
        auth_type, auth_info = auth_header.split(' ', 1)
        if auth_type.lower() != 'basic':
            # 仅支持 Basic 方案(Bearer/Digest 等不处理)。
            return False, "Unsupported auth type"
        # base64 解码为字符串,再按首个冒号拆出用户名与密码。
        decoded = base64.b64decode(auth_info).decode('utf-8')
        username, password = decoded.split(':', 1)
        # 用常量时间比较,避免通过响应耗时推断用户名/密码的逐字符差异。
        if hmac.compare_digest(username, expected_username) and hmac.compare_digest(password, expected_password):
            return True, None
        else:
            return False, "Authentication failed"
    except Exception as e:
        # base64 解码失败、缺冒号、缺空格等任意格式异常都归为格式错误,
        # 记录日志便于排查,但不向客户端暴露具体异常细节。
        logger.error(f"Auth parse error: {e}")
        return False, "Auth format error"
