from __future__ import annotations

import base64
import hmac
import logging

logger = logging.getLogger(__name__)


def check_auth(headers: dict[str, str], auth_enabled: bool,
               expected_username: str, expected_password: str) -> tuple[bool, str | None]:
    """Validate a client's HTTP Basic credentials against the expected pair.

    Returns (ok, reason). reason is None on success, a short explanation on
    failure. When auth_enabled is False the check is bypassed (open proxy).
    Uses hmac.compare_digest for constant-time comparison.
    """
    if not auth_enabled:
        return True, None

    # Prefer Proxy-Authorization (proxy standard header), fallback to Authorization
    auth_header = headers.get('Proxy-Authorization') or headers.get('Authorization')
    if not auth_header:
        return False, "Authentication required"
    try:
        # Format: "Basic base64(username:password)"
        auth_type, auth_info = auth_header.split(' ', 1)
        if auth_type.lower() != 'basic':
            return False, "Unsupported auth type"
        decoded = base64.b64decode(auth_info).decode('utf-8')
        username, password = decoded.split(':', 1)
        if hmac.compare_digest(username, expected_username) and hmac.compare_digest(password, expected_password):
            return True, None
        else:
            return False, "Authentication failed"
    except Exception as e:
        logger.error(f"Auth parse error: {e}")
        return False, "Auth format error"
