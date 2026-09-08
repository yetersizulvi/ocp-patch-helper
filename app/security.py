from __future__ import annotations

import hashlib
import hmac


def sign_request(
    token: str,
    timestamp: str,
    method: str,
    path: str,
    body: bytes = b"",
) -> str:
    digest = hashlib.sha256(body).hexdigest()
    canonical = "\n".join((timestamp, method.upper(), path, digest)).encode("utf-8")
    return hmac.new(token.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
