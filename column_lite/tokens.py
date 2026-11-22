"""JWT HS256 для glagol колонки.

Колонка просит у бэкенда токен (/glagol/token) и ходит с ним в свой локальный
glagol на 127.0.0.1:1961. Ключ генерируется один раз и лежит рядом с конфигом.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from pathlib import Path


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def jwt_hs256(claims: dict, key: bytes) -> str:
    head = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = _b64(json.dumps(claims, separators=(",", ":")).encode())
    sig = hmac.new(key, f"{head}.{body}".encode(), hashlib.sha256).digest()
    return f"{head}.{body}.{_b64(sig)}"


def check_jwt(token: str, key: bytes, leeway_s: float = 60.0) -> bool:
    """Подпись И срок действия.

    Раньше проверялась только подпись: выпущенный однажды токен оставался
    валидным навсегда, даже после завершения exp (аудит 20.11). Токены
    выпускаются на сутки (glagol_token), колонка переспрашивает валидность
    через /glagol/v2.0/check_token и получает новый по /glagol/token;
    запас в минуту — на расхождение часов.
    """
    try:
        head, body, sig = token.strip().split(".")
        good = _b64(hmac.new(key, f"{head}.{body}".encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(good, sig):
            return False
        raw = body + "=" * (-len(body) % 4)
        claims = json.loads(base64.urlsafe_b64decode(raw).decode("utf-8", "replace"))
        exp = claims.get("exp")
        if exp is None:
            return True
        return float(exp) + leeway_s > time.time()
    except (ValueError, TypeError, AttributeError):
        return False


def load_or_create_key(path: str | Path) -> bytes:
    p = Path(path)
    try:
        data = p.read_bytes()
        if len(data) >= 16:
            return data
    except OSError:
        pass
    p.parent.mkdir(parents=True, exist_ok=True)
    key = secrets.token_bytes(32)
    p.write_bytes(key)
    return key


def glagol_token(device_id: str, platform: str, key: bytes) -> str:
    now = int(time.time())
    return jwt_hs256({"plt": platform, "sub": device_id, "iss": "quasar-backend",
                      "aud": "glagol", "iat": now, "exp": now + 86400}, key)
