"""Who autodev is on GitHub. token: a personal token (run as yourself). app: a GitHub App installation (service)."""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def app_jwt(app_id: int, pem: bytes, now: float | None = None) -> str:
    """RS256 JWT for GitHub App auth: valid 9 minutes, iat backdated 60 s for clock drift."""
    now = int(time.time() if now is None else now)
    header = _b64(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64(json.dumps({"iat": now - 60, "exp": now + 540, "iss": str(app_id)}).encode())
    key = serialization.load_pem_private_key(pem, password=None)
    sig = key.sign(f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{payload}.{_b64(sig)}"


class AppTokens:
    """Installation tokens, minted on demand and reused until 5 minutes before they expire."""

    def __init__(self, app_id: int, installation_id: int, key_path: Path, api_url: str = "https://api.github.com",
                 clock: Callable[[], float] = time.time, http: httpx.Client | None = None):
        self._app_id, self._inst, self._key_path, self._api = app_id, installation_id, Path(key_path), api_url
        self._clock = clock
        self._http = http or httpx.Client(timeout=30)
        self._token: str | None = None
        self._expires = 0.0
        self._lock = threading.Lock()

    def __call__(self) -> str:
        with self._lock:
            if self._token is None or self._clock() > self._expires - 300:
                # ponytail: a sync HTTP call about once an hour from inside the event loop; fine at this rate.
                jwt = app_jwt(self._app_id, self._key_path.read_bytes(), now=self._clock())
                r = self._http.post(f"{self._api}/app/installations/{self._inst}/access_tokens",
                                    headers={**_HEADERS, "Authorization": f"Bearer {jwt}"})
                r.raise_for_status()
                body = r.json()
                self._token = body["token"]
                self._expires = datetime.fromisoformat(body["expires_at"]).timestamp()
            return self._token


def token_fn(cfg) -> Callable[[], str]:
    g = cfg.github
    if g.auth == "app":
        return AppTokens(g.app_id, g.installation_id, g.private_key_path, g.api_url)

    def from_env_or_file() -> str:
        if tok := os.environ.get(g.token_env):
            return tok
        if g.token_file and g.token_file.is_file():
            return g.token_file.read_text().strip()
        raise RuntimeError(f"no GitHub token: set ${g.token_env} (or put it in .env), or github.token_file")

    return from_env_or_file


async def bot_login(cfg) -> str:
    """The login autodev's own comments, commits and PRs appear under."""
    g = cfg.github
    async with httpx.AsyncClient(base_url=g.api_url, timeout=30, headers=_HEADERS) as c:
        if g.auth == "token":
            r = await c.get("/user", headers={"Authorization": f"Bearer {token_fn(cfg)()}"})
            r.raise_for_status()
            return r.json()["login"]
        jwt = app_jwt(g.app_id, g.private_key_path.read_bytes())
        r = await c.get("/app", headers={"Authorization": f"Bearer {jwt}"})
        r.raise_for_status()
        return f"{r.json()['slug']}[bot]"
