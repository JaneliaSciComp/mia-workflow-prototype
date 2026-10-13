"""Who autodev is on GitHub, and how it gets a token.

Two modes: `token` runs as you, with a personal access token from the environment or a file; `app` runs
as a GitHub App installation, minting short-lived installation tokens from the app's private key.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

if TYPE_CHECKING:
    from .config import Config

_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


def _b64url(data: bytes) -> str:
    """Base64url without padding, as JWTs want it."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def app_jwt(app_id: int, pem: bytes, now: float | None = None) -> str:
    """RS256 JWT for GitHub App auth: valid 9 minutes, iat backdated 60 s for clock drift."""
    now = int(time.time() if now is None else now)
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"iat": now - 60, "exp": now + 540, "iss": str(app_id)}).encode())
    key = serialization.load_pem_private_key(pem, password=None)
    signature = key.sign(f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{payload}.{_b64url(signature)}"


class AppTokens:
    """A callable that returns a valid installation token, minting a new one when the current one is within
    5 minutes of expiry. Thread-safe, since git runs in worker threads."""

    def __init__(self, app_id: int, installation_id: int, key_path: Path, api_url: str = "https://api.github.com",
                 clock: Callable[[], float] = time.time, http: httpx.Client | None = None):
        self._app_id, self._installation_id, self._key_path, self._api_url = app_id, installation_id, Path(key_path), api_url
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
                response = self._http.post(f"{self._api_url}/app/installations/{self._installation_id}/access_tokens",
                                           headers={**_HEADERS, "Authorization": f"Bearer {jwt}"})
                response.raise_for_status()
                body = response.json()
                self._token = body["token"]
                self._expires = datetime.fromisoformat(body["expires_at"]).timestamp()
            return self._token


def token_fn(cfg: Config) -> Callable[[], str]:
    """A zero-argument callable that returns the current GitHub token for this config's auth mode."""
    github = cfg.github
    if github.auth == "app":
        return AppTokens(github.app_id, github.installation_id, github.private_key_path, github.api_url)

    def from_env_or_file() -> str:
        if token := os.environ.get(github.token_env):
            return token
        if github.token_file and github.token_file.is_file():
            return github.token_file.read_text().strip()
        raise RuntimeError(f"no GitHub token: set ${github.token_env} (or put it in .env), or github.token_file")

    return from_env_or_file


async def bot_login(cfg: Config) -> str:
    """The login autodev's own comments, commits and PRs appear under ("<app slug>[bot]" for an app)."""
    github = cfg.github
    async with httpx.AsyncClient(base_url=github.api_url, timeout=30, headers=_HEADERS) as http:
        if github.auth == "token":
            response = await http.get("/user", headers={"Authorization": f"Bearer {token_fn(cfg)()}"})
            response.raise_for_status()
            return response.json()["login"]
        jwt = app_jwt(github.app_id, github.private_key_path.read_bytes())
        response = await http.get("/app", headers={"Authorization": f"Bearer {jwt}"})
        response.raise_for_status()
        return f"{response.json()['slug']}[bot]"


async def noreply_email(cfg: Config, login: str) -> str:
    """The address that links a commit to `login` on GitHub (for an app, the id of its [bot] user, not the app id)."""
    async with httpx.AsyncClient(base_url=cfg.github.api_url, timeout=30, headers=_HEADERS) as http:
        response = await http.get(f"/users/{quote(login)}")
        response.raise_for_status()
        return f"{response.json()['id']}+{login}@users.noreply.github.com"
