import base64
import json

import httpx
import pytest
import respx
from autodev_helpers import API, make_cfg
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from autodev.auth import AppTokens, app_jwt, bot_login, noreply_email, token_fn


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


@pytest.fixture(scope="module")
def key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def test_app_jwt_is_signed_and_backdated(key):
    tok = app_jwt(42, _pem(key), now=1000)
    h, p, s = tok.split(".")
    key.public_key().verify(_unb64(s), f"{h}.{p}".encode(), padding.PKCS1v15(), hashes.SHA256())
    assert json.loads(_unb64(h)) == {"alg": "RS256", "typ": "JWT"}
    assert json.loads(_unb64(p)) == {"iat": 940, "exp": 1540, "iss": "42"}


@respx.mock
def test_app_tokens_are_cached_until_near_expiry(key, tmp_path):
    kp = tmp_path / "app.pem"
    kp.write_bytes(_pem(key))
    route = respx.post(f"{API}/app/installations/7/access_tokens").mock(
        side_effect=[
            httpx.Response(201, json={"token": "t1", "expires_at": "2026-10-10T12:00:00Z"}),
            httpx.Response(201, json={"token": "t2", "expires_at": "2026-10-10T13:00:00Z"}),
        ]
    )
    now = [1791633600.0 - 3600]  # 2026-10-10T11:00:00Z
    tokens = AppTokens(42, 7, kp, clock=lambda: now[0])
    assert tokens() == "t1" and tokens() == "t1" and route.call_count == 1
    now[0] = 1791633600.0 - 200  # within 5 minutes of expiry
    assert tokens() == "t2" and route.call_count == 2
    assert route.calls[0].request.headers["authorization"].startswith("Bearer ey")


def test_token_mode_reads_env_then_file(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "pat")
    assert token_fn(make_cfg(tmp_path))() == "pat"
    monkeypatch.delenv("GITHUB_TOKEN")
    with pytest.raises(RuntimeError, match="GITHUB_TOKEN"):
        token_fn(make_cfg(tmp_path))()
    (tmp_path / "tok").write_text("from-file\n")
    assert token_fn(make_cfg(tmp_path, github={"token_file": str(tmp_path / "tok")}))() == "from-file"


@respx.mock
async def test_bot_login_token_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "pat")
    respx.get(f"{API}/user").mock(return_value=httpx.Response(200, json={"login": "me"}))
    assert await bot_login(make_cfg(tmp_path)) == "me"


@respx.mock
async def test_bot_login_app_mode(tmp_path, key):
    kp = tmp_path / "app.pem"
    kp.write_bytes(_pem(key))
    respx.get(f"{API}/app").mock(return_value=httpx.Response(200, json={"slug": "autodev-bot"}))
    cfg = make_cfg(tmp_path, github={"auth": "app", "app_id": 42, "installation_id": 7, "private_key_path": str(kp)})
    assert await bot_login(cfg) == "autodev-bot[bot]"


@respx.mock
async def test_noreply_email_uses_the_bot_user_id(tmp_path):
    respx.get(f"{API}/users/autodev-bot%5Bbot%5D").mock(return_value=httpx.Response(200, json={"id": 123}))
    assert (
        await noreply_email(make_cfg(tmp_path), "autodev-bot[bot]") == "123+autodev-bot[bot]@users.noreply.github.com"
    )
