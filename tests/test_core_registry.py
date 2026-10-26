import logging

import httpx
import pytest

from mia_core.registry import Registry, RegistryError, best_effort, read_token


def transport(seen, status=200, body=None):
    def handler(request):
        seen.append(request)
        return httpx.Response(status, json=body if body is not None else {"ok": True})

    return httpx.MockTransport(handler)


def test_bearer_header_and_quoting():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.put_asset("staged", "a/b c", {"x": 1})
    assert seen[0].headers["authorization"] == "Bearer tok" and seen[0].url.raw_path == b"/assets/staged/a%2Fb%20c"


def test_no_token_sends_no_header():
    seen = []
    Registry("http://svc", None, transport=transport(seen)).datasets("prepared")
    assert "authorization" not in seen[0].headers and seen[0].url.params["state"] == "prepared"


def test_errors_carry_status_and_message():
    reg = Registry("http://svc", "t", transport=transport([], 401, {"error": "unauthorized"}))
    with pytest.raises(RegistryError) as err:
        reg.put_dataset("rec", state="prepared")
    assert err.value.status == 401 and err.value.message == "unauthorized" and str(err.value) == "unauthorized"


def test_404_is_none_for_lookups():
    reg = Registry("http://svc", "t", transport=transport([], 404, {"error": "no"}))
    assert reg.dataset("rec") is None and reg.asset("staged", "k") is None


def test_asset_returns_the_data_field_and_204_is_none():
    assert Registry("http://svc", "t", transport=transport([], 200, {"data": {"a": 1}})).asset("k", "v") == {"a": 1}
    reg = Registry("http://svc", "t", transport=httpx.MockTransport(lambda request: httpx.Response(204)))
    assert reg.sync_datasets() is None


def test_put_records_body():
    seen = []
    Registry("http://svc", "t", transport=transport(seen)).put_records("r/1", [("a", {"x": 1})])
    assert seen[0].url.raw_path == b"/records/r%2F1"
    assert seen[0].read() == b'{"records":[{"path":"a","record":{"x":1}}]}'


def test_transport_error_is_registry_error():
    def boom(request):
        raise httpx.ConnectError("down")

    with pytest.raises(RegistryError) as err:
        Registry("http://svc", "t", transport=httpx.MockTransport(boom)).catalogs()
    assert err.value.status is None


def test_best_effort_logs_and_continues(caplog):
    def fail():
        raise RegistryError(None, "down")

    assert best_effort(fail, "dataset rec", logging.getLogger("t")) is False
    assert "dataset rec: registry write skipped: down" in caplog.text
    assert best_effort(lambda: None, "x", logging.getLogger("t")) is True


def test_read_token(tmp_path):
    (tmp_path / "t").write_text("abc\n")
    assert read_token(tmp_path / "t") == "abc"
    (tmp_path / "empty").write_text("\n")
    for name in ("missing", "empty"):
        with pytest.raises(RegistryError):
            read_token(tmp_path / name)
