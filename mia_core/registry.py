"""The worker-side client of buffer-api's registry: bearer token, JSON in and out, one error type."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from mia_core.config import ServiceCfg

SYNC_TIMEOUT_S = 600.0  # a sync walks a whole root (du, stage.json reads): minutes, not the 30 s of a lookup


class RegistryError(RuntimeError):
    """A failed registry call: `status` is the HTTP status, or None when the service was not reached."""

    def __init__(self, status: int | None, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def read_token(path: Path) -> str:
    """The worker token in `path`, stripped; RegistryError when the file is missing or empty."""
    try:
        token = path.read_text().strip()
    except OSError as err:
        raise RegistryError(None, f"cannot read the service token {path}: {err}") from err
    if not token:
        raise RegistryError(None, f"the service token file {path} is empty")
    return token


class Registry:
    """buffer-api's registry over HTTP; one connection pool for the object's life."""

    def __init__(
        self,
        url: str,
        token: str | None = None,
        *,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._client = httpx.Client(base_url=url, timeout=timeout, transport=transport, headers=headers)

    @classmethod
    def from_config(cls, service: ServiceCfg) -> Registry:
        return cls(service.url, read_token(service.token_file) if service.token_file else None)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Registry:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def request(
        self, method: str, path: str, *, json: Any = None, params: dict | None = None, timeout: float | None = None
    ) -> Any:
        """Send the request; the parsed JSON body (None when empty), or RegistryError on any failure. `timeout`
        (seconds) overrides the client's for this call."""
        per_call = httpx.USE_CLIENT_DEFAULT if timeout is None else httpx.Timeout(timeout)  # httpx: None is no limit
        try:
            response = self._client.request(method, path, json=json, params=params, timeout=per_call)
        except (httpx.HTTPError, httpx.InvalidURL) as err:
            raise RegistryError(None, str(err)) from err
        if response.status_code >= 400:
            try:
                body = response.json()
            except ValueError:
                body = None
            message = body.get("error") if isinstance(body, dict) else None
            raise RegistryError(response.status_code, message or response.text)
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as err:
            raise RegistryError(response.status_code, f"not a JSON body: {response.text[:200]}") from err

    def _lookup(self, path: str) -> Any:
        """GET `path`; None when the registry has no such thing (404)."""
        try:
            return self.request("GET", path)
        except RegistryError as err:
            if err.status == 404:
                return None
            raise

    def put_dataset(self, record: str, **fields: Any) -> dict:
        return self.request("PUT", f"/datasets/{quote(record, safe='')}", json=fields)

    def dataset(self, record: str) -> dict | None:
        return self._lookup(f"/datasets/{quote(record, safe='')}")

    def datasets(self, state: str | None = None) -> list[dict]:
        return self.request("GET", "/datasets", params={"state": state} if state else None)["datasets"]

    def put_asset(self, kind: str, key: str, data: dict) -> dict:
        return self.request("PUT", f"/assets/{quote(kind, safe='')}/{quote(key, safe='')}", json=data)

    def asset(self, kind: str, key: str) -> dict | None:
        found = self._lookup(f"/assets/{quote(kind, safe='')}/{quote(key, safe='')}")
        return None if found is None else found["data"]

    def assets(self, kind: str, state: str | None = None, record: str | None = None) -> dict:
        """{kind, assets: [{key, data, refs}], bytes}, filtered on the rows' state and record when given."""
        params = {name: value for name, value in (("state", state), ("record", record)) if value is not None}
        return self.request("GET", f"/assets/{quote(kind, safe='')}", params=params or None)

    def sync_assets(self) -> dict:
        return self.request("POST", "/sync/assets", timeout=SYNC_TIMEOUT_S)

    def locks(self) -> list[dict]:
        return self.request("GET", "/locks")["locks"]

    def put_records(self, ref: str, records: list[tuple[str, dict]]) -> dict:
        body = {"records": [{"path": path, "record": record} for path, record in records]}
        return self.request("PUT", f"/records/{quote(ref, safe='')}", json=body)

    def plan(self, spec_text: str) -> dict:
        return self.request("POST", "/plan", json={"spec": spec_text})

    def sync_datasets(self) -> dict:
        return self.request("POST", "/sync/datasets", timeout=SYNC_TIMEOUT_S)

    def catalogs(self) -> list[dict]:
        return self.request("GET", "/catalogs")["catalogs"]

    def records(self, ref: str) -> list[tuple[str, dict]]:
        """The (path, record) pairs synced for `ref`; RegistryError 404 when the ref was never synced."""
        rows = self.request("GET", "/records", params={"ref": ref})["records"]
        return [(row["path"], row["record"]) for row in rows]

    # --- a buffer's status board, events and lifecycle ------------------------------------------

    def put_buffer_dataset(
        self, buffer: str, record: str, status: str, reason: str = "", job: str | None = None
    ) -> dict:
        path = f"/buffers/{quote(buffer, safe='')}/datasets/{quote(record, safe='')}"
        return self.request("PUT", path, json={"status": status, "reason": reason, "job": job})

    def buffer_datasets(self, buffer: str) -> list[dict]:
        return self.request("GET", f"/buffers/{quote(buffer, safe='')}/datasets")["datasets"]

    def event(self, buffer: str, kind: str, **fields: Any) -> dict:
        """Append a lifecycle event (`issue`, `event_id`, `detail` optional); returns it with its seq."""
        return self.request("POST", f"/buffers/{quote(buffer, safe='')}/events", json={"kind": kind, **fields})

    def buffers(self) -> list[dict]:
        return self.request("GET", "/buffers")["buffers"]

    def retire(self, buffer: str) -> dict:
        """Drop the buffer's references and archive its draws: {refs, draws, archive}. The archive gzips every
        draw of the buffer: minutes on a large one, so the sync timeout."""
        return self.request("DELETE", f"/buffers/{quote(buffer, safe='')}/refs", timeout=SYNC_TIMEOUT_S)

    def status(self, buffer: str) -> dict:
        """The buffer's status (stats its pyramids, flags the missing); a consumer route, the token is harmless."""
        return self.request("GET", f"/buffers/{quote(buffer, safe='')}/status")


def best_effort(action: Callable[[], Any], what: str, log: logging.Logger) -> bool:
    """Run a registry write that must not fail its caller; True on success, a logged warning and False if not."""
    try:
        action()
    except RegistryError as err:
        log.warning("%s: registry write skipped: %s", what, err)
        return False
    return True
