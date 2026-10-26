"""buffer-api: the one sampler and ledger for a buffer, as a small HTTP service (spec v0.5 §5).

`BufferService.handle` does all routing and validation without sockets; `make_server` wraps it in the standard
library's threading HTTP server. Consumers are untrusted (§7): bodies are validated strictly and a consumer can
only name its split.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import random
import re
import threading
import time
from collections import deque
from dataclasses import asdict, replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Literal
from urllib.parse import parse_qsl, unquote, urlparse, urlsplit

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from mia_core.assets import ASSET_STATES
from mia_core.fs import require_local_dir
from mia_core.locks import LockDir
from mia_core.registry import read_token

from .config import Config
from .ledger import BOARD_STATUSES, EVENT_KINDS, Ledger
from .plan import build_plan
from .sampler import Level, Volume, draw, fits, servable
from .spec import SpecError, load_spec_text, spec_text_hash
from .sync import scan_assets, scan_storage

RETRY_AFTER_S = 30
DEFAULT_SPLIT_SIZE = 256
MAX_N = 1024  # descriptors per request; consumers are untrusted (§7)
DATASET_STATES = ("in-progress", "prepared", "ineligible", "needs-human")  # no row = unprepared
CATALOG_REF_RE = re.compile(r"^[0-9a-f]{40}$")  # a commit, never a branch
log = logging.getLogger(__name__)
UI_NAME_RE = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9._-]*$")  # a file of the page by basename; no dotfile, no "/"
UI_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript",
    ".css": "text/css",
    ".svg": "image/svg+xml",
    ".png": "image/png",
}
EMPTY_WINDOW_S = 3600  # the usage route's "last hour"
JSON_NULL: tuple[bytes, dict[str, str]] = (b"null", {"Content-Type": "application/json"})
# a dict (or None) goes out as JSON; bytes go out as-is, with the Content-Type the handler put in the headers
Response = tuple[int, dict | bytes | None, dict[str, str]]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BufferConfig(_Strict):
    spec_name: str
    spec_hash: str
    catalog_ref: str | None = None
    levels: list[int]
    patch: int
    channels: int | Literal["all"]
    seed: int = 0
    tensorizer: str = "muvit-2d"
    tensorizer_params: dict = {}
    splits: dict[str, float]
    split_sizes: dict[str, int] = {}  # frozen list lengths for val and test
    recipe: dict | None = None
    plan_hash: str | None = None
    idle_hours: float = 72
    retire_after_days: float = 14
    auto_approve: bool = False
    max_empty_fraction: float = 0.05  # spec health: the share of empty answers (204) the page flags above


class LevelBody(_Strict):
    factor: int
    shape: list[int]
    scale_nm: dict[str, float]
    translation_nm: dict[str, float]

    @model_validator(mode="after")
    def _has_y_x(self) -> LevelBody:
        for name, mapping in (("scale_nm", self.scale_nm), ("translation_nm", self.translation_nm)):
            if "y" not in mapping or "x" not in mapping:
                raise ValueError(f"level {self.factor}: {name} needs y and x")
        return self


class VolumeBody(_Strict):
    path: str
    record: str
    axes: str
    shape: list[int]
    levels: list[LevelBody]
    split_planes: dict[str, list[int]]
    labels: dict[str, str] = {}
    weight: float
    channels_stored: int
    recipe: dict | None = None
    voxel_size_nm: dict[str, float]
    normalize: dict | None = None  # {"lo": [per channel], "hi": [per channel]} for normalize.scope volume|dataset
    artifact: str | None = None  # the staged artifact's id; without one the volume references its directory


class ConsumerBody(_Strict):
    name: str
    host: str
    pid: int
    rank: int = 0
    split: str
    mode: Literal["local", "remote"] = "local"


class RecordBody(_Strict):
    path: str
    record: dict  # a missing or odd id is the planner's to report, not a reason to refuse the catalog


class RecordsBody(_Strict):
    records: list[RecordBody]


class PlanBody(_Strict):
    spec: str  # the spec's YAML text


class BoardBody(_Strict):
    status: str
    reason: str = ""
    job: str | None = None


class EventBody(_Strict):
    kind: str
    issue: int | None = None
    event_id: str | None = None
    detail: str = ""


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _parse[Model: _Strict](model: type[Model], body: bytes) -> Model:
    try:
        return model.model_validate_json(body or b"null")
    except ValidationError as error:
        raise HttpError(422, str(error)) from None


def _int(query: dict[str, str], key: str, default: int | None = None, *, low: int, high: int) -> int:
    value = query.get(key)
    if value is None and default is not None:
        return default
    try:
        parsed = int(value or "")
    except ValueError:
        raise HttpError(422, f"query {key} must be an integer") from None
    if not low <= parsed <= high:
        raise HttpError(422, f"query {key} must be between {low} and {high}, got {parsed}")
    return parsed


# (method, pattern, handler, capability): consumer and read routes are open, worker routes need the bearer token
ROUTES = [
    ("PUT", re.compile(r"^/buffers/([^/]+)$"), "put_buffer", "worker"),
    ("PUT", re.compile(r"^/buffers/([^/]+)/volumes/([^/]+)$"), "put_volume", "worker"),
    ("POST", re.compile(r"^/buffers/([^/]+)/consumers$"), "add_consumer", "consumer"),
    ("GET", re.compile(r"^/buffers/([^/]+)/next$"), "next", "consumer"),
    ("GET", re.compile(r"^/buffers/([^/]+)/split/([^/]+)$"), "split", "consumer"),
    ("GET", re.compile(r"^/buffers/([^/]+)/status$"), "status", "consumer"),
    ("GET", re.compile(r"^/buffers$"), "list_buffers", "read"),
    ("PUT", re.compile(r"^/buffers/([^/]+)/datasets/([^/]+)$"), "put_board", "worker"),
    ("GET", re.compile(r"^/buffers/([^/]+)/datasets$"), "board", "read"),
    ("POST", re.compile(r"^/buffers/([^/]+)/events$"), "add_event", "worker"),
    ("GET", re.compile(r"^/buffers/([^/]+)/events$"), "events", "read"),
    ("DELETE", re.compile(r"^/buffers/([^/]+)/refs$"), "retire", "worker"),
    ("PUT", re.compile(r"^/assets/([a-z]+)/(.+)$"), "put_asset", "worker"),
    ("GET", re.compile(r"^/assets/([a-z]+)/(.+)$"), "get_asset", "read"),
    ("GET", re.compile(r"^/assets/([a-z]+)$"), "list_assets", "read"),
    ("PUT", re.compile(r"^/records/([^/]+)$"), "put_records", "worker"),
    ("GET", re.compile(r"^/records$"), "records", "read"),
    ("GET", re.compile(r"^/catalogs$"), "catalogs", "read"),
    ("GET", re.compile(r"^/datasets$"), "list_datasets", "read"),
    ("PUT", re.compile(r"^/datasets/([^/]+)$"), "put_dataset", "worker"),
    ("GET", re.compile(r"^/datasets/([^/]+)$"), "get_dataset", "read"),
    ("POST", re.compile(r"^/plan$"), "plan", "read"),  # a POST only to carry the spec text; it writes nothing
    ("POST", re.compile(r"^/sync/datasets$"), "sync_datasets", "worker"),
    ("POST", re.compile(r"^/sync/assets$"), "sync_assets", "worker"),
    ("GET", re.compile(r"^/locks$"), "locks", "read"),
    ("GET", re.compile(r"^/$"), "page", "read"),
    ("GET", re.compile(r"^/ui/([^/]+)$"), "ui", "read"),
    ("GET", re.compile(r"^/meta$"), "meta", "read"),
    ("GET", re.compile(r"^/usage/([^/]+)$"), "usage", "read"),
    ("PUT", re.compile(r"^/keeper$"), "put_keeper", "worker"),
    ("GET", re.compile(r"^/keeper$"), "keeper", "read"),
]


class BufferService:
    """Routes → ledger operations. One train rng per buffer, seeded from the config; frozen lists seeded per split."""

    def __init__(
        self,
        ledger: Ledger,
        stale_s: float = 300.0,
        *,
        token: str | None = None,
        storage_root: Path | None = None,
        raw_root: Path | None = None,
        staged_root: Path | None = None,
        locks: LockDir | None = None,
        archive_dir: Path | None = None,
        meta: dict | None = None,
        ui_dir: Path | None = None,
    ):
        self.ledger = ledger
        self.token = token  # None disables the worker check (tests, a dev service)
        self.stale_s = stale_s
        self.storage_root = storage_root  # mia-data's published datasets; None disables the datasets sync
        self.raw_root, self.staged_root = raw_root, staged_root  # the assets sync scans these; None skips one
        self.locks = locks  # the workers' lock files, shown read-only; None shows none
        self.archive_dir = archive_dir  # a retire writes the buffer's draws here; None keeps them (a dev service)
        self.meta = meta  # what the page needs from mia.yaml (page_meta); None answers nulls
        self.ui_dir = ui_dir  # the page's files; None serves no page
        self._empty: dict[str, deque[float]] = {}  # per buffer, the times of the last hour's 204s
        self._rng: dict[str, random.Random] = {}
        self._lock = threading.Lock()
        self._sync_lock = threading.Lock()

    def _authorized(self, headers: dict[str, str]) -> bool:
        if self.token is None:
            return True
        given = {key.lower(): value for key, value in headers.items()}.get("authorization", "")
        return hmac.compare_digest(given.encode(), f"Bearer {self.token}".encode())

    def handle(
        self, method: str, path: str, query: dict[str, str], body: bytes, headers: dict[str, str] | None = None
    ) -> Response:
        try:
            for route_method, pattern, name, capability in ROUTES:
                match = pattern.match(path)
                if match and route_method == method:
                    if capability == "worker" and not self._authorized(headers or {}):
                        return 401, {"error": "unauthorized"}, {}
                    segments = [unquote(group) for group in match.groups()]  # volume ids hold "/" (record/v0.zarr)
                    return getattr(self, f"_{name}")(*segments, query=query, body=body)
            raise HttpError(404, f"no route {method} {path}")
        except HttpError as error:
            return error.status, {"error": str(error)}, {}
        except Exception as error:  # a bug answers 500 instead of dropping the connection
            return 500, {"error": f"{type(error).__name__}: {error}"}, {}

    def _config(self, buffer: str) -> BufferConfig:
        config = self.ledger.buffer(buffer)
        if config is None:
            raise HttpError(404, f"unknown buffer {buffer}")
        return BufferConfig.model_validate(config)

    def _serving(self, buffer: str) -> BufferConfig:
        """The config of a buffer consumers may still use: 410 once its latest event is `retired`."""
        config = self._config(buffer)
        if (self.ledger.latest_event(buffer) or {}).get("kind") == "retired":
            raise HttpError(410, "buffer retired")
        return config

    def _consumer(self, buffer: str, query: dict[str, str]) -> str:
        consumer_id = query.get("consumer", "")
        if not self.ledger.touch(buffer, consumer_id):
            raise HttpError(404, f"unknown consumer {consumer_id!r} for buffer {buffer}")
        return consumer_id

    def _put_buffer(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        config = _parse(BufferConfig, body)
        self.ledger.put_buffer(buffer, config.model_dump())
        with self._lock:
            self._rng.pop(buffer, None)
        return 200, {"buffer": buffer}, {}

    def _put_volume(self, buffer: str, volume_id: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._config(buffer)
        parsed = _parse(VolumeBody, body)
        levels = tuple(
            Level(level.factor, tuple(level.shape), level.scale_nm, level.translation_nm) for level in parsed.levels
        )
        factors = [level.factor for level in levels]
        if not factors or factors[0] != 1 or not set(config.levels) <= set(factors):
            raise HttpError(422, f"pyramid levels {factors} do not cover the buffer's levels {config.levels}")
        volume = Volume(
            id=volume_id,
            path=parsed.path,
            record=parsed.record,
            axes=parsed.axes,
            shape=tuple(parsed.shape),
            levels=levels,
            split_planes={split: tuple(planes) for split, planes in parsed.split_planes.items()},
            labels=parsed.labels,
            weight=parsed.weight,
            channels_stored=parsed.channels_stored,
            recipe=parsed.recipe,
            voxel_size_nm=parsed.voxel_size_nm,
            normalize=parsed.normalize,
            artifact=parsed.artifact,
        )
        if len(volume.shape) != len(volume.axes):
            raise HttpError(422, f"shape {volume.shape} does not match axes {volume.axes!r}")
        reason = fits(volume, config.patch, config.levels)
        if reason:
            raise HttpError(422, reason)
        staged = parsed.artifact or str(PurePosixPath(parsed.path).parent)  # the artifact, else its directory
        self.ledger.put_volume(buffer, volume, refs=[("staged", staged), ("raw", parsed.record)])
        return 200, {"buffer": buffer, "volume": volume_id}, {}

    # --- the registry: what exists on disk, and which buffers use it -----------------------------

    def _put_asset(self, kind: str, key: str, *, query: dict[str, str], body: bytes) -> Response:
        try:
            data = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(data, dict):
            raise HttpError(422, "an asset is a JSON object")
        if "state" in data and data["state"] not in ASSET_STATES:
            raise HttpError(422, f"state must be one of {list(ASSET_STATES)}, got {data['state']!r}")
        return 200, {"kind": kind, "key": key, "data": self.ledger.put_asset(kind, key, data)}, {}

    def _get_asset(self, kind: str, key: str, *, query: dict[str, str], body: bytes) -> Response:
        data, refs = self.ledger.asset(kind, key), self.ledger.refs(kind, key)
        if data is None and not refs:
            raise HttpError(404, f"no {kind} asset {key}")
        return 200, {"kind": kind, "key": key, "data": data, "refs": refs}, {}

    def _list_assets(self, kind: str, *, query: dict[str, str], body: bytes) -> Response:
        assets = self.ledger.assets(kind, state=query.get("state"), record=query.get("record"))
        return 200, {"kind": kind, "assets": assets, "bytes": self.ledger.asset_bytes(kind)}, {}

    def _sync_assets(self, *, query: dict[str, str], body: bytes) -> Response:
        if self.raw_root is None and self.staged_root is None:
            raise HttpError(503, "the service has neither raw_root nor staged_root")
        if not self._sync_lock.acquire(blocking=False):
            raise HttpError(409, "a sync is running")
        try:
            started = time.time()
            known = {("raw", row["key"]): row["data"] for row in self.ledger.assets("raw") if row["data"]}
            known |= {("staged", row["key"]): row["data"] for row in self.ledger.assets("staged") if row["data"]}
            found, building, skipped = scan_assets(self.raw_root, self.staged_root, known)
            kinds = tuple(kind for kind in ("raw", "staged") if kind not in skipped)
            counts = self.ledger.sync_assets(found, older_than=started, kinds=kinds)
            return 200, {**counts, "building": [str(path) for path in building], "skipped": skipped}, {}
        finally:
            self._sync_lock.release()

    def _locks(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"locks": [asdict(holder) for holder in self.locks.holders()] if self.locks else []}, {}

    # --- records, datasets and the plan: what the catalog holds and which of it is prepared ------

    def _put_records(self, ref: str, *, query: dict[str, str], body: bytes) -> Response:
        if not CATALOG_REF_RE.match(ref):
            raise HttpError(422, f"ref {ref!r} is not a 40-hex commit")
        records = [(item.path, item.record) for item in _parse(RecordsBody, body).records]
        return 200, {"ref": ref, "count": self.ledger.put_records(ref, records)}, {}

    def _records(self, *, query: dict[str, str], body: bytes) -> Response:
        ref = query.get("ref") or self.ledger.latest_catalog()
        if ref is None:
            raise HttpError(404, "no catalog synced")
        if ref not in {catalog["ref"] for catalog in self.ledger.catalogs()}:
            raise HttpError(404, f"catalog ref {ref} is not synced")
        records = [{"path": path, "record": record} for path, record in self.ledger.records(ref)]
        return 200, {"ref": ref, "records": records}, {}

    def _catalogs(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"catalogs": self.ledger.catalogs()}, {}

    def _put_dataset(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        try:
            fields = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(fields, dict):
            raise HttpError(422, "a dataset is a JSON object")
        if "state" in fields and fields["state"] not in DATASET_STATES:
            raise HttpError(422, f"state must be one of {list(DATASET_STATES)}, got {fields['state']!r}")
        return 200, self.ledger.put_dataset(record, fields), {}

    def _get_dataset(self, record: str, *, query: dict[str, str], body: bytes) -> Response:
        row = self.ledger.dataset(record)
        if row is None:
            raise HttpError(404, f"no dataset row for {record}")
        return 200, row, {}

    def _list_datasets(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"datasets": self.ledger.datasets(query.get("state"))}, {}

    def _plan(self, *, query: dict[str, str], body: bytes) -> Response:
        text = _parse(PlanBody, body).spec
        try:
            spec = load_spec_text(text)
        except SpecError as error:
            raise HttpError(422, str(error)) from None
        ref = spec.catalog.ref
        records = self.ledger.records(ref)
        if not records:
            raise HttpError(409, f"catalog ref {ref} is not synced; run: mia-buffers catalog sync --ref {ref}")
        states = {row["id"]: row.get("state", "unprepared") for row in self.ledger.datasets()}
        prepared = {record for record, state in states.items() if state == "prepared"}
        plan = build_plan(spec, spec_text_hash(text), ref, records, prepared=prepared)
        return 200, plan.to_dict(states), {}

    def _sync_datasets(self, *, query: dict[str, str], body: bytes) -> Response:
        root = self.storage_root
        if root is None:
            raise HttpError(503, "the service has no storage_root")
        if not self._sync_lock.acquire(blocking=False):
            raise HttpError(409, "a datasets sync is running")
        try:
            # an unmounted or empty storage_root would demote every prepared row
            if not (root.is_dir() and any(root.iterdir())):
                raise HttpError(503, f"storage_root {root} is missing or empty; not syncing")
            started = time.time()
            return 200, self.ledger.sync_datasets(scan_storage(root), older_than=started), {}
        finally:
            self._sync_lock.release()

    # --- a buffer's status board, events, summary and retire: the page and the lifecycle -----------

    def _list_buffers(self, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"buffers": self.ledger.buffers()}, {}

    def _put_board(self, buffer: str, record: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        row = _parse(BoardBody, body)
        if row.status not in BOARD_STATUSES:
            raise HttpError(422, f"status must be one of {list(BOARD_STATUSES)}, got {row.status!r}")
        return 200, self.ledger.put_buffer_dataset(buffer, record, row.status, row.reason, row.job), {}

    def _board(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        return 200, {"buffer": buffer, "datasets": self.ledger.buffer_datasets(buffer)}, {}

    def _add_event(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        # no config check: `created` and `planned` precede the buffer's config (create_buffer runs at sourcing)
        event = _parse(EventBody, body)
        if event.kind not in EVENT_KINDS:
            raise HttpError(422, f"kind must be one of {list(EVENT_KINDS)}, got {event.kind!r}")
        added = self.ledger.add_event(
            buffer, event.kind, issue=event.issue, event_id=event.event_id, detail=event.detail
        )
        return 200, added, {}

    def _events(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        return 200, {"buffer": buffer, "events": self.ledger.events(buffer)}, {}

    def _retire(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._config(buffer)
        return 200, self.ledger.retire(buffer, self.archive_dir), {}

    def _add_consumer(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        self._serving(buffer)
        info = _parse(ConsumerBody, body)
        return 201, {"consumer_id": self.ledger.add_consumer(buffer, info.model_dump())}, {}

    def _split_of(self, buffer: str, consumer_id: str) -> str:
        consumer = self.ledger.consumer(buffer, consumer_id)
        if consumer is None:
            raise HttpError(404, f"unknown consumer {consumer_id!r}")
        return str(consumer["split"])

    def _next(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._serving(buffer)
        consumer_id = self._consumer(buffer, query)
        n = _int(query, "n", 1, low=1, high=MAX_N)
        split = self._split_of(buffer, consumer_id)
        volumes = self.ledger.volumes(buffer)
        with self._lock:
            if buffer not in self._rng:  # seed from what the ledger already served, so a restart draws fresh crops
                self._rng[buffer] = random.Random(f"{config.seed}:{self.ledger.max_seq(buffer)}")
            rng = self._rng[buffer]
            descriptors = [
                d
                for _ in range(n)
                if (d := draw(rng, volumes, split, patch=config.patch, levels=config.levels, channels=config.channels))
            ]
        if not descriptors:
            return self._no_content(buffer)
        return 200, {"descriptors": self.ledger.record_draws(buffer, consumer_id, descriptors)}, {}

    def _split(self, buffer: str, split: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._serving(buffer)
        self._consumer(buffer, query)
        start, n = _int(query, "from", 0, low=0, high=10**9), _int(query, "n", 1, low=1, high=MAX_N)
        size = config.split_sizes.get(split, DEFAULT_SPLIT_SIZE)
        if start >= size:
            raise HttpError(416, f"from {start} is past the {split} list of {size}")
        volumes = self.ledger.volumes(buffer)
        if not servable(volumes, split):
            return self._no_content(buffer)
        end = min(start + n, size)
        with self._lock:
            frozen = self.ledger.frozen(buffer, split)
            if len(frozen) < end:
                rng = random.Random(f"{config.seed}:{split}")
                fresh = []
                for index in range(end):  # replay the whole prefix so the list never depends on request order
                    d = draw(rng, volumes, split, patch=config.patch, levels=config.levels, channels=config.channels)
                    if index >= len(frozen) and d is not None:
                        fresh.append({**d, "seq": index})
                self.ledger.extend_frozen(buffer, split, fresh)
                frozen += fresh
        return 200, {"descriptors": frozen[start:end], "size": size}, {}

    def _no_content(self, buffer: str) -> Response:
        """A 204 (nothing servable yet), counted for the buffer's usage: the day's row and the last hour's window."""
        now = time.time()
        with self._lock:
            self._empty_window(buffer, now).append(now)
        self.ledger.count_empty(buffer, datetime.fromtimestamp(now, UTC).strftime("%Y-%m-%d"))
        return 204, None, {"Retry-After": str(RETRY_AFTER_S)}

    def _empty_window(self, buffer: str, now: float) -> deque[float]:
        """The buffer's 204 times within the last hour, older ones dropped; call under `_lock`."""
        window = self._empty.setdefault(buffer, deque())
        while window and window[0] < now - EMPTY_WINDOW_S:
            window.popleft()
        return window

    # --- the page: its files, what it needs from mia.yaml, usage and the keeper's last pass --------

    def _page(self, *, query: dict[str, str], body: bytes) -> Response:
        return self._ui("index.html", query=query, body=body)

    def _ui(self, name: str, *, query: dict[str, str], body: bytes) -> Response:
        """A file of the page by basename only (the segment is unquoted already, so check it here)."""
        content_type = UI_TYPES.get(PurePosixPath(name).suffix)
        if self.ui_dir is None or not UI_NAME_RE.match(name) or content_type is None:
            raise HttpError(404, "no such file")
        path = self.ui_dir / name
        if not path.is_file():
            raise HttpError(404, "no such file")
        return 200, path.read_bytes(), {"Content-Type": content_type}

    def _meta(self, *, query: dict[str, str], body: bytes) -> Response:
        if self.meta is not None:
            return 200, self.meta, {}
        names = ("buffers_repo", "datasets_repo", "catalog_repo", "service_url")
        return 200, {**dict.fromkeys(names), "roots": {}, "quota": {}, "keeper": {}}, {}

    def _usage(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        config = self._config(buffer)
        with self._lock:
            empty = len(self._empty_window(buffer, time.time()))
        last_hour = {**self.ledger.last_hour(buffer), "empty": empty}
        days = self.ledger.usage(buffer)
        return (
            200,
            {"buffer": buffer, "days": days, "last_hour": last_hour, "max_empty_fraction": config.max_empty_fraction},
            {},
        )

    def _put_keeper(self, *, query: dict[str, str], body: bytes) -> Response:
        try:
            report = json.loads(body)
        except ValueError as error:
            raise HttpError(422, f"invalid JSON: {error}") from None
        if not isinstance(report, dict):
            raise HttpError(422, "a keeper report is a JSON object")
        report.setdefault("at", time.time())
        self.ledger.put_kv("keeper", report)
        return 200, report, {}

    def _keeper(self, *, query: dict[str, str], body: bytes) -> Response:
        report = self.ledger.kv("keeper")
        if report is None:
            return 200, *JSON_NULL
        return 200, report, {}

    def _status(self, buffer: str, *, query: dict[str, str], body: bytes) -> Response:
        """The buffer's summary; with a staged root, first stats each volume's pyramid once and flags the missing."""
        config = self._config(buffer)
        volumes = self.ledger.volumes(buffer)
        if self.staged_root is not None:  # a service without one (tests, a dev service) has no view of the disk
            for index, volume in enumerate(volumes):
                missing = not os.path.exists(volume.path)
                if missing != volume.missing:
                    self.ledger.set_volume_missing(buffer, volume.id, missing, volume.path)
                    volumes[index] = replace(volume, missing=missing)
        splits = sorted({split for volume in volumes for split in volume.split_planes})
        return (
            200,
            {
                "buffer": buffer,
                "config": config.model_dump(),
                "volumes": len(volumes),
                "servable": {split: len(servable(volumes, split)) for split in splits},
                "missing": [volume.id for volume in volumes if volume.missing],
                "draws": self.ledger.draws_per_volume(buffer),
                "consumers": self.ledger.consumers(buffer, self.stale_s),
            },
            {},
        )


def make_server(service: BufferService, host: str, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def _dispatch(self) -> None:
            parts = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            status, payload, headers = service.handle(
                self.command, parts.path, dict(parse_qsl(parts.query)), body, dict(self.headers.items())
            )
            if isinstance(payload, bytes):
                data = payload  # the handler's headers carry its Content-Type
            else:
                data = json.dumps(payload).encode() if payload is not None else b""
                if data:
                    headers = {**headers, "Content-Type": "application/json"}
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        do_GET = do_PUT = do_POST = do_DELETE = _dispatch

        def log_message(self, format: str, *args: object) -> None:  # quiet; the ledger is the record
            return

    return ThreadingHTTPServer((host, port), Handler)


def ledger_path(cfg: Config) -> Path:
    """Where buffer-api keeps its SQLite file: host-local disk, never the shared state directory."""
    if cfg.paths.local_root is None:
        raise RuntimeError("set paths.local_root in mia.yaml: buffer-api keeps its SQLite file on host-local disk")
    return cfg.paths.local_root / "service" / "buffers.sqlite"


def open_ledger(cfg: Config) -> Ledger:
    """The ledger on the local root. The first start moves the old `<state_dir>/buffers.sqlite` there (the old file
    keeps a `.moved` suffix); a missing ledger beside existing backups is refused, never silently started empty."""
    path = ledger_path(cfg)
    require_local_dir(path.parent)
    old = cfg.paths.state_dir / "buffers.sqlite"
    if not path.exists() and old.exists():
        old_ledger = Ledger(old)
        old_ledger.backup(path)
        old_ledger.close()
        os.replace(old, old.with_suffix(".sqlite.moved"))
        print(f"buffer-api: moved the ledger from {old} to {path}", flush=True)
    elif not path.exists():
        backups = sorted((cfg.paths.state_dir / "backups").glob("buffers-*.sqlite"))
        if backups:
            raise RuntimeError(
                f"the ledger {path} is missing but backups exist; the newest is {backups[-1]}: "
                f"copy it to {path} to restore it"
            )
    return Ledger(path)


def prune_backups(directory: Path, keep: int = 48) -> None:
    """Delete the oldest `buffers-*.sqlite` files beyond `keep` (the UTC stamp in the name sorts by age)."""
    for stale in sorted(directory.glob("buffers-*.sqlite"))[:-keep]:
        stale.unlink(missing_ok=True)


def backup_forever(ledger: Ledger, directory: Path, minutes: int, stop: threading.Event) -> None:
    """Back the ledger up every `minutes` until `stop` is set; a failed backup is logged, never raised."""
    while not stop.wait(minutes * 60):
        try:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            ledger.backup(directory / f"buffers-{stamp}.sqlite")
            prune_backups(directory)
        except Exception:
            log.exception("ledger backup failed")


def page_meta(cfg: Config) -> dict:
    """What the page reads from mia.yaml (`GET /meta`): the repos, the service URL, each root's browser URL (None when
    unset), the quota and the keeper's cadence."""
    github, paths = cfg.github, cfg.paths
    return {
        "buffers_repo": github.buffers_repo,
        "datasets_repo": github.datasets_repo,
        "catalog_repo": github.catalog_repo,
        "service_url": cfg.service.url,
        "roots": {root: getattr(paths, f"{root}_url") for root in paths.ROOTS},
        "quota": cfg.quota.model_dump(),
        "keeper": cfg.keeper.model_dump(),
    }


def serve(cfg: Config) -> None:
    """Run buffer-api until interrupted, as configured: URL, ledger on the local root, backups, worker token."""
    if cfg.service.token_file is None:
        raise SystemExit("set service.token_file in mia.yaml and run `mia init-token`")
    token = read_token(cfg.service.token_file)
    if cfg.paths.local_root is None:
        raise SystemExit("set paths.local_root in mia.yaml")
    try:
        ledger = open_ledger(cfg)
    except RuntimeError as error:
        raise SystemExit(str(error)) from error
    url = urlparse(cfg.service.url)
    assert url.port is not None  # ServiceCfg requires an explicit port
    server = make_server(
        BufferService(
            ledger,
            token=token,
            storage_root=cfg.paths.storage_root,
            raw_root=cfg.paths.raw_root,
            staged_root=cfg.paths.staged_root,
            locks=LockDir(cfg.paths.local_root / "locks"),
            archive_dir=cfg.paths.state_dir / "archive",
            meta=page_meta(cfg),
            ui_dir=Path(__file__).parent / "ui",
        ),
        url.hostname or "127.0.0.1",
        url.port,
    )
    backups = cfg.paths.state_dir / "backups"
    stop = threading.Event()
    threading.Thread(target=backup_forever, args=(ledger, backups, cfg.keeper.minutes, stop), daemon=True).start()
    host_name, port_number = server.server_address[0], server.server_address[1]
    print(f"buffer-api on http://{host_name!s}:{port_number} ledger {ledger_path(cfg)} backups {backups}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
