"""The buffer ledger: one SQLite file holding buffers, their volumes, consumers, every draw and the frozen lists,
plus the registry: what exists on disk (`assets`, by kind and key: a staged directory, a record's raw cache) and
which buffers reference it (`refs`), so a buffer can reuse what an earlier round staged and a retire can count
what is still in use. The registry is an index over the on-disk manifests (stage.json, files.json), rebuildable
from them; the files stay the source of truth.

Spec v0.5 §5 and §9.5: the ledger is the record of a run; replaying a run means replaying its descriptors.
"""

from __future__ import annotations

import dataclasses
import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from .sampler import Level, Volume

SCHEMA = """
CREATE TABLE IF NOT EXISTS buffers (id TEXT PRIMARY KEY, config TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS volumes (buffer TEXT, id TEXT, data TEXT NOT NULL, PRIMARY KEY (buffer, id));
CREATE TABLE IF NOT EXISTS consumers (id TEXT PRIMARY KEY, buffer TEXT, info TEXT NOT NULL, last_seen REAL);
CREATE TABLE IF NOT EXISTS draws (buffer TEXT, seq INTEGER, consumer TEXT, descriptor TEXT NOT NULL, ts REAL,
                                  PRIMARY KEY (buffer, seq));
CREATE TABLE IF NOT EXISTS frozen (buffer TEXT, split TEXT, idx INTEGER, descriptor TEXT NOT NULL,
                                   PRIMARY KEY (buffer, split, idx));
CREATE TABLE IF NOT EXISTS assets (kind TEXT, key TEXT, data TEXT NOT NULL, updated REAL, PRIMARY KEY (kind, key));
CREATE TABLE IF NOT EXISTS refs (buffer TEXT, kind TEXT, key TEXT, PRIMARY KEY (buffer, kind, key));
CREATE TABLE IF NOT EXISTS catalogs (ref TEXT PRIMARY KEY, synced_at REAL, count INTEGER);
CREATE TABLE IF NOT EXISTS records (ref TEXT, id TEXT, path TEXT NOT NULL, data TEXT NOT NULL,
                                    PRIMARY KEY (ref, path));
CREATE TABLE IF NOT EXISTS datasets (id TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at REAL);
"""


def _volume_from(data: dict) -> Volume:
    data = dict(data)
    data["levels"] = tuple(Level(**{**level, "shape": tuple(level["shape"])}) for level in data["levels"])
    data["shape"] = tuple(data["shape"])
    data["split_planes"] = {split: tuple(planes) for split, planes in data["split_planes"].items()}
    return Volume(**data)


class Ledger:
    """All reads and writes go through one connection guarded by a lock; writes are single transactions."""

    # ponytail: one process, one connection; a pool if the service ever runs multi-process

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def put_buffer(self, buffer: str, config: dict) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO buffers VALUES (?, ?)", (buffer, json.dumps(config)))

    def buffer(self, buffer: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT config FROM buffers WHERE id = ?", (buffer,)).fetchone()
        return json.loads(row[0]) if row else None

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """One locked BEGIN IMMEDIATE ... COMMIT; rolls back and re-raises on error."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def put_volume(self, buffer: str, volume: Volume, refs: Iterable[tuple[str, str]] = ()) -> None:
        """Upsert the volume and add its (kind, key) references in one transaction."""
        with self._transaction() as db:
            db.execute(
                "INSERT OR REPLACE INTO volumes VALUES (?, ?, ?)",
                (buffer, volume.id, json.dumps(dataclasses.asdict(volume))),
            )
            db.executemany("INSERT OR IGNORE INTO refs VALUES (?, ?, ?)", [(buffer, kind, key) for kind, key in refs])

    def volumes(self, buffer: str) -> list[Volume]:
        with self._lock:
            rows = self._db.execute("SELECT data FROM volumes WHERE buffer = ? ORDER BY id", (buffer,)).fetchall()
        return [_volume_from(json.loads(row[0])) for row in rows]

    def backup(self, dest: Path) -> None:
        """Write a consistent copy of the ledger to `dest`, atomically (temp file, then rename)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".tmp")
        target = sqlite3.connect(tmp)
        try:
            with self._lock:
                self._db.backup(target)
        finally:
            target.close()
        os.replace(tmp, dest)

    # --- catalogs and records: the parsed dataset records of a git ref ---------------------------

    def put_records(self, ref: str, records: list[tuple[str, dict]]) -> int:
        """Replace the ref's records (path, record) and note the sync; returns how many."""
        with self._transaction() as db:
            db.execute("DELETE FROM records WHERE ref = ?", (ref,))
            db.executemany(
                "INSERT INTO records VALUES (?, ?, ?, ?)",
                [(ref, record.get("id"), path, json.dumps(record)) for path, record in records],
            )
            db.execute("INSERT OR REPLACE INTO catalogs VALUES (?, ?, ?)", (ref, time.time(), len(records)))
        return len(records)

    def records(self, ref: str) -> list[tuple[str, dict]]:
        with self._lock:
            rows = self._db.execute("SELECT path, data FROM records WHERE ref = ? ORDER BY path", (ref,)).fetchall()
        return [(path, json.loads(data)) for path, data in rows]

    def catalogs(self) -> list[dict]:
        """Every synced ref, newest first: {ref, synced_at, count}."""
        with self._lock:
            rows = self._db.execute("SELECT ref, synced_at, count FROM catalogs ORDER BY synced_at DESC").fetchall()
        return [{"ref": ref, "synced_at": synced_at, "count": count} for ref, synced_at, count in rows]

    def latest_catalog(self) -> str | None:
        catalogs = self.catalogs()
        return catalogs[0]["ref"] if catalogs else None

    # --- datasets: one row per record, the state the prep workflow and the sync keep ---------------

    @staticmethod
    def _merge_dataset(db: sqlite3.Connection, record: str, fields: dict, now: float) -> dict:
        row = db.execute("SELECT data FROM datasets WHERE id = ?", (record,)).fetchone()
        merged = {**(json.loads(row[0]) if row else {}), **fields}
        db.execute("INSERT OR REPLACE INTO datasets VALUES (?, ?, ?)", (record, json.dumps(merged), now))
        return {**merged, "id": record, "updated_at": now}

    def put_dataset(self, record: str, fields: dict) -> dict:
        """Merge `fields` into the record's row (new keys win); returns the merged row."""
        with self._transaction() as db:
            return self._merge_dataset(db, record, fields, time.time())

    def dataset(self, record: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT data, updated_at FROM datasets WHERE id = ?", (record,)).fetchone()
        return {**json.loads(row[0]), "id": record, "updated_at": row[1]} if row else None

    def datasets(self, state: str | None = None) -> list[dict]:
        query = "SELECT id, data, updated_at FROM datasets"
        with self._lock:
            if state is None:
                rows = self._db.execute(query + " ORDER BY id").fetchall()
            else:
                rows = self._db.execute(
                    query + " WHERE json_extract(data, '$.state') = ? ORDER BY id", (state,)
                ).fetchall()
        return [{**json.loads(data), "id": record, "updated_at": updated} for record, data, updated in rows]

    def in_progress_records(self) -> set[str]:
        """Dataset ids whose state is in-progress: a prep job may be reading their raw cache."""
        return {row["id"] for row in self.datasets("in-progress")}

    def sync_datasets(self, prepared: dict[str, dict], *, older_than: float | None = None) -> dict:
        """Mark each given record prepared and drop prepared rows no longer given; other states are untouched.
        With `older_than`, only rows last written before it are dropped: one written during the scan stays."""
        now = time.time()
        with self._transaction() as db:
            for record, fields in prepared.items():
                self._merge_dataset(db, record, {**fields, "state": "prepared"}, now)
            rows = db.execute("SELECT id, updated_at FROM datasets WHERE json_extract(data, '$.state') = 'prepared'")
            stale = [
                record
                for record, updated_at in rows
                if record not in prepared and (older_than is None or updated_at < older_than)
            ]
            db.executemany("DELETE FROM datasets WHERE id = ?", [(record,) for record in stale])
        return {"prepared": len(prepared), "demoted": len(stale)}

    # --- the registry ------------------------------------------------------------------------

    @staticmethod
    def _merge_asset(db: sqlite3.Connection, kind: str, key: str, fields: dict, now: float) -> dict:
        row = db.execute("SELECT data FROM assets WHERE kind = ? AND key = ?", (kind, key)).fetchone()
        merged = {**(json.loads(row[0]) if row else {}), **fields}
        db.execute("INSERT OR REPLACE INTO assets VALUES (?, ?, ?, ?)", (kind, key, json.dumps(merged), now))
        return merged

    def put_asset(self, kind: str, key: str, data: dict) -> dict:
        """Merge `data` into the row (new keys win; a new row when none); returns the merged data."""
        with self._transaction() as db:
            return self._merge_asset(db, kind, key, data, time.time())

    def asset(self, kind: str, key: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT data FROM assets WHERE kind = ? AND key = ?", (kind, key)).fetchone()
        return json.loads(row[0]) if row else None

    def add_ref(self, buffer: str, kind: str, key: str) -> None:
        with self._lock:
            self._db.execute("INSERT OR IGNORE INTO refs VALUES (?, ?, ?)", (buffer, kind, key))

    def refs(self, kind: str, key: str) -> list[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT buffer FROM refs WHERE kind = ? AND key = ? ORDER BY buffer", (kind, key)
            ).fetchall()
        return [row[0] for row in rows]

    def assets(self, kind: str, state: str | None = None, record: str | None = None) -> list[dict]:
        """Every key of `kind` that has a row or a reference: {key, data (None without a row), refs}. With `state`
        or `record`, only rows whose data has that value; a reference without a row passes no filter."""
        with self._lock:
            if state is None and record is None:
                rows = self._db.execute(
                    "SELECT key, data FROM assets WHERE kind = ? UNION SELECT DISTINCT key, NULL FROM refs "
                    "WHERE kind = ? AND key NOT IN (SELECT key FROM assets WHERE kind = ?) ORDER BY key",
                    (kind, kind, kind),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT key, data FROM assets WHERE kind = ? AND (? IS NULL OR json_extract(data, '$.state') = ?) "
                    "AND (? IS NULL OR json_extract(data, '$.record') = ?) ORDER BY key",
                    (kind, state, state, record, record),
                ).fetchall()
        return [
            {"key": key, "data": json.loads(data) if data else None, "refs": self.refs(kind, key)} for key, data in rows
        ]

    def asset_bytes(self, kind: str) -> int:
        """Σ `bytes` over the rows of `kind` that are on disk (state neither missing nor evicted)."""
        with self._lock:
            (total,) = self._db.execute(
                "SELECT COALESCE(SUM(json_extract(data, '$.bytes')), 0) FROM assets WHERE kind = ? "
                "AND COALESCE(json_extract(data, '$.state'), '') NOT IN ('missing', 'evicted')",
                (kind,),
            ).fetchone()
        return int(total)

    def staged_last_used(self) -> dict[str, float]:
        """Artifact id → the newest draw from any volume that lives in that artifact."""
        with self._lock:
            rows = self._db.execute(
                "SELECT json_extract(volumes.data, '$.artifact'), MAX(draws.ts) FROM draws JOIN volumes "
                "ON volumes.buffer = draws.buffer AND volumes.id = json_extract(draws.descriptor, '$.volume') "
                "WHERE json_extract(volumes.data, '$.artifact') IS NOT NULL GROUP BY 1"
            ).fetchall()
        return {artifact: ts for artifact, ts in rows}

    def sync_assets(
        self,
        found: dict[tuple[str, str], dict],
        *,
        older_than: float | None = None,
        kinds: tuple[str, ...] = ("raw", "staged"),
    ) -> dict:
        """Merge what a scan found (a new, stateless, missing or evicted row becomes ready; other states are kept),
        mark `missing` every row of `kinds` the scan did not find unless a job is building it or it is evicted, then
        merge each staged row's `last_used` from the draws (a new row, or a known one without it, starts with its
        `scanned_at`; later scans leave it). With `older_than`, a row written since (by a worker,
        during the scan) is neither merged nor marked: the scan may predate that write."""
        last_used = self.staged_last_used()
        now = time.time()
        with self._transaction() as db:
            rows = {
                (kind, key): (json.loads(data), updated)
                for kind, key, data, updated in db.execute("SELECT kind, key, data, updated FROM assets")
            }
            new = 0
            for (kind, key), fields in found.items():
                known = rows.get((kind, key))
                if known is not None and older_than is not None and known[1] >= older_than:
                    continue
                new += known is None
                if "scanned_at" in fields and (known is None or not known[0].get("last_used")):
                    # a new row (or one from before last_used) was last used when first seen, never later
                    fields = {"last_used": fields["scanned_at"], **fields}
                becomes_ready = known is None or known[0].get("state") in (None, "missing", "evicted")
                self._merge_asset(db, kind, key, {**fields, "state": "ready"} if becomes_ready else fields, now)
            missing = 0
            for (kind, key), (data, updated) in rows.items():
                if kind not in kinds or (kind, key) in found or (older_than is not None and updated >= older_than):
                    continue
                if data.get("state") not in ("fetching", "staging", "waiting-drop", "evicted", "missing"):
                    self._merge_asset(db, kind, key, {"state": "missing"}, now)
                    missing += 1
            for artifact, ts in last_used.items():
                row = db.execute("SELECT data FROM assets WHERE kind = 'staged' AND key = ?", (artifact,)).fetchone()
                if row and ts > (json.loads(row[0]).get("last_used") or 0):
                    self._merge_asset(db, "staged", artifact, {"last_used": ts}, now)
        return {"scanned": len(found), "new": new, "missing": missing}

    def add_consumer(self, buffer: str, info: dict) -> str:
        consumer_id = uuid.uuid4().hex
        with self._lock:
            self._db.execute(
                "INSERT INTO consumers VALUES (?, ?, ?, ?)", (consumer_id, buffer, json.dumps(info), time.time())
            )
        return consumer_id

    def touch(self, buffer: str, consumer_id: str) -> bool:
        with self._lock:
            cursor = self._db.execute(
                "UPDATE consumers SET last_seen = ? WHERE id = ? AND buffer = ?", (time.time(), consumer_id, buffer)
            )
        return cursor.rowcount == 1

    def consumer(self, buffer: str, consumer_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT info FROM consumers WHERE id = ? AND buffer = ?", (consumer_id, buffer)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def max_seq(self, buffer: str) -> int:
        with self._lock:
            (last,) = self._db.execute("SELECT COALESCE(MAX(seq), 0) FROM draws WHERE buffer = ?", (buffer,)).fetchone()
        return int(last)

    def consumers(self, buffer: str, stale_s: float) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT id, info, last_seen FROM consumers WHERE buffer = ? ORDER BY last_seen", (buffer,)
            ).fetchall()
        now = time.time()
        return [
            {**json.loads(info), "consumer_id": consumer_id, "last_seen": seen, "stale": now - seen > stale_s}
            for consumer_id, info, seen in rows
        ]

    def record_draws(self, buffer: str, consumer_id: str, descriptors: list[dict]) -> list[dict]:
        """Assign the next sequence numbers and store the draws, atomically; returns the descriptors with seq."""
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                (last,) = self._db.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM draws WHERE buffer = ?", (buffer,)
                ).fetchone()
                out = [{**descriptor, "seq": last + i + 1} for i, descriptor in enumerate(descriptors)]
                self._db.executemany(
                    "INSERT INTO draws VALUES (?, ?, ?, ?, ?)",
                    [(buffer, d["seq"], consumer_id, json.dumps(d), now) for d in out],
                )
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
        return out

    def frozen(self, buffer: str, split: str) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT descriptor FROM frozen WHERE buffer = ? AND split = ? ORDER BY idx", (buffer, split)
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def extend_frozen(self, buffer: str, split: str, descriptors: list[dict]) -> None:
        with self._lock:
            (start,) = self._db.execute(
                "SELECT COUNT(*) FROM frozen WHERE buffer = ? AND split = ?", (buffer, split)
            ).fetchone()
            self._db.executemany(
                "INSERT INTO frozen VALUES (?, ?, ?, ?)",
                [(buffer, split, start + i, json.dumps(d)) for i, d in enumerate(descriptors)],
            )

    def draws_per_volume(self, buffer: str) -> dict[str, int]:
        with self._lock:
            rows = self._db.execute(
                "SELECT json_extract(descriptor, '$.volume'), COUNT(*) FROM draws WHERE buffer = ? GROUP BY 1 ORDER BY 1",
                (buffer,),
            ).fetchall()
        return {volume: count for volume, count in rows}
