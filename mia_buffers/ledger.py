"""The buffer ledger: one SQLite file holding buffers, their volumes, consumers, every draw and the frozen lists.

Spec v0.5 §5 and §9.5: the ledger is the record of a run; replaying a run means replaying its descriptors.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
import threading
import time
import uuid
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

    def put_buffer(self, buffer: str, config: dict) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO buffers VALUES (?, ?)", (buffer, json.dumps(config)))

    def buffer(self, buffer: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT config FROM buffers WHERE id = ?", (buffer,)).fetchone()
        return json.loads(row[0]) if row else None

    def put_volume(self, buffer: str, volume: Volume) -> None:
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO volumes VALUES (?, ?, ?)",
                (buffer, volume.id, json.dumps(dataclasses.asdict(volume))),
            )

    def volumes(self, buffer: str) -> list[Volume]:
        with self._lock:
            rows = self._db.execute("SELECT data FROM volumes WHERE buffer = ? ORDER BY id", (buffer,)).fetchall()
        return [_volume_from(json.loads(row[0])) for row in rows]

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
