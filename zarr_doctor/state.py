"""SQLite state owned by the service: known volumes + a small key/value table."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS volumes (
    path TEXT PRIMARY KEY, issue_number INTEGER, fingerprint TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS poll_cursor (key TEXT PRIMARY KEY, value TEXT);
"""


class State:
    def __init__(self, state_dir: Path, readonly: bool = False):
        self.path = Path(state_dir) / "zarr_doctor.sqlite"
        self.readonly = readonly
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._conn() as c:
                c.executescript(_SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        if self.readonly:
            return sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        return sqlite3.connect(self.path, timeout=30)

    def _query(self, sql: str, args=()) -> list[tuple]:
        if self.readonly and not self.path.exists():
            return []
        with self._conn() as c:
            try:
                return c.execute(sql, args).fetchall()
            except sqlite3.OperationalError:  # read-only and tables never created
                if self.readonly:
                    return []
                raise

    def get_volume(self, path: str) -> tuple[int, str] | None:
        rows = self._query("SELECT issue_number, fingerprint FROM volumes WHERE path=?", (path,))
        return rows[0] if rows else None

    def path_for_issue(self, issue: int) -> str | None:
        rows = self._query("SELECT path FROM volumes WHERE issue_number=?", (issue,))
        return rows[0][0] if rows else None

    def count_volumes(self) -> int:
        return self._query("SELECT count(*) FROM volumes")[0][0] if self.path.exists() else 0

    def upsert_volume(self, path: str, issue: int, fingerprint: str) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO volumes VALUES (?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                "issue_number=excluded.issue_number, fingerprint=excluded.fingerprint",
                (path, issue, fingerprint, datetime.now(timezone.utc).isoformat()),
            )

    def get(self, key: str) -> str | None:
        rows = self._query("SELECT value FROM poll_cursor WHERE key=?", (key,))
        return rows[0][0] if rows else None

    def set(self, key: str, value: str) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO poll_cursor VALUES (?,?)", (key, value))
