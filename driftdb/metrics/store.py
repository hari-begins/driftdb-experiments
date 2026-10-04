"""
driftdb.metrics.store
=======================
Durable, append-only store for QueryMetric observations, backed by SQLite
(safe for a single monitoring process; swap the connection for a real
warehouse table if you need multi-writer concurrency).

The INSERT statement is generated from QueryMetric.columns(), so adding a
field to schema.py only needs a matching column added to _CREATE_SQL below.
"""
from __future__ import annotations

import sqlite3
import threading
from typing import Iterable, List, Optional

from .schema import QueryMetric

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS query_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signature TEXT NOT NULL,
    raw_plan_digest TEXT,
    execution_id INTEGER,
    timestamp REAL NOT NULL,
    func_name TEXT,
    latency_ms REAL,
    input_rows INTEGER,
    output_rows INTEGER,
    bytes_read INTEGER,
    bytes_scanned INTEGER,
    selectivity REAL,
    shuffle_read_bytes INTEGER,
    shuffle_write_bytes INTEGER,
    shuffle_partition_bytes INTEGER,
    selectivity_by_relation TEXT,
    peak_executor_memory_bytes INTEGER,
    num_tasks INTEGER,
    executor_run_time_ms REAL,
    cluster_active_executors INTEGER,
    join_strategies TEXT,
    scan_relations TEXT,
    literal_values TEXT,
    run_id TEXT,
    phase TEXT,
    is_warmup INTEGER,
    failed INTEGER,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_signature_ts ON query_metrics(signature, timestamp);
CREATE INDEX IF NOT EXISTS idx_run ON query_metrics(run_id, timestamp);
"""

_COLS = QueryMetric.columns()
_INSERT_SQL = (
    f"INSERT INTO query_metrics ({', '.join(_COLS)}) "
    f"VALUES ({', '.join('?' for _ in _COLS)})"
)


class MetricsStore:
    """Thread-safe wrapper around a SQLite metrics log."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.executescript(_CREATE_SQL)
        self._conn.commit()

        # Fail fast if schema.py and an existing table disagree (e.g. an
        # old .sqlite file from before new fields were added).
        cur = self._conn.execute("PRAGMA table_info(query_metrics)")
        table_cols = {r[1] for r in cur.fetchall()}
        missing = [c for c in _COLS if c not in table_cols]
        if missing:
            raise RuntimeError(
                f"Existing DB {db_path} is missing columns {missing}. "
                f"Delete the old .sqlite file and rerun."
            )

    def write(self, metric: QueryMetric) -> None:
        row = metric.as_dict()
        values = tuple(
            int(row[c]) if isinstance(row[c], bool) else row[c] for c in _COLS
        )
        with self._lock:
            self._conn.execute(_INSERT_SQL, values)
            self._conn.commit()

    def write_many(self, metrics: Iterable[QueryMetric]) -> None:
        for m in metrics:
            self.write(m)

    def signal_series(self, signature: str, signal: str, limit: int = 5000,
                      include_warmup: bool = False) -> List[float]:
        """Time-ordered values of `signal` for one query signature -- the
        raw input to the drift detectors. Warm-up runs are excluded unless
        include_warmup=True."""
        if signal not in QueryMetric.columns():
            raise ValueError(f"Unknown signal column: {signal}")
        warm = "" if include_warmup else "AND is_warmup=0 "
        with self._lock:
            cur = self._conn.execute(
                f"SELECT {signal} FROM query_metrics WHERE signature=? "
                f"AND failed=0 {warm}ORDER BY timestamp ASC LIMIT ?",
                (signature, limit),
            )
            return [r[0] for r in cur.fetchall() if r[0] is not None]

    def distinct_signatures(self) -> List[str]:
        with self._lock:
            cur = self._conn.execute("SELECT DISTINCT signature FROM query_metrics")
            return [r[0] for r in cur.fetchall()]

    def latest(self, signature: str, n: int = 1) -> List[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM query_metrics WHERE signature=? "
                "ORDER BY timestamp DESC LIMIT ?",
                (signature, n),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def run_rows(self, run_id: str) -> List[dict]:
        """All rows of one experiment run, in time order (ground truth for
        evaluating detection latency / false positives)."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM query_metrics WHERE run_id=? ORDER BY timestamp ASC",
                (run_id,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def cluster_utilization_series(self, limit: int = 5000) -> List[int]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT cluster_active_executors FROM query_metrics "
                "ORDER BY timestamp ASC LIMIT ?", (limit,),
            )
            return [r[0] for r in cur.fetchall() if r[0] is not None]

    def count(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) FROM query_metrics")
            return cur.fetchone()[0]

    def close(self):
        self._conn.close()