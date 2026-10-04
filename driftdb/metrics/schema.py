"""
driftdb.metrics.schema
=======================
Canonical shape of a single query-execution observation (C1: Unified
Query-Performance Monitoring Signal from the proposal). Every signal the
drift detectors consume is a field on this record.
"""
from __future__ import annotations

import dataclasses
import time
from typing import Optional


@dataclasses.dataclass
class QueryMetric:
    # Identity
    signature: str                 # normalized query template hash (see signature.py)
    raw_plan_digest: str            # digest of the *unnormalized* plan, for debugging
    execution_id: Optional[int]     # Spark SQL execution id, if resolvable
    timestamp: float = dataclasses.field(default_factory=time.time)
    func_name: str = ""             # Spark action that triggered it ("collect", ...)

    # Timing
    latency_ms: float = 0.0

    # Volume / selectivity signals
    input_rows: int = 0
    output_rows: int = 0
    bytes_read: int = 0
    bytes_scanned: int = 0          # alias kept explicit for detector config parity
    selectivity: float = 1.0        # geometric mean over relations
    selectivity_by_relation: str = "{}"   # JSON: {"item": 0.0008, ...}
    shuffle_partition_bytes: int = 0      # AQE partitionDataSize

    # Resource / shuffle signals (used for the confound guard: resource
    # variability vs. genuine data/workload drift)
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    peak_executor_memory_bytes: int = 0
    num_tasks: int = 0
    executor_run_time_ms: float = 0.0
    cluster_active_executors: int = 0

    # Plan choice (used by severity classifier to attribute drift to a
    # component: join strategy changes point at the cost model / cardinality
    # estimator, not at stale statistics)
    join_strategies: str = ""       # comma-joined, e.g. "SortMergeJoin,BroadcastHashJoin"
    scan_relations: str = ""        # comma-joined base relations touched

    # Workload context (ground truth for evaluation)
    literal_values: str = "{}"      # JSON: {"i_manufact_id =": 345, "d_moy =": 11}
    run_id: str = ""                # one experiment run
    phase: str = ""                 # e.g. "stable", "gradual", "abrupt"
    is_warmup: bool = False         # True = exclude from detector input

    # Outcome
    failed: bool = False
    error: Optional[str] = None

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)

    @staticmethod
    def columns() -> list:
        return [f.name for f in dataclasses.fields(QueryMetric)]