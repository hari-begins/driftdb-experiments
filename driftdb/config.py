"""
driftdb.config
==============
Central configuration for the DriftDB monitor -> detect -> adapt loop.

All values here are real operating knobs (not placeholders) and are
consumed by the metrics collector, drift detectors, severity classifier,
adaptor, and scheduler. Everything can be overridden via environment
variables (prefixed DRIFTDB_) or a YAML file passed to DriftDBConfig.from_yaml.
"""
from __future__ import annotations

import os
import dataclasses
from typing import Optional
import yaml


def _env_float(name: str, default: float) -> float:
    val = os.environ.get(f"DRIFTDB_{name}")
    return float(val) if val is not None else default


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(f"DRIFTDB_{name}")
    return int(val) if val is not None else default


def _env_str(name: str, default: str) -> str:
    return os.environ.get(f"DRIFTDB_{name}", default)


@dataclasses.dataclass
class ADWINConfig:
    delta: float = 0.002          # confidence parameter (smaller = more conservative)
    clock: int = 32               # sub-window compression clock
    max_buckets: int = 5          # exponential histogram buckets per row
    min_window_length: int = 10   # minimum window size before cuts are considered


@dataclasses.dataclass
class DDMConfig:
    warning_level: float = 2.0    # sigma multiplier for WARNING state
    drift_level: float = 3.0      # sigma multiplier for DRIFT state
    min_num_instances: int = 30   # minimum samples before DDM starts flagging


@dataclasses.dataclass
class SeverityThresholds:
    # Relative deviation (fraction) of the post-drift mean from the pre-drift
    # mean of a signal, used to bucket severity once a detector fires.
    minor_max: float = 0.25       # <=25% relative change -> minor
    moderate_max: float = 0.75    # <=75% relative change -> moderate
    # anything above moderate_max -> major
    sustained_windows_required: int = 3  # consecutive detector firings needed
    # before we escalate past "minor", guarding against one-off noise


@dataclasses.dataclass
class SchedulerConfig:
    # Cost-aware gate: only adapt if estimated performance loss from staying
    # stale exceeds the (amortized) cost of adaptation by this margin.
    min_benefit_cost_ratio: float = 1.2
    # Cooldown (seconds) after an adaptation for the same (signature, component)
    # before another adaptation of the same kind is allowed, to avoid thrashing.
    cooldown_seconds: float = 60.0
    # Rolling estimate of cost (seconds) for each remedy type, used until
    # empirical measurements are available (see CostModel in scheduler).
    prior_cost_seconds: dict = dataclasses.field(default_factory=lambda: {
        "refresh_statistics": 5.0,
        "incremental_update": 20.0,
        "full_retrain": 120.0,
        "reset_cache": 1.0,
    })


@dataclasses.dataclass
class DriftDBConfig:
    # Storage
    metrics_db_path: str = "driftdb_metrics.sqlite"
    model_store_path: str = "driftdb_model_store"
    # Detection
    adwin: ADWINConfig = dataclasses.field(default_factory=ADWINConfig)
    ddm: DDMConfig = dataclasses.field(default_factory=DDMConfig)
    signals_to_monitor: tuple = ("latency_ms", "selectivity", "output_rows", "bytes_scanned", "shuffle_read_bytes")
    # Severity
    severity: SeverityThresholds = dataclasses.field(default_factory=SeverityThresholds)
    # Scheduling
    scheduler: SchedulerConfig = dataclasses.field(default_factory=SchedulerConfig)
    # Resource-variability guard: if cluster-level resource utilization (CPU/
    # executor count) itself shifted by more than this fraction at the same
    # time as a signal drift, we down-weight the drift confidence, since it
    # may be infra noise rather than data/workload drift.
    resource_confound_threshold: float = 0.3
    # SLA used for "missed deadline" metric evaluation
    sla_latency_ms: Optional[float] = None
    # Spark session options used only by the live collector
    spark_app_name: str = "driftdb-monitor"
    poll_interval_seconds: float = 2.0

    @staticmethod
    def from_yaml(path: str) -> "DriftDBConfig":
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        cfg = DriftDBConfig()
        for k, v in raw.items():
            if hasattr(cfg, k) and isinstance(v, dict) and dataclasses.is_dataclass(getattr(cfg, k)):
                sub = getattr(cfg, k)
                for sk, sv in v.items():
                    setattr(sub, sk, sv)
            elif hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg

    @staticmethod
    def from_env() -> "DriftDBConfig":
        cfg = DriftDBConfig()
        cfg.metrics_db_path = _env_str("METRICS_DB_PATH", cfg.metrics_db_path)
        cfg.model_store_path = _env_str("MODEL_STORE_PATH", cfg.model_store_path)
        cfg.adwin.delta = _env_float("ADWIN_DELTA", cfg.adwin.delta)
        cfg.ddm.warning_level = _env_float("DDM_WARNING_LEVEL", cfg.ddm.warning_level)
        cfg.ddm.drift_level = _env_float("DDM_DRIFT_LEVEL", cfg.ddm.drift_level)
        cfg.poll_interval_seconds = _env_float("POLL_INTERVAL_SECONDS", cfg.poll_interval_seconds)
        return cfg
