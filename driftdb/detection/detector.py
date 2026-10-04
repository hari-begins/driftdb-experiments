"""
driftdb.detection.detector
============================
C2: Statistical Drift Detector for Query Signals.

Two complementary online detectors, one instance per (query signature,
signal) pair:

* ADWIN (Adaptive Windowing) -- runs directly on the continuous signal
  (latency, selectivity, bytes scanned, ...) and flags a change in the
  underlying mean with a formal false-positive-rate guarantee governed by
  `delta`. Good at catching *gradual* drift because it keeps shrinking its
  window as evidence of change accumulates.

* DDM (Drift Detection Method) -- originally designed for a binary
  classification error stream. We adapt it, as the proposal specifies, by
  converting each new signal value into a binary "surprising" indicator
  relative to a trailing baseline (z-score beyond a threshold => 1, else
  0), then feed that indicator stream to DDM. DDM's warning/drift
  two-level output is what lets the severity classifier distinguish "keep
  watching" from "act now", and it tends to react faster to *abrupt*
  step-changes than ADWIN does.

Both detectors are real river.drift implementations (not reimplemented
here), which is the same "adapt streaming concept-drift algorithms to
query-performance signals" framing the proposal uses for Warper's
generalization.
"""
from __future__ import annotations

import collections
import dataclasses
import enum
import time
from typing import Deque, Dict, Optional

from river.drift import ADWIN
from river.drift.binary import DDM

from ..config import DriftDBConfig


class DriftState(enum.Enum):
    STABLE = "stable"
    WARNING = "warning"
    DRIFT = "drift"


@dataclasses.dataclass
class DriftEvent:
    signature: str
    signal: str
    state: DriftState
    detector: str
    value: float
    baseline_mean: float
    baseline_std: float
    timestamp: float = dataclasses.field(default_factory=time.time)
    resource_confounded: bool = False
    plan_changed: bool = False  # NEW: whether this query's plan shape
    # (join strategy / scanned relations) differs from the last-seen plan
    # for this signature, used by the severity classifier for C5's
    # plan-based component attribution.


class _BaselineTracker:
    """Trailing mean/std used (a) to convert a continuous signal into DDM's
    binary indicator, and (b) to report the pre/post-drift magnitude the
    severity classifier needs."""

    def __init__(self, window: int = 200):
        self.window = window
        self._buf: Deque[float] = collections.deque(maxlen=window)

    def update(self, value: float) -> None:
        self._buf.append(value)

    @property
    def mean(self) -> float:
        return sum(self._buf) / len(self._buf) if self._buf else 0.0

    @property
    def std(self) -> float:
        n = len(self._buf)
        if n < 2:
            return 0.0
        m = self.mean
        var = sum((x - m) ** 2 for x in self._buf) / (n - 1)
        return var ** 0.5

    def z_score(self, value: float) -> float:
        s = self.std
        if s == 0:
            return 0.0
        return (value - self.mean) / s


class SignalDetectorPair:
    """One ADWIN + one DDM instance tracking a single (signature, signal)."""

    def __init__(self, cfg: DriftDBConfig, anomaly_z: float = 2.5):
        self.adwin = ADWIN(delta=cfg.adwin.delta, clock=cfg.adwin.clock,
                            max_buckets=cfg.adwin.max_buckets,
                            min_window_length=cfg.adwin.min_window_length)
        self.ddm = DDM(warm_start=cfg.ddm.min_num_instances,
                        warning_threshold=cfg.ddm.warning_level,
                        drift_threshold=cfg.ddm.drift_level)
        self.baseline = _BaselineTracker()
        self.anomaly_z = anomaly_z
        self.n_seen = 0

    def update(self, value: float) -> Dict[str, DriftState]:
        pre_mean, pre_std = self.baseline.mean, self.baseline.std

        self.adwin.update(value)
        adwin_state = DriftState.DRIFT if self.adwin.drift_detected else DriftState.STABLE

        # Adapt DDM: binarize relative to the *pre-update* baseline so DDM
        # isn't chasing a baseline that already includes the new point.
        anomaly = 1 if (pre_std > 0 and abs((value - pre_mean) / pre_std) > self.anomaly_z) else 0
        self.ddm.update(anomaly)
        if self.ddm.drift_detected:
            ddm_state = DriftState.DRIFT
        elif self.ddm.warning_detected:
            ddm_state = DriftState.WARNING
        else:
            ddm_state = DriftState.STABLE

        self.baseline.update(value)
        self.n_seen += 1
        return {"adwin": adwin_state, "ddm": ddm_state,
                "_pre_mean": pre_mean, "_pre_std": pre_std}


class DriftDetector:
    """Owns a SignalDetectorPair per (signature, signal), plus the
    cluster-utilization confound guard described in C2."""

    def __init__(self, cfg: DriftDBConfig):
        self.cfg = cfg
        self._pairs: Dict[str, SignalDetectorPair] = {}
        self._resource_baseline = _BaselineTracker()
        self._plan_state: Dict[str, tuple] = {} 

    def _key(self, signature: str, signal: str) -> str:
        return f"{signature}::{signal}"

    def note_plan(self, signature: str, join_strategies: str, scan_relations: str) -> bool:
        """Records the current query's plan shape (join strategy choice
        and set of scanned relations) for this signature and returns
        whether it changed from the previously recorded plan. This is
        what lets the severity classifier distinguish "the optimizer's
        plan itself changed" (e.g. a join strategy flip because its cost
        inputs are stale) from "the same plan is just producing different
        numbers" (e.g. underlying data changed but Catalyst kept the same
        physical plan) -- the C5 attribution claim this file's docstring
        makes. Call once per query execution (not per signal)."""
        key = (join_strategies or "", scan_relations or "")
        prev = self._plan_state.get(signature)
        self._plan_state[signature] = key
        if prev is None:
            return False
        return prev != key

    def observe(self, signature: str, signal: str, value: float,
                cluster_active_executors: Optional[int] = None,
                plan_changed: bool = False) -> Optional[DriftEvent]:
        """Feed one new observation. Returns a DriftEvent if either detector
        fired (WARNING or DRIFT), else None."""
        key = self._key(signature, signal)
        if key not in self._pairs:
            self._pairs[key] = SignalDetectorPair(self.cfg)
        pair = self._pairs[key]
        result = pair.update(value)

        # Resource confound guard: if cluster capacity itself moved a lot in
        # the same window, down-weight (but don't discard) the drift signal.
        confounded = False
        if cluster_active_executors is not None:
            pre_res_mean = self._resource_baseline.mean
            self._resource_baseline.update(cluster_active_executors)
            if pre_res_mean > 0:
                rel_change = abs(cluster_active_executors - pre_res_mean) / pre_res_mean
                confounded = rel_change > self.cfg.resource_confound_threshold

        adwin_state, ddm_state = result["adwin"], result["ddm"]
        fired_state = DriftState.STABLE
        detector_name = ""
        if adwin_state == DriftState.DRIFT:
            fired_state, detector_name = DriftState.DRIFT, "adwin"
        if ddm_state != DriftState.STABLE:
            # DDM's own severity (WARNING vs DRIFT) wins if it is stronger
            if ddm_state == DriftState.DRIFT or fired_state == DriftState.STABLE:
                fired_state, detector_name = ddm_state, "ddm"

        if fired_state == DriftState.STABLE:
            return None

        return DriftEvent(
            signature=signature, signal=signal, state=fired_state,
            detector=detector_name, value=value,
            baseline_mean=result["_pre_mean"], baseline_std=result["_pre_std"],
            resource_confounded=confounded, plan_changed=plan_changed,
        )

    def reset(self, signature: str, signal: Optional[str] = None) -> None:
        """Called by the Adaptor after a successful adaptation, so the
        detector doesn't keep re-flagging the same already-handled drift."""
        if signal is not None:
            self._pairs.pop(self._key(signature, signal), None)
        else:
            for k in list(self._pairs.keys()):
                if k.startswith(f"{signature}::"):
                    self._pairs.pop(k, None)
