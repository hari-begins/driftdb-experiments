"""
driftdb.severity.classifier
==============================
C5: Drift Severity Classification.

Turns a stream of raw DriftEvents (which fire on almost every signal wiggle
that crosses a statistical threshold) into a much smaller number of
actionable SeverityVerdicts: minor / moderate / major, each attributed to
the learned component most likely responsible.

Severity itself is the relative deviation of the post-event value from the
pre-event trailing baseline, bucketed by SeverityThresholds, but an event
is only escalated past "minor" once it has been seen on
`sustained_windows_required` consecutive OBSERVATIONS for that
(signature, signal) -- not consecutive firings. This distinction matters:
a signal that drifts once, goes quiet for 50 stable queries, then drifts
again should not count as "2 consecutive" toward escalation. To make this
correct, the streak counter must be updated on every observation (via
note_observation()), including the ones that don't fire and therefore
never produce a DriftEvent -- see service.py's _on_metric, which now
calls note_observation() unconditionally and only calls classify() when
an event actually fired.
"""
from __future__ import annotations

import collections
import dataclasses
import enum
import time
from typing import Deque, Dict, Optional

from ..config import DriftDBConfig
from ..detection.detector import DriftEvent, DriftState


class Severity(enum.Enum):
    MINOR = "minor"
    MODERATE = "moderate"
    MAJOR = "major"


class Component(enum.Enum):
    CARDINALITY_ESTIMATOR = "cardinality_estimator"
    COST_MODEL = "cost_model"
    STATISTICS = "statistics"
    TUNED_CONFIG = "tuned_config"
    UNKNOWN = "unknown"


@dataclasses.dataclass
class SeverityVerdict:
    signature: str
    signal: str
    severity: Severity
    component: Component
    relative_deviation: float
    sustained_count: int
    resource_confounded: bool
    timestamp: float = dataclasses.field(default_factory=time.time)


class SeverityClassifier:
    def __init__(self, cfg: DriftDBConfig):
        self.cfg = cfg
        self._streaks: Dict[str, int] = collections.defaultdict(int)
        self._recent: Dict[str, Deque[DriftEvent]] = collections.defaultdict(
            lambda: collections.deque(maxlen=20)
        )

    def _key(self, signature: str, signal: str) -> str:
        return f"{signature}::{signal}"

    def note_observation(self, signature: str, signal: str, fired: bool) -> int:
        """Must be called once per (signature, signal) observation,
        whether or not the detector fired -- this is what makes
        sustained_count reflect consecutive OBSERVATIONS rather than
        consecutive firings with an unbounded number of stable
        observations silently skipped in between. Returns the current
        streak length after this update."""
        key = self._key(signature, signal)
        if fired:
            self._streaks[key] += 1
        else:
            self._streaks[key] = 0
        return self._streaks[key]

    def reset(self, signature: str, signal: Optional[str] = None) -> None:
        """Called by the service after a successful adaptation, mirroring
        DriftDetector.reset(), so a resolved (signature, signal)'s streak
        doesn't carry over stale history into the next detection cycle."""
        if signal is not None:
            self._streaks.pop(self._key(signature, signal), None)
        else:
            prefix = f"{signature}::"
            for k in list(self._streaks.keys()):
                if k.startswith(prefix):
                    self._streaks.pop(k, None)

    def _relative_deviation(self, event: DriftEvent) -> float:
        if event.baseline_mean == 0:
            return 1.0 if event.value != 0 else 0.0
        return abs(event.value - event.baseline_mean) / abs(event.baseline_mean)

    def _bucket(self, rel_dev: float, sustained: int) -> Severity:
        th = self.cfg.severity
        if sustained < th.sustained_windows_required:
            return Severity.MINOR
        if rel_dev <= th.minor_max:
            return Severity.MINOR
        if rel_dev <= th.moderate_max:
            return Severity.MODERATE
        return Severity.MAJOR

    def _attribute_component(self, event: DriftEvent, signature: str) -> Component:
        recent_signals = {e.signal for e in self._recent[signature]}

        if event.signal in ("selectivity", "output_rows"):
            return Component.CARDINALITY_ESTIMATOR

        if event.signal == "latency_ms":
            # Plan-based attribution (C5): a latency drift accompanied by
            # an actual join-strategy/scan-shape change means the
            # optimizer's plan choice flipped -- its cost/cardinality
            # inputs are stale, so the remedy should target the cost
            # model rather than assume pure resource contention.
            return Component.COST_MODEL

        if event.signal == "bytes_scanned":
            # Plan-based attribution (C5): stable selectivity, stable
            # latency, AND a stable plan (no join/scan-shape change)
            # means the extra bytes scanned reflect stale table
            # statistics alone -- the cheapest remedy -- rather than a
            # cardinality-estimation problem that also needs the plan
            # to be re-optimized.
            if (not event.plan_changed
                    and "selectivity" not in recent_signals
                    and "latency_ms" not in recent_signals):
                return Component.STATISTICS
            return Component.CARDINALITY_ESTIMATOR

        if event.signal == "shuffle_read_bytes":
            return Component.TUNED_CONFIG

        return Component.UNKNOWN

    def classify(self, event: DriftEvent, sustained: int) -> SeverityVerdict:
        """sustained must come from note_observation(), called by the
        caller on this same observation before classify()."""
        rel_dev = self._relative_deviation(event)
        severity = self._bucket(rel_dev, sustained)
        component = self._attribute_component(event, event.signature)

        self._recent[event.signature].append(event)

        return SeverityVerdict(
            signature=event.signature, signal=event.signal, severity=severity,
            component=component, relative_deviation=rel_dev,
            sustained_count=sustained, resource_confounded=event.resource_confounded,
        )