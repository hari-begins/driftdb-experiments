"""
driftdb.scheduler.cost_aware_scheduler
=========================================
C4: Cost-Aware Adaptation Scheduler.

Before the Adaptor is actually invoked, the scheduler weighs:

  estimated_benefit_seconds -- how much aggregate query time we expect to
      keep losing per hour if we do *not* adapt, extrapolated from the
      signal's relative deviation, the query's own baseline latency, and
      how often this query signature actually recurs (queries/hour, from
      the MetricsStore's own history -- a query template nobody runs isn't
      worth adapting for even under major drift).

  estimated_cost_seconds -- an empirical, continuously-updated (EWMA)
      estimate of how long each remedy actually takes, seeded from
      config.scheduler.prior_cost_seconds and corrected every time
      remedies.py reports a real `duration_s`.

Adaptation proceeds only if benefit/cost clears `min_benefit_cost_ratio`,
and a per-(signature, component) cooldown prevents thrashing -- two
consecutive drift firings on the same component don't each trigger a fresh
retrain while the previous one is still "settling in".
"""
from __future__ import annotations

import collections
import logging
import time
from typing import Dict, Optional, Tuple

from ..config import DriftDBConfig
from ..metrics.store import MetricsStore
from ..severity.classifier import SeverityVerdict

logger = logging.getLogger("driftdb.scheduler")


class CostModel:
    """EWMA-updated per-remedy cost estimate, seeded from config priors."""

    def __init__(self, cfg: DriftDBConfig, alpha: float = 0.3):
        self.alpha = alpha
        self._estimate: Dict[str, float] = dict(cfg.scheduler.prior_cost_seconds)

    def estimate(self, remedy: str) -> float:
        return self._estimate.get(remedy, 30.0)

    def observe(self, remedy: str, actual_seconds: float) -> None:
        prev = self._estimate.get(remedy, actual_seconds)
        self._estimate[remedy] = self.alpha * actual_seconds + (1 - self.alpha) * prev


class CostAwareScheduler:
    def __init__(self, cfg: DriftDBConfig, metrics_store: MetricsStore):
        self.cfg = cfg
        self.metrics_store = metrics_store
        self.cost_model = CostModel(cfg)
        self._last_adapted: Dict[Tuple[str, str], float] = {}

    def _cooldown_key(self, verdict: SeverityVerdict) -> Tuple[str, str]:
        return (verdict.signature, verdict.component.value)

    def _queries_per_hour(self, signature: str) -> float:
        series = self.metrics_store.signal_series(signature, "timestamp", limit=200)
        if len(series) < 2:
            return 1.0
        span_s = max(series[-1] - series[0], 1.0)
        return len(series) / (span_s / 3600.0)

    def _baseline_latency_ms(self, signature: str) -> float:
        series = self.metrics_store.signal_series(signature, "latency_ms", limit=200)
        return (sum(series) / len(series)) if series else 1000.0

    def estimate_benefit_seconds(self, verdict: SeverityVerdict, horizon_hours: float = 1.0) -> float:
        qph = self._queries_per_hour(verdict.signature)
        baseline_latency_s = self._baseline_latency_ms(verdict.signature) / 1000.0
        # Expected extra time per future execution if we do nothing, as a
        # fraction of baseline latency driven by how far the signal has moved.
        extra_time_per_query_s = baseline_latency_s * min(verdict.relative_deviation, 5.0)
        expected_future_queries = qph * horizon_hours
        return max(extra_time_per_query_s * expected_future_queries, 0.0)

    def should_adapt(self, verdict: SeverityVerdict, remedy_name: str) -> Tuple[bool, dict]:
        key = self._cooldown_key(verdict)
        now = time.time()
        last = self._last_adapted.get(key)
        if last is not None and (now - last) < self.cfg.scheduler.cooldown_seconds:
            return False, {"reason": "cooldown", "seconds_remaining":
                            self.cfg.scheduler.cooldown_seconds - (now - last)}

        if verdict.resource_confounded:
            # Down-weight (require a much stronger signal) rather than
            # outright block, per C2's confound guard.
            if verdict.relative_deviation < self.cfg.severity.moderate_max:
                return False, {"reason": "resource_confounded_insufficient_deviation"}

        benefit = self.estimate_benefit_seconds(verdict)
        cost = self.cost_model.estimate(remedy_name)
        ratio = benefit / cost if cost > 0 else float("inf")
        decision = ratio >= self.cfg.scheduler.min_benefit_cost_ratio
        info = {"reason": "ratio", "benefit_s": benefit, "cost_s": cost, "ratio": ratio}
        if decision:
            self._last_adapted[key] = now
        return decision, info

    def record_actual_cost(self, remedy_name: str, actual_seconds: float) -> None:
        self.cost_model.observe(remedy_name, actual_seconds)
