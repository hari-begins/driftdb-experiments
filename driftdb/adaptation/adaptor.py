"""
driftdb.adaptation.adaptor
=============================
C3: Component-Level Selective Adaptation.

Maps a SeverityVerdict (signature, signal, severity, attributed component)
to the *cheapest sufficient* concrete remedy from remedies.py, following the
proposal's severity -> remedy mapping:

    minor    -> refresh_statistics / reset_cache   (cheap, targeted)
    moderate -> incremental_update                 (online learning nudge)
    major    -> full_retrain                       (expensive, thorough)

and always attributed to the *specific* component the SeverityClassifier
named, rather than reacting with a single "retrain everything" action --
this is the generalization of Warper's targeted-update-query philosophy the
proposal's C3 asks for.

remedy_name_for() is the single source of truth for this mapping. It is
called both by CostAwareScheduler (via service.py, to estimate cost
*before* deciding whether to adapt) and by adapt() itself (to actually
execute). Previously service.py duplicated this mapping using severity
alone, missing the component-level branching below -- so for e.g. a MINOR
verdict on tuned_config/cost_model, the scheduler evaluated
refresh_statistics's cost while the Adaptor actually ran the much cheaper
reset_cache, making the cost/benefit gate untrustworthy. Routing both
call sites through this one method removes that possibility structurally.
"""
from __future__ import annotations

import dataclasses
import logging
import time
from typing import Any, Dict, List, Optional

from ..metrics.store import MetricsStore
from ..store.model_store import ModelStore
from ..severity.classifier import Component, Severity, SeverityVerdict
from . import remedies

logger = logging.getLogger("driftdb.adaptation.adaptor")


@dataclasses.dataclass
class AdaptationResult:
    verdict: SeverityVerdict
    remedy_name: str
    detail: Dict[str, Any]
    # Whether the remedy genuinely completed, as opposed to merely being
    # invoked. A remedy call can return normally while every relation it
    # touched was actually skipped, so this must be computed from the
    # remedy's own per-relation detail rather than assumed True just
    # because adapt() didn't raise.
    success: bool = True
    timestamp: float = dataclasses.field(default_factory=time.time)


def _remedy_succeeded(detail: Dict[str, Any]) -> bool:
    """Inspect a remedy's detail dict for evidence of a real failure.

    remedies.refresh_statistics / reset_cache report per-relation outcomes
    in detail["relations"] as either "ok" or "skipped: <error>". Any
    "skipped: ..." entry means that relation's operation did not actually
    happen, even though the Python call itself didn't raise."""
    relations = detail.get("relations")
    if not relations:
        return True
    return all(not str(v).startswith("skipped:") for v in relations.values())


class Adaptor:
    def __init__(self, spark, metrics_store: MetricsStore, model_store: ModelStore):
        self.spark = spark
        self.metrics_store = metrics_store
        self.model_store = model_store

    def _feature_target_rows(self, signature: str, target_signal: str):
        rows = self.metrics_store.latest(signature, n=500)
        rows = list(reversed(rows))  # chronological
        feats, targets = [], []
        for r in rows:
            feats.append((
                r.get("input_rows") or 0,
                r.get("bytes_scanned") or 0,
                r.get("shuffle_read_bytes") or 0,
                r.get("cluster_active_executors") or 0,
            ))
            targets.append(r.get(target_signal) or 0.0)
        return feats, targets

    def _relations_for(self, signature: str) -> List[str]:
        rows = self.metrics_store.latest(signature, n=1)
        if not rows:
            return []
        rel_str = rows[0].get("scan_relations") or ""
        return [r for r in rel_str.split(",") if r]

    def _target_signal_for(self, verdict: SeverityVerdict) -> str:
        return "selectivity" if verdict.component == Component.CARDINALITY_ESTIMATOR else "latency_ms"

    def remedy_name_for(self, verdict: SeverityVerdict) -> str:
        """The one place severity+component maps to a remedy name. See
        module docstring for why this must be the only place."""
        if verdict.severity == Severity.MINOR:
            if verdict.component == Component.STATISTICS or verdict.component == Component.CARDINALITY_ESTIMATOR:
                return "refresh_statistics"
            return "reset_cache"

        elif verdict.severity == Severity.MODERATE:
            target_signal = self._target_signal_for(verdict)
            feats, _ = self._feature_target_rows(verdict.signature, target_signal)
            if len(feats) < 2:
                # Not enough history yet -- Adaptor falls back to the cheap
                # remedy rather than fitting on almost nothing.
                return "refresh_statistics"
            return "incremental_update"

        else:  # MAJOR
            return "full_retrain"

    def adapt(self, verdict: SeverityVerdict) -> AdaptationResult:
        component = verdict.component.value
        target_signal = self._target_signal_for(verdict)
        remedy_name = self.remedy_name_for(verdict)

        if remedy_name == "refresh_statistics":
            relations = self._relations_for(verdict.signature)
            detail = remedies.refresh_statistics(self.spark, relations)

        elif remedy_name == "reset_cache":
            relations = self._relations_for(verdict.signature)
            detail = remedies.reset_cache(self.spark, relations)

        elif remedy_name == "incremental_update":
            feats, targets = self._feature_target_rows(verdict.signature, target_signal)
            detail = remedies.incremental_update(
                self.model_store, verdict.signature, component, feats, targets)

        else:  # full_retrain
            feats, targets = self._feature_target_rows(verdict.signature, target_signal)
            detail = remedies.full_retrain(
                self.model_store, verdict.signature, component, feats, targets)
            if verdict.component == Component.TUNED_CONFIG:
                tune_detail = remedies.tune_config(self.spark, verdict.signal)
                detail = {**detail, "tune_config": tune_detail}

        success = _remedy_succeeded(detail)

        logger.info("Adapted signature=%s component=%s severity=%s -> %s (success=%s)",
                    verdict.signature, component, verdict.severity.value, remedy_name, success)
        return AdaptationResult(verdict=verdict, remedy_name=remedy_name,
                                 detail=detail, success=success)