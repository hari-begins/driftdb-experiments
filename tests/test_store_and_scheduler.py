import tempfile
import shutil

from driftdb.store.model_store import ModelStore
from driftdb.config import DriftDBConfig
from driftdb.metrics.store import MetricsStore
from driftdb.metrics.schema import QueryMetric
from driftdb.scheduler.cost_aware_scheduler import CostAwareScheduler
from driftdb.severity.classifier import Severity, Component, SeverityVerdict


def test_model_store_versions_and_rolls_back():
    d = tempfile.mkdtemp()
    try:
        store = ModelStore(d)
        v1 = store.put("sig1", "cost_model", {"w": 1})
        v2 = store.put("sig1", "cost_model", {"w": 2})
        assert v2.version == v1.version + 1
        assert store.latest("sig1", "cost_model").version == v2.version
        rolled = store.rollback("sig1", "cost_model")
        assert rolled.version == v1.version
        assert store.load_object(rolled) == {"w": 1}
    finally:
        shutil.rmtree(d)


def test_model_store_history_tracks_all_versions():
    d = tempfile.mkdtemp()
    try:
        store = ModelStore(d)
        for i in range(3):
            store.put("sig1", "cardinality_estimator", {"n": i})
        hist = store.history("sig1", "cardinality_estimator")
        assert [h.version for h in hist] == [1, 2, 3]
    finally:
        shutil.rmtree(d)


def _make_metrics_store_with_history(tmpdir, n=100, latency=100.0):
    db = f"{tmpdir}/m.sqlite"
    store = MetricsStore(db)
    for i in range(n):
        store.write(QueryMetric(signature="sig1", raw_plan_digest="x",
                                 execution_id=i, latency_ms=latency,
                                 timestamp=1000.0 + i))
    return store


def test_scheduler_blocks_within_cooldown():
    d = tempfile.mkdtemp()
    try:
        cfg = DriftDBConfig()
        cfg.scheduler.cooldown_seconds = 999
        ms = _make_metrics_store_with_history(d)
        sched = CostAwareScheduler(cfg, ms)
        verdict = SeverityVerdict(signature="sig1", signal="latency_ms",
                                   severity=Severity.MAJOR, component=Component.COST_MODEL,
                                   relative_deviation=2.0, sustained_count=5,
                                   resource_confounded=False)
        go1, _ = sched.should_adapt(verdict, "full_retrain")
        go2, info2 = sched.should_adapt(verdict, "full_retrain")
        assert go1 is True
        assert go2 is False
        assert info2["reason"] == "cooldown"
    finally:
        shutil.rmtree(d)


def test_scheduler_rejects_low_benefit_cost_ratio():
    d = tempfile.mkdtemp()
    try:
        cfg = DriftDBConfig()
        ms = _make_metrics_store_with_history(d, latency=1.0)  # tiny baseline latency
        sched = CostAwareScheduler(cfg, ms)
        verdict = SeverityVerdict(signature="sig1", signal="latency_ms",
                                   severity=Severity.MINOR, component=Component.STATISTICS,
                                   relative_deviation=0.01, sustained_count=1,
                                   resource_confounded=False)
        go, info = sched.should_adapt(verdict, "full_retrain")
        assert go is False
        assert info["reason"] == "ratio"
    finally:
        shutil.rmtree(d)


def test_cost_model_ewma_updates_toward_observed_cost():
    from driftdb.scheduler.cost_aware_scheduler import CostModel
    cfg = DriftDBConfig()
    cm = CostModel(cfg, alpha=0.5)
    before = cm.estimate("refresh_statistics")
    cm.observe("refresh_statistics", 1000.0)
    after = cm.estimate("refresh_statistics")
    assert after > before
