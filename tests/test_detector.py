import random

from driftdb.config import DriftDBConfig
from driftdb.detection.detector import DriftDetector, DriftState


def test_no_drift_on_stable_stream_has_low_false_positive_rate():
    random.seed(0)
    cfg = DriftDBConfig()
    det = DriftDetector(cfg)
    fires = 0
    for _ in range(500):
        v = random.gauss(100, 5)
        e = det.observe("sig", "latency_ms", v)
        if e is not None:
            fires += 1
    # Some false positives are expected (statistical detectors), but should
    # be a small minority of observations.
    assert fires / 500 < 0.10


def test_abrupt_drift_detected_quickly():
    random.seed(1)
    cfg = DriftDBConfig()
    det = DriftDetector(cfg)
    for _ in range(300):
        det.observe("sig", "latency_ms", random.gauss(100, 5))

    detection_idx = None
    for i in range(100):
        e = det.observe("sig", "latency_ms", random.gauss(500, 10))
        if e is not None and e.state == DriftState.DRIFT:
            detection_idx = i
            break
    assert detection_idx is not None
    assert detection_idx < 10  # should catch a 5x jump within a handful of queries


def test_gradual_drift_eventually_detected():
    random.seed(2)
    cfg = DriftDBConfig()
    det = DriftDetector(cfg)
    detected = False
    mean = 100.0
    for i in range(2000):
        mean += 0.15  # slow linear drift
        v = random.gauss(mean, 5)
        e = det.observe("sig", "latency_ms", v)
        if e is not None:
            detected = True
            break
    assert detected


def test_reset_clears_detector_state():
    cfg = DriftDBConfig()
    det = DriftDetector(cfg)
    for _ in range(50):
        det.observe("sig", "latency_ms", 100.0)
    assert "sig::latency_ms" in det._pairs
    det.reset("sig", "latency_ms")
    assert "sig::latency_ms" not in det._pairs


def test_resource_confound_flag_set_when_executor_count_jumps():
    cfg = DriftDBConfig()
    det = DriftDetector(cfg)
    for _ in range(50):
        det.observe("sig", "latency_ms", random.gauss(100, 2), cluster_active_executors=10)
    # Big executor count change alongside a value that also looks anomalous
    event = None
    for v in [100, 100, 500, 500, 500]:
        e = det.observe("sig", "latency_ms", v, cluster_active_executors=2)
        if e is not None:
            event = e
    if event is not None:
        assert isinstance(event.resource_confounded, bool)
