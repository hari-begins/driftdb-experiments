from driftdb.config import DriftDBConfig
from driftdb.detection.detector import DriftEvent, DriftState
from driftdb.severity.classifier import SeverityClassifier, Severity, Component


def _event(signature="sig", signal="selectivity", value=0.05, baseline_mean=0.5,
           state=DriftState.DRIFT, detector="adwin"):
    return DriftEvent(signature=signature, signal=signal, state=state, detector=detector,
                       value=value, baseline_mean=baseline_mean, baseline_std=0.05)


def test_single_firing_stays_minor_even_with_large_deviation():
    cfg = DriftDBConfig()
    clf = SeverityClassifier(cfg)
    verdict = clf.classify(_event())
    assert verdict.severity == Severity.MINOR
    assert verdict.sustained_count == 1


def test_sustained_firing_escalates_severity():
    cfg = DriftDBConfig()
    clf = SeverityClassifier(cfg)
    verdicts = [clf.classify(_event()) for _ in range(cfg.severity.sustained_windows_required)]
    assert verdicts[-1].severity in (Severity.MODERATE, Severity.MAJOR)


def test_streak_resets_on_stable_observation():
    cfg = DriftDBConfig()
    clf = SeverityClassifier(cfg)
    clf.classify(_event())
    clf.classify(_event())
    clf.classify(_event(state=DriftState.STABLE))
    v = clf.classify(_event())
    assert v.sustained_count == 1  # streak restarted


def test_selectivity_signal_attributed_to_cardinality_estimator():
    cfg = DriftDBConfig()
    clf = SeverityClassifier(cfg)
    v = clf.classify(_event(signal="selectivity"))
    assert v.component == Component.CARDINALITY_ESTIMATOR


def test_shuffle_signal_attributed_to_tuned_config():
    cfg = DriftDBConfig()
    clf = SeverityClassifier(cfg)
    v = clf.classify(_event(signal="shuffle_read_bytes"))
    assert v.component == Component.TUNED_CONFIG
