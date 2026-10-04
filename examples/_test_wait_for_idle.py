# examples/_test_wait_for_idle.py  (throwaway, not part of the suite)
# examples/_test_worker_queue.py (throwaway)
# 1. monkeypatch driftdb.adaptation.remedies.refresh_statistics with a
#    version that sleeps 5s then returns success
# 2. start the service, feed metrics that trigger 2 drift events back-to-back
#    (same signature, two different signals) fast, on the "listener" thread
#    (call service._on_metric(...) directly twice with no delay)
# 3. assert _on_metric() returns near-instantly both times (proves the
#    listener thread never blocks)
# 4. assert wait_for_idle(timeout=1) is False immediately after
# 5. assert wait_for_idle(timeout=15) is True once the worker catches up
# 6. assert exactly 2 adaptation_completed alerts were emitted, serialized
#    (second remedy didn't start until the first finished)
import time
import threading
from unittest.mock import patch

from driftdb import DriftDBService, DriftDBConfig
from driftdb.severity.classifier import SeverityVerdict, Severity, Component

def fake_slow_refresh(spark, relations):
    time.sleep(5)  # stand-in for a real ~20s ANALYZE TABLE
    return {"remedy": "refresh_statistics",
            "relations": {r: "ok" for r in relations},
            "duration_s": 5.0}

# ... build a minimal spark session + service, monkeypatch
# driftdb.adaptation.remedies.refresh_statistics with fake_slow_refresh,
# call service._maybe_adapt(verdict) on a background thread, then assert:
#   1. wait_for_idle(timeout=1) returns False while the thread is mid-sleep
#   2. wait_for_idle(timeout=10) returns True after it finishes
#   3. exactly one adaptation_completed alert is emitted, after the sleep