"""
driftdb.service
=================
DriftDBService wires every component in the proposal's architecture
diagram into the single monitor -> detect -> adapt control loop:

    SparkMetricsCollector  (Metrics Collector)
          |  QueryMetric
          v
    DriftDetector          (Drift Detector: ADWIN + adapted DDM)
          |  DriftEvent
          v
    SeverityClassifier     (Severity Classifier: minor/moderate/major + component)
          |  SeverityVerdict
          v
    CostAwareScheduler     (gate: benefit/cost, cooldown, resource-confound)
          |  go/no-go
          v
    Adaptor                (component-targeted remedy) -- backed by --
    ModelStore              (versioned cost/cardinality/config store)
          |
          v
    AlertingInterface       (surfaces every step; supports manual override)

This is the executable version of the proposal's Section "System
Architecture" and its C1-C6 contributions.

Threading note (adaptation worker):
Spark's QueryExecutionListener contract expects callbacks to be fast
("Spark calls these synchronously after each action, so keep the work
small" -- see collector.py's _PyQueryExecutionListener docstring). Running
a real remedy (e.g. a 15-30s ANALYZE TABLE) synchronously inside that
callback caused two bugs: (1) spark.stop() racing an in-flight remedy on
that same thread, and (2) every QueryMetric's timestamp (stamped at
construction time inside the callback) getting compressed into a burst
right after the blocking remedy returned, corrupting wall-clock ordering.

Fix: the listener thread (via _on_metric -> _maybe_adapt) only decides
whether to adapt and enqueues the verdict; a single dedicated worker
thread drains that queue and actually calls Adaptor.adapt().

Pending-signature guard (added after the worker split):
Moving adapt() off the listener thread removed an implicit exclusivity
the old synchronous design had "for free" -- because adapt() used to
block the listener thread, no new metric for the same signature could
arrive and re-trigger the *same*, not-yet-resolved drift until
detector.reset() had already run. With adapt() now async, queries kept
flowing while the first remedy was still executing, kept re-detecting
the same unresolved drift, and produced duplicate/inflated adaptations
for one real drift event. _pending_signatures restores "at most one
outstanding adaptation per signature" without going back to blocking the
listener thread: a signature is marked pending when its verdict is
queued and released only once the worker has actually finished handling
it (success, failure, or exception).
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Optional

from .config import DriftDBConfig
from .metrics.collector import SparkMetricsCollector
from .metrics.schema import QueryMetric
from .metrics.store import MetricsStore
from .detection.detector import DriftDetector
from .severity.classifier import SeverityClassifier
from .scheduler.cost_aware_scheduler import CostAwareScheduler
from .adaptation.adaptor import Adaptor
from .store.model_store import ModelStore
from .alerting.interface import AlertingInterface, Alert

logger = logging.getLogger("driftdb.service")

# Sentinel telling the worker thread to exit.
_SHUTDOWN = object()


class DriftDBService:
    def __init__(self, spark, cfg: Optional[DriftDBConfig] = None,
                 webhook_url: Optional[str] = None):
        self.spark = spark
        self.cfg = cfg or DriftDBConfig()

        self.metrics_store = MetricsStore(self.cfg.metrics_db_path)
        self.model_store = ModelStore(self.cfg.model_store_path)
        self.detector = DriftDetector(self.cfg)
        self.severity_classifier = SeverityClassifier(self.cfg)
        self.scheduler = CostAwareScheduler(self.cfg, self.metrics_store)
        self.adaptor = Adaptor(spark, self.metrics_store, self.model_store)
        self.alerting = AlertingInterface(webhook_url=webhook_url)

        self.collector = SparkMetricsCollector(spark, self.metrics_store,
                                                on_metric=self._on_metric)

        # simple counters for the evaluation harness (Section "Metrics")
        self.stats = dict(observed=0, drift_events=0, adaptations=0,
                           adaptations_skipped=0, adaptations_failed=0,
                           missed_deadlines=0)
        # Protects self.stats and self.alerting.emit() calls made from the
        # worker thread, since the listener thread touches both too.
        self._stats_lock = threading.Lock()

        # -- adaptation worker ------------------------------------------
        self._adapt_queue: "queue.Queue" = queue.Queue()
        self._adapting = threading.Event()  # set while a remedy is running
        self._worker_thread: Optional[threading.Thread] = None

        # -- pending-signature guard (see module docstring) --------------
        self._pending_keys: set = set()
        self._pending_lock = threading.Lock()

    def start(self) -> None:
        self._worker_thread = threading.Thread(
            target=self._adapt_worker_loop, name="driftdb-adapt-worker",
            daemon=True,
        )
        self._worker_thread.start()
        self.collector.start()
        logger.info("DriftDB control loop started (signals=%s)",
                    self.cfg.signals_to_monitor)

    def wait_for_idle(self, timeout: float = 120.0, quiet_period: float = 3.0) -> bool:
        """Block until the adaptation queue/worker are idle AND the metrics
        store has stopped receiving new rows for `quiet_period` seconds --
        the latter catches a query callback still in flight in the JVM's
        listener-bus queue, which could otherwise trigger a fresh verdict
        after this function would have naively looked idle."""
        deadline = time.time() + timeout
        last_count = self.metrics_store.count()
        stable_since = time.time()
        while True:
            cur_count = self.metrics_store.count()
            if cur_count != last_count:
                last_count = cur_count
                stable_since = time.time()
            queue_idle = self._adapt_queue.empty() and not self._adapting.is_set()
            quiet_long_enough = (time.time() - stable_since) >= quiet_period
            if queue_idle and quiet_long_enough:
                return True
            if time.time() >= deadline:
                logger.warning(
                    "wait_for_idle timed out after %.1fs (queue_size=%d, "
                    "adapting=%s, store_count=%d)", timeout,
                    self._adapt_queue.qsize(), self._adapting.is_set(), cur_count,
                )
                return False
            time.sleep(0.5)

    def stop(self) -> None:
        self.collector.stop()
        if self._worker_thread is not None:
            self._adapt_queue.put(_SHUTDOWN)
            self._worker_thread.join(timeout=5)
            self._worker_thread = None

    # -- the control loop itself ---------------------------------------------

    def _on_metric(self, metric: QueryMetric) -> None:
        # Runs on Spark's listener-bus callback thread. Keep this fast:
        # detection only, never remedy execution (see module docstring).
        self.stats["observed"] += 1

        if self.cfg.sla_latency_ms and metric.latency_ms > self.cfg.sla_latency_ms and not metric.failed:
            self.stats["missed_deadlines"] += 1

        if metric.failed:
            return

        plan_changed = self.detector.note_plan(
            metric.signature, metric.join_strategies, metric.scan_relations,
        )

        for signal in self.cfg.signals_to_monitor:
            value = getattr(metric, signal, None)
            if value is None:
                continue
            event = self.detector.observe(
                metric.signature, signal, float(value),
                cluster_active_executors=metric.cluster_active_executors,
                plan_changed=plan_changed,
            )
            sustained = self.severity_classifier.note_observation(
                metric.signature, signal, fired=event is not None,
            )
            if event is None:
                continue

            self.stats["drift_events"] += 1
            verdict = self.severity_classifier.classify(event, sustained)
            self.alerting.emit(Alert(
                kind="drift_detected", signature=verdict.signature,
                component=verdict.component.value, severity=verdict.severity.value,
                detail=dict(signal=verdict.signal, relative_deviation=verdict.relative_deviation,
                            sustained_count=verdict.sustained_count,
                            resource_confounded=verdict.resource_confounded),
            ))

            if self.alerting.is_suppressed(verdict.signature, verdict.component.value):
                self.stats["adaptations_skipped"] += 1
                self.alerting.emit(Alert(
                    kind="adaptation_skipped", signature=verdict.signature,
                    component=verdict.component.value, severity=verdict.severity.value,
                    detail={"reason": "manually_suppressed"},
                ))
                continue

            self._maybe_adapt(verdict)

    def _maybe_adapt(self, verdict) -> None:
        # Still runs on the listener thread, but only does the cheap
        # go/no-go check plus the pending-signature guard -- the actual
        # remedy is handed off to the worker thread via the queue, so this
        # returns immediately either way.
        #
        # remedy_name comes from Adaptor.remedy_name_for(), the same
        # method adapt() itself uses to pick a remedy -- so the
        # scheduler's cost/benefit estimate and what actually executes
        # can never diverge (see adaptor.py module docstring).
        remedy_name = self.adaptor.remedy_name_for(verdict)
        go, info = self.scheduler.should_adapt(verdict, remedy_name)
        if not go:
            self.stats["adaptations_skipped"] += 1
            self.alerting.emit(Alert(
                kind="adaptation_skipped", signature=verdict.signature,
                component=verdict.component.value, severity=verdict.severity.value,
                detail=info,
            ))
            return
        key = (verdict.signature, verdict.component)
        with self._pending_lock:
            if key in self._pending_keys:
                # An adaptation for this signature is already queued or
                # in flight. This new verdict is almost certainly a
                # re-detection of the same, not-yet-resolved drift (the
                # worker hasn't called detector.reset() yet) rather than a
                # genuinely new, independent drift event -- skip it rather
                # than piling up redundant remedies.
                self.stats["adaptations_skipped"] += 1
                self.alerting.emit(Alert(
                    kind="adaptation_skipped", signature=verdict.signature,
                    component=verdict.component.value, severity=verdict.severity.value,
                    detail={"reason": "adaptation_already_pending"},
                ))
                return
            self._pending_keys.add(key)

        self._adapt_queue.put(verdict)

    def _adapt_worker_loop(self) -> None:
        """Runs on its own dedicated thread. Drains verdicts and executes
        the actual remedy here -- off the Spark listener-bus thread -- so
        listener callbacks stay fast and every QueryMetric's timestamp
        stays accurate even while a slow remedy (e.g. ANALYZE TABLE) is
        running."""
        while True:
            verdict = self._adapt_queue.get()
            if verdict is _SHUTDOWN:
                self._adapt_queue.task_done()
                return

            self._adapting.set()
            try:
                try:
                    result = self.adaptor.adapt(verdict)
                except Exception:
                    logger.exception(
                        "Adaptor.adapt raised for signature=%s -- treating "
                        "as a failed adaptation", verdict.signature,
                    )
                    with self._stats_lock:
                        self.stats["adaptations_failed"] += 1
                        self.alerting.emit(Alert(
                            kind="adaptation_failed", signature=verdict.signature,
                            component=verdict.component.value,
                            severity=verdict.severity.value,
                            detail={"error": "Adaptor.adapt raised an exception"},
                        ))
                    continue

                actual_cost = result.detail.get("duration_s", 0.0)
                self.scheduler.record_actual_cost(result.remedy_name, actual_cost)

                with self._stats_lock:
                    if result.success:
                        self.detector.reset(verdict.signature, verdict.signal)
                        self.severity_classifier.reset(verdict.signature, verdict.signal)
                        self.stats["adaptations"] += 1
                        self.alerting.emit(Alert(
                            kind="adaptation_completed", signature=verdict.signature,
                            component=verdict.component.value,
                            severity=verdict.severity.value,
                            detail=result.detail,
                        ))
                    else:
                        self.stats["adaptations_failed"] += 1
                        self.alerting.emit(Alert(
                            kind="adaptation_failed", signature=verdict.signature,
                            component=verdict.component.value,
                            severity=verdict.severity.value,
                            detail=result.detail,
                        ))
            finally:
                key = (verdict.signature, verdict.component)
                with self._pending_lock:
                    self._pending_keys.discard(key)
                self._adapting.clear()
                self._adapt_queue.task_done()