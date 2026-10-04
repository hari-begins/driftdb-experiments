"""
examples/run_demo.py
=======================
The proposal's Experimental Design, executed for real: three optimizer
configurations run over the *same* injected-drift workload against a real
local Spark session, reporting the metrics from its "Metrics" table.

Configurations (Section 7 "Baselines"):
  static         -- metrics collected for reporting only; no adaptation ever
  fixed_interval -- unconditionally reruns refresh_statistics every N
                    queries, regardless of whether drift was detected
  driftdb        -- the full monitor -> detect -> adapt control loop

Workload (Section 7 "Workloads / Drift Scenarios"): a recurring
region/aggregate query against a `sales` table, run through:
  1. a stable phase (no-drift control, to measure false positives)
  2. an abrupt drift phase (predicate selectivity step-change)

Honesty note: this demo runs on a 1-vCPU local Spark session against a
small synthetic table, specifically so it's reproducible without a real
cluster. The *mechanism* being measured (does the system notice drift
quickly, and does it avoid unnecessary adaptation work) is real and the
numbers below are real measurements, not illustrative placeholders -- but
absolute latency numbers on this dataset are not representative of a
production cluster's, and are reported for relative comparison across the
three configurations, not as a general DriftDB performance claim.
"""
from __future__ import annotations

import json
import os
import random
import shutil
import sys
import tempfile
import threading
import time

os.environ.setdefault("PYSPARK_PYTHON", "python3")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

STABLE_QUERIES = 40
DRIFT_QUERIES = 40
SLA_LATENCY_MS = 400.0
FIXED_INTERVAL_N = 15  # fixed-interval baseline retrains every N queries


def _safe_teardown(*fns, timeout=5):
    """Run teardown calls (collector.stop / service.stop / spark.stop) on a
    background thread with a timeout, so a py4j/JVM shutdown hang can never
    block the caller from returning. Since this script calls os._exit(0)
    right after main()/the --config branch finishes, teardown doesn't need
    to succeed cleanly -- the OS reclaims everything on process exit."""
    def run():
        for fn in fns:
            try:
                fn()
            except Exception:
                pass
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout)


def _build_spark(tmp_path):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").appName("driftdb-demo")
             .config("spark.driver.memory", "1g")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", os.path.join(tmp_path, "warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _seed_table(spark, seed=123, n=20000):
    random.seed(seed)
    rows = [(i, random.randint(1, 100), random.choice(["east", "west", "north", "south"]))
            for i in range(n)]
    df = spark.createDataFrame(rows, ["id", "amount", "region"])
    df.write.mode("overwrite").saveAsTable("sales")


QUERY = "select region, count(*) as c, avg(amount) as a from sales where amount > {t} group by region"


def _run_queries(spark, n, threshold_center, seed_offset, on_each=None):
    random.seed(1000 + seed_offset)
    for i in range(n):
        t = threshold_center + random.randint(-2, 2)
        start = time.time()
        spark.sql(QUERY.format(t=t)).collect()
        latency_ms = (time.time() - start) * 1000.0
        if on_each:
            on_each(i, latency_ms)


def run_static(tmp_path, out_path=None) -> dict:
    from driftdb.metrics.store import MetricsStore
    from driftdb.metrics.collector import SparkMetricsCollector

    spark = _build_spark(tmp_path)
    _seed_table(spark)
    store = MetricsStore(os.path.join(tmp_path, "static.sqlite"))
    collector = SparkMetricsCollector(spark, store)
    collector.start()

    missed = {"n": 0}
    def on_each(i, latency_ms):
        if latency_ms > SLA_LATENCY_MS:
            missed["n"] += 1

    t0 = time.time()
    _run_queries(spark, STABLE_QUERIES, 50, 0, on_each)
    _run_queries(spark, DRIFT_QUERIES, 95, 1, on_each)
    _drain(store)
    elapsed = time.time() - t0

    result = dict(config="static", total_queries=STABLE_QUERIES + DRIFT_QUERIES,
                  missed_deadlines=missed["n"], adaptations=0, adaptation_cost_s=0.0,
                  false_positive_adaptations=0, detection_latency_queries=None,
                  avg_latency_stable_ms=_avg_latency(store, 0, STABLE_QUERIES),
                  avg_latency_drift_ms=_avg_latency(store, STABLE_QUERIES, STABLE_QUERIES + DRIFT_QUERIES),
                  wall_time_s=elapsed)
    _write_result(result, out_path)
    _safe_teardown(collector.stop, spark.stop)
    return result


def run_fixed_interval(tmp_path, out_path=None) -> dict:
    from driftdb.metrics.store import MetricsStore
    from driftdb.metrics.collector import SparkMetricsCollector
    from driftdb.adaptation import remedies

    spark = _build_spark(tmp_path)
    _seed_table(spark)
    store = MetricsStore(os.path.join(tmp_path, "fixed.sqlite"))
    collector = SparkMetricsCollector(spark, store)
    collector.start()

    missed = {"n": 0}
    adapt_stats = {"count": 0, "cost_s": 0.0, "false_positives": 0}
    counter = {"i": 0}

    def on_each(i, latency_ms):
        if latency_ms > SLA_LATENCY_MS:
            missed["n"] += 1
        counter["i"] += 1
        if counter["i"] % FIXED_INTERVAL_N == 0:
            detail = remedies.refresh_statistics(spark, ["spark_catalog.default.sales"])
            adapt_stats["count"] += 1
            adapt_stats["cost_s"] += detail["duration_s"]
            if counter["i"] <= STABLE_QUERIES:
                # fired during the no-drift control phase -> unnecessary
                adapt_stats["false_positives"] += 1

    t0 = time.time()
    _run_queries(spark, STABLE_QUERIES, 50, 0, on_each)
    _run_queries(spark, DRIFT_QUERIES, 95, 1, on_each)
    _drain(store)
    elapsed = time.time() - t0

    result = dict(config="fixed_interval", total_queries=STABLE_QUERIES + DRIFT_QUERIES,
                  missed_deadlines=missed["n"], adaptations=adapt_stats["count"],
                  adaptation_cost_s=adapt_stats["cost_s"],
                  false_positive_adaptations=adapt_stats["false_positives"],
                  detection_latency_queries=None,
                  avg_latency_stable_ms=_avg_latency(store, 0, STABLE_QUERIES),
                  avg_latency_drift_ms=_avg_latency(store, STABLE_QUERIES, STABLE_QUERIES + DRIFT_QUERIES),
                  wall_time_s=elapsed)
    _write_result(result, out_path)
    _safe_teardown(collector.stop, spark.stop)
    return result


def run_driftdb(tmp_path, out_path=None) -> dict:
    from driftdb import DriftDBService, DriftDBConfig

    spark = _build_spark(tmp_path)
    _seed_table(spark)

    cfg = DriftDBConfig()
    cfg.metrics_db_path = os.path.join(tmp_path, "driftdb.sqlite")
    cfg.model_store_path = os.path.join(tmp_path, "driftdb_models")
    cfg.severity.sustained_windows_required = 3
    cfg.scheduler.cooldown_seconds = 1.0
    cfg.ddm.min_num_instances = 15
    cfg.sla_latency_ms = SLA_LATENCY_MS

    service = DriftDBService(spark, cfg)
    service.start()

    t0 = time.time()
    _run_queries(spark, STABLE_QUERIES, 50, 0)
    boundary_ts = time.time()  # true stable/drift boundary, used for false-positive
    # classification below -- NOT an in-loop flag, because adaptation events
    # arrive asynchronously off Spark's listener bus and can lag the query
    # that triggered them by several queries' worth of wall-clock time; an
    # in-loop check would misclassify a late-arriving *real* detection as a
    # false positive.
    _run_queries(spark, DRIFT_QUERIES, 95, 1)
    _drain(service.metrics_store)
    elapsed = time.time() - t0

    adaptation_alerts = [a for a in service.alerting.recent(1000)
                         if a.kind == "adaptation_completed"]
    false_positives = sum(1 for a in adaptation_alerts if a.timestamp < boundary_ts)
    real_adaptations = [a for a in adaptation_alerts if a.timestamp >= boundary_ts]

    detection_latency = None
    if real_adaptations:
        first_ts = min(a.timestamp for a in real_adaptations)
        sigs = service.metrics_store.distinct_signatures()
        main_sig = max(sigs, key=lambda s: len(service.metrics_store.signal_series(s, "timestamp")))
        ts_series = service.metrics_store.signal_series(main_sig, "timestamp")
        drift_phase_ts = [t for t in ts_series if t >= boundary_ts]
        detection_latency = sum(1 for t in drift_phase_ts if t <= first_ts)

    total_cost = sum(a.detail.get("duration_s", 0.0) for a in adaptation_alerts)

    result = dict(config="driftdb", total_queries=STABLE_QUERIES + DRIFT_QUERIES,
                  missed_deadlines=service.stats["missed_deadlines"],
                  adaptations=service.stats["adaptations"], adaptation_cost_s=total_cost,
                  false_positive_adaptations=false_positives,
                  detection_latency_queries=detection_latency,
                  avg_latency_stable_ms=_avg_latency(service.metrics_store, 0, STABLE_QUERIES),
                  avg_latency_drift_ms=_avg_latency(service.metrics_store, STABLE_QUERIES,
                                                     STABLE_QUERIES + DRIFT_QUERIES),
                  wall_time_s=elapsed)
    _write_result(result, out_path)
    _safe_teardown(service.stop, spark.stop)
    return result


def _write_result(result, out_path):
    if out_path:
        with open(out_path, "w") as f:
            json.dump(result, f)


def _drain(store, max_wait_s=90):
    prev, stable = -1, 0
    for _ in range(max_wait_s):
        cur = store.count()
        if cur == prev:
            stable += 1
            if stable >= 5:
                return
        else:
            stable = 0
        prev = cur
        time.sleep(1)


def _avg_latency(store, start_idx, end_idx):
    # the metrics table's own SQL execution (ANALYZE TABLE etc.) may also be
    # present with far fewer rows; only average over the recurring query's
    # own signature (the one with the most rows).
    sigs = store.distinct_signatures()
    if not sigs:
        return None
    main_sig = max(sigs, key=lambda s: len(store.signal_series(s, "latency_ms")))
    series = store.signal_series(main_sig, "latency_ms")
    window = series[start_idx:end_idx]
    return (sum(window) / len(window)) if window else None


def main():
    print("DriftDB comparative evaluation")
    print(f"  stable queries: {STABLE_QUERIES}, drift queries: {DRIFT_QUERIES}, "
          f"SLA: {SLA_LATENCY_MS}ms, fixed-interval N: {FIXED_INTERVAL_N}\n")

    import subprocess
    here = os.path.abspath(__file__)
    results = []
    for name in ["static", "fixed_interval", "driftdb"]:
        print(f"--- running {name} (in its own subprocess) ---")
        out_file = tempfile.mktemp(prefix=f"driftdb_result_{name}_", suffix=".json")
        try:
            proc = subprocess.run(
                [sys.executable, here, "--config", name, "--out", out_file],
                timeout=200, capture_output=True, text=True,
            )
            if proc.returncode not in (0,):
                print(f"  [warn] subprocess exited with code {proc.returncode}")
                print(proc.stdout[-2000:])
                print(proc.stderr[-2000:])
        except subprocess.TimeoutExpired:
            print(f"  [warn] {name} subprocess timed out (likely Spark/py4j teardown hang -- "
                  f"see docs/known_limitations.md); using whatever result it managed to write")

        if os.path.exists(out_file):
            with open(out_file) as f:
                r = json.load(f)
            results.append(r)
            print(json.dumps(r, indent=2))
        else:
            print(f"  [error] {name} produced no result file -- it likely crashed before "
                  f"finishing the workload, not just during teardown")
        print()

    if not results:
        print("No configuration produced a result; see subprocess output above.")
        return

    print("=" * 100)
    header = f"{'config':<15}{'missed_dl':<11}{'adapts':<8}{'adapt_cost_s':<14}" \
             f"{'false_pos':<11}{'detect_lat_q':<14}{'lat_stable_ms':<15}{'lat_drift_ms':<14}"
    print(header)
    print("-" * 100)
    for r in results:
        print(f"{r['config']:<15}{r['missed_deadlines']:<11}{r['adaptations']:<8}"
              f"{r['adaptation_cost_s']:<14.2f}{r['false_positive_adaptations']:<11}"
              f"{str(r['detection_latency_queries']):<14}"
              f"{(r['avg_latency_stable_ms'] or 0):<15.1f}{(r['avg_latency_drift_ms'] or 0):<14.1f}")
    print("=" * 100)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nFull results written to {out_path}")


_RUNNERS = {"static": run_static, "fixed_interval": run_fixed_interval, "driftdb": run_driftdb}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", choices=list(_RUNNERS.keys()), default=None,
                         help="Run a single configuration (used internally by the "
                              "subprocess orchestrator) instead of the full comparison.")
    parser.add_argument("--out", default=None, help="Where to write this config's result JSON.")
    args = parser.parse_args()

    if args.config:
        tmp = tempfile.mkdtemp(prefix=f"driftdb_demo_{args.config}_")
        try:
            result = _RUNNERS[args.config](tmp, out_path=args.out)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print(json.dumps(result, indent=2))
    else:
        main()
    os._exit(0)