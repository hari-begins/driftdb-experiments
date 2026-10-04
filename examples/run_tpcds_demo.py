"""
examples/run_tpcds_demo.py
=============================
The real TPC-DS-grounded version of the comparative evaluation: same three
configurations (static / fixed_interval / driftdb) as run_demo.py, but
against real dsdgen-generated TPC-DS SF1 data and a real dsqgen-generated
query (see docs/tpcds_methodology.md for exact reproduction), with BOTH
abrupt and gradual drift scenarios, and all six metrics from the
proposal's Section 7 table -- including Recovery Time, which run_demo.py
did not compute.

Prerequisites (not bundled -- see docs/tpcds_methodology.md for why):
  1. Build tpcds-kit's dsdgen/dsqgen.
  2. Generate SF1 data: ./dsdgen -SCALE 1 -DIR <data_dir> -FORCE Y
  3. Convert to Parquet once (shared across all runs below):
       python3 examples/tpcds_convert_once.py --data-dir <data_dir> --out <parquet_dir>

Usage (each configuration run as its own subprocess -- see
docs/known_limitations.md #1 for why):
    python3 examples/run_tpcds_demo.py --config static --out /tmp/r_static.json --parquet-dir <parquet_dir>
    python3 examples/run_tpcds_demo.py --config fixed_interval --out /tmp/r_fixed.json --parquet-dir <parquet_dir>
    python3 examples/run_tpcds_demo.py --config driftdb --out /tmp/r_driftdb.json --parquet-dir <parquet_dir>
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time

os.environ.setdefault("PYSPARK_PYTHON", "python3")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpcds_drift_workload import stable_queries, abrupt_drift_queries, gradual_drift_queries

STABLE_N = 20
DRIFT_N = 20        # abrupt scenario's drift phase
GRADUAL_N = 40       # gradual scenario's drift phase (8 stages x 5 reps)
FIXED_INTERVAL_N = 10
RECOVERY_TOLERANCE = 0.20   # within 20% of pre-drift baseline counts as "recovered"
RECOVERY_MAX_QUERIES = 200  # give up looking for recovery after this many post-adaptation queries

SCENARIOS = [("abrupt", abrupt_drift_queries, DRIFT_N), ("gradual", gradual_drift_queries, GRADUAL_N)]


def _build_spark(tmp_path):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").appName("driftdb-tpcds-demo")
             .config("spark.driver.memory", "2g")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", os.path.join(tmp_path, "warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _register_tables(spark, parquet_dir):
    for table in ["store_sales", "date_dim", "item"]:
        path = os.path.join(parquet_dir, table)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"CREATE TABLE {table} USING PARQUET LOCATION '{path}'")


def _run_query_list(spark, queries, on_each=None, exec_fn=None):
    exec_fn = exec_fn or (lambda q: spark.sql(q).collect())
    for i, q in enumerate(queries):
        start = time.time()
        exec_fn(q)
        latency_ms = (time.time() - start) * 1000.0
        if on_each:
            on_each(i, latency_ms)


def _drain(store, max_wait_s=90):
    prev, stable = -1, 0
    for _ in range(max_wait_s):
        cur = store.count()
        if cur == prev:
            stable += 1
            if stable >= 8:
                return
        else:
            stable = 0
        prev = cur
        time.sleep(1)


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _recovery_time(latency_series_ms, drift_start_idx, adaptation_query_idx, baseline_ms):
    """Number of queries after the adaptation before a trailing-5 rolling
    average latency comes back within RECOVERY_TOLERANCE of the pre-drift
    baseline. None if it never does within RECOVERY_MAX_QUERIES (an honest
    result when drift reflects genuine added computation, not a fixable
    misestimation -- see docs/tpcds_methodology.md Section 5)."""
    if adaptation_query_idx is None or baseline_ms is None:
        return None
    window = 5
    for i in range(adaptation_query_idx, min(len(latency_series_ms), adaptation_query_idx + RECOVERY_MAX_QUERIES)):
        if i + window > len(latency_series_ms):
            break
        roll = sum(latency_series_ms[i:i + window]) / window
        if roll <= baseline_ms * (1 + RECOVERY_TOLERANCE):
            return i - adaptation_query_idx
    return None


def _scenario(spark, store, drift_queries, config_name, sla_ms, on_adapt_check=None,exec_fn=None,warmup_exec_fn=None):
    """Runs stable + drift phase for one scenario, returns
    (missed_deadlines, stable_start_ts, drift_start_ts).

    A short warmup (a handful of untimed queries against the same query
    shape) runs first and is excluded from all metrics -- including from
    the metrics store's own averages, not just from wall-clock timing --
    via stable_start_ts: the very first execution of any query shape pays
    Spark's one-time codegen/JIT compilation cost (~4-5s observed on this
    query, vs. ~0.7s steady state), which would otherwise swamp the actual
    experimental signal since the stable phase always runs before the
    drift phase."""
    warmup_exec_fn = warmup_exec_fn or exec_fn or (lambda q: spark.sql(q).collect())
    for q in stable_queries(20):
        warmup_exec_fn(q)

    stable_start_ts = time.time()
    missed = {"n": 0}

    def on_each(i, latency_ms):
        if latency_ms > sla_ms:
            missed["n"] += 1
        if on_adapt_check:
            on_adapt_check(i, latency_ms)

    _run_query_list(spark, stable_queries(STABLE_N), on_each, exec_fn=exec_fn)
    boundary_ts = time.time()
    _run_query_list(spark, drift_queries, on_each, exec_fn=exec_fn)
    _drain(store)
    time.sleep(30)  # extra fixed buffer: a real adaptation (e.g. a ~20s
    # ANALYZE TABLE on the 2.88M-row store_sales table) runs synchronously
    # on the same single listener-bus thread that processes every other
    # query's callback, so a backlog of pending callbacks can briefly look
    # "stable" to the count-based _drain() heuristic above while a slow
    # adaptation is still being processed for an earlier query. Verified
    # via direct log inspection: a genuine, correctly-triggered adaptation
    # was observed completing ~20s after _drain() alone had already
    # returned. This flat buffer is a blunter fix than instrumenting the
    # scheduler with a "processing complete" signal, but it is honest and
    # sufficient for this evaluation's query volumes. run_driftdb() adds a
    # further, deterministic barrier (service.wait_for_idle) on top of
    # this before it trusts adaptation metrics or tears down Spark -- see
    # there for why this fixed buffer alone was still not sufficient.
    return missed["n"], stable_start_ts, boundary_ts


def run_static(tmp_path, parquet_dir, out_path=None) -> dict:
    from driftdb.metrics.store import MetricsStore
    from driftdb.metrics.collector import SparkMetricsCollector

    spark = _build_spark(tmp_path)
    _register_tables(spark, parquet_dir)
    store = MetricsStore(os.path.join(tmp_path, "static.sqlite"))
    collector = SparkMetricsCollector(spark, store)
    collector.start()

    def _warmup(q):
        collector.set_context(is_warmup=True)
        try:
            spark.sql(q).collect()
        finally:
            collector.set_context(is_warmup=False)

    result = {"config": "static"}
    for scenario_name, drift_fn, n in SCENARIOS:
        missed, stable_start_ts, boundary_ts = _scenario(
            spark, store, drift_fn(n), "static", 5000.0,
            warmup_exec_fn=_warmup,
        )
        sigs = store.distinct_signatures()
        main_sig = max(sigs, key=lambda s: len(store.signal_series(s, "latency_ms")))
        lat = store.signal_series(main_sig, "latency_ms")
        sel = store.signal_series(main_sig, "selectivity")
        ts = store.signal_series(main_sig, "timestamp")
        stable_lat = [l for l, t in zip(lat, ts) if stable_start_ts <= t < boundary_ts]
        drift_lat = [l for l, t in zip(lat, ts) if t >= boundary_ts]
        stable_sel = [s for s, t in zip(sel, ts) if stable_start_ts <= t < boundary_ts]
        drift_sel = [s for s, t in zip(sel, ts) if t >= boundary_ts]
        result[scenario_name] = dict(
            missed_deadlines=missed, adaptations=0, adaptation_cost_s=0.0,
            false_positive_adaptations=0, detection_latency_queries=None,
            recovery_time_queries=None, avg_latency_stable_ms=_avg(stable_lat),
            avg_latency_drift_ms=_avg(drift_lat),
            avg_selectivity_stable=_avg(stable_sel), avg_selectivity_drift=_avg(drift_sel),
        )
        _write_result(result, out_path)

    collector.stop()
    spark.stop()
    return result


def run_fixed_interval(tmp_path, parquet_dir, out_path=None) -> dict:
    from driftdb.metrics.store import MetricsStore
    from driftdb.metrics.collector import SparkMetricsCollector
    from driftdb.adaptation import remedies

    spark = _build_spark(tmp_path)
    _register_tables(spark, parquet_dir)
    store = MetricsStore(os.path.join(tmp_path, "fixed.sqlite"))
    collector = SparkMetricsCollector(spark, store)
    collector.start()

    def _warmup(q):
        collector.set_context(is_warmup=True)
        try:
            spark.sql(q).collect()
        finally:
            collector.set_context(is_warmup=False)

    result = {"config": "fixed_interval"}
    for scenario_name, drift_fn, n in SCENARIOS:
        adapt_stats = {"count": 0, "cost_s": 0.0}
        adapt_events = []  # (query_idx, timestamp)
        counter = {"i": 0}

        def on_each(i, latency_ms):
            counter["i"] += 1
            if counter["i"] % FIXED_INTERVAL_N == 0:
                detail = remedies.refresh_statistics(spark, ["spark_catalog.default.item",
                                                              "spark_catalog.default.store_sales"])
                adapt_stats["count"] += 1
                adapt_stats["cost_s"] += detail["duration_s"]
                adapt_events.append((counter["i"], time.time()))

        missed, stable_start_ts, boundary_ts = _scenario(
            spark, store, drift_fn(n), "fixed_interval", 5000.0, on_each,
            warmup_exec_fn=_warmup,
        )

        false_positives = sum(1 for _, ts in adapt_events if ts < boundary_ts)
        post_boundary = [(qi, ts) for qi, ts in adapt_events if ts >= boundary_ts]

        sigs = store.distinct_signatures()
        main_sig = max(sigs, key=lambda s: len(store.signal_series(s, "latency_ms")))
        lat = store.signal_series(main_sig, "latency_ms")
        sel = store.signal_series(main_sig, "selectivity")
        ts_series = store.signal_series(main_sig, "timestamp")
        stable_lat = [l for l, t in zip(lat, ts_series) if stable_start_ts <= t < boundary_ts]
        drift_lat = [l for l, t in zip(lat, ts_series) if t >= boundary_ts]
        stable_sel = [s for s, t in zip(sel, ts_series) if stable_start_ts <= t < boundary_ts]
        drift_sel = [s for s, t in zip(sel, ts_series) if t >= boundary_ts]

        detection_latency, recovery_time = None, None
        if post_boundary:
            first_ts = post_boundary[0][1]
            drift_phase_ts = [t for t in ts_series if t >= boundary_ts]
            detection_latency = sum(1 for t in drift_phase_ts if t <= first_ts)
            adapt_idx_in_series = detection_latency  # index within drift phase
            recovery_time = _recovery_time(drift_lat, 0, adapt_idx_in_series, _avg(stable_lat))

        result[scenario_name] = dict(
            missed_deadlines=missed, adaptations=adapt_stats["count"],
            adaptation_cost_s=adapt_stats["cost_s"], false_positive_adaptations=false_positives,
            detection_latency_queries=detection_latency, recovery_time_queries=recovery_time,
            avg_latency_stable_ms=_avg(stable_lat), avg_latency_drift_ms=_avg(drift_lat),
            avg_selectivity_stable=_avg(stable_sel), avg_selectivity_drift=_avg(drift_sel),
        )
        _write_result(result, out_path)

    collector.stop()
    spark.stop()
    return result


def run_driftdb(tmp_path, parquet_dir, out_path=None) -> dict:
    from driftdb import DriftDBService, DriftDBConfig

    spark = _build_spark(tmp_path)
    _register_tables(spark, parquet_dir)

    cfg = DriftDBConfig()
    cfg.metrics_db_path = os.path.join(tmp_path, "driftdb.sqlite")
    cfg.model_store_path = os.path.join(tmp_path, "driftdb_models")
    cfg.severity.sustained_windows_required = 3
    cfg.scheduler.cooldown_seconds = 1.0
    cfg.ddm.min_num_instances = 10
    cfg.sla_latency_ms = 5000.0

    service = DriftDBService(spark, cfg)
    service.start()

    def _driftdb_warmup(q):
        service.collector.set_context(is_warmup=True)
        try:
            service.collector.run_and_wait(q)
        finally:
            service.collector.set_context(is_warmup=False)

    result = {"config": "driftdb"}
    for scenario_name, drift_fn, n in SCENARIOS:
        missed_before = service.stats["missed_deadlines"]
        missed, stable_start_ts, boundary_ts = _scenario(
            spark,
            service.metrics_store,
            drift_fn(n),
            "driftdb",
            5000.0,
            exec_fn=lambda q: service.collector.run_and_wait(q),
            warmup_exec_fn=_driftdb_warmup,
        )
        missed = service.stats["missed_deadlines"] - missed_before

        # Deterministic barrier: _scenario()'s _drain()+sleep(30) is a
        # heuristic that can return early while a real remedy (e.g. a
        # multi-table ANALYZE TABLE) is still executing on the listener-bus
        # callback thread -- this is the root cause previously traced for
        # "Cannot call methods on a stopped SparkContext". Block here for
        # real before trusting this scenario's adaptation alerts or moving
        # on / tearing down Spark.
        idle = service.wait_for_idle(timeout=120.0)
        if not idle:
            print(f"WARNING: adaptation still in flight after 120s for "
                  f"scenario={scenario_name}; treat this scenario's "
                  f"adaptation metrics as unreliable.", file=sys.stderr)

        adaptation_alerts = [a for a in service.alerting.recent(1000) if a.kind == "adaptation_completed"]
        failed_alerts = [a for a in service.alerting.recent(1000) if a.kind == "adaptation_failed"]
        if failed_alerts:
            print(f"NOTE: {len(failed_alerts)} adaptation(s) were attempted but did not "
                  f"succeed in scenario={scenario_name} (see adaptation_failed alerts); "
                  f"these are excluded from the adaptations/adaptation_cost_s counts below.",
                  file=sys.stderr)

        false_positives = sum(1 for a in adaptation_alerts if a.timestamp < boundary_ts)
        real_adaptations = [a for a in adaptation_alerts if a.timestamp >= boundary_ts]
        total_cost = sum(a.detail.get("duration_s", 0.0) for a in adaptation_alerts)

        sigs = service.metrics_store.distinct_signatures()
        main_sig = max(sigs, key=lambda s: len(service.metrics_store.signal_series(s, "latency_ms")))
        lat = service.metrics_store.signal_series(main_sig, "latency_ms")
        sel = service.metrics_store.signal_series(main_sig, "selectivity")
        ts_series = service.metrics_store.signal_series(main_sig, "timestamp")
        stable_lat = [l for l, t in zip(lat, ts_series) if stable_start_ts <= t < boundary_ts]
        drift_lat = [l for l, t in zip(lat, ts_series) if t >= boundary_ts]
        stable_sel = [s for s, t in zip(sel, ts_series) if stable_start_ts <= t < boundary_ts]
        drift_sel = [s for s, t in zip(sel, ts_series) if t >= boundary_ts]

        detection_latency, recovery_time = None, None
        if real_adaptations:
            first_ts = min(a.timestamp for a in real_adaptations)
            drift_phase_ts = [t for t in ts_series if t >= boundary_ts]
            detection_latency = sum(1 for t in drift_phase_ts if t <= first_ts)
            recovery_time = _recovery_time(drift_lat, 0, detection_latency, _avg(stable_lat))

        result[scenario_name] = dict(
            missed_deadlines=missed, adaptations=len(adaptation_alerts),
            adaptation_cost_s=total_cost, false_positive_adaptations=false_positives,
            detection_latency_queries=detection_latency, recovery_time_queries=recovery_time,
            avg_latency_stable_ms=_avg(stable_lat), avg_latency_drift_ms=_avg(drift_lat),
            avg_selectivity_stable=_avg(stable_sel), avg_selectivity_drift=_avg(drift_sel),
            adaptation_wait_timed_out=not idle,
            adaptations_attempted_but_failed=len(failed_alerts),
        )
        _write_result(result, out_path)

    # Final barrier before teardown, independent of the per-scenario one
    # above (a later scenario in "both" mode could still have something
    # in flight even if an earlier scenario's wait succeeded).
    service.wait_for_idle(timeout=120.0)
    service.stop()
    spark.stop()
    return result


def _write_result(result, out_path):
    if out_path:
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)


_RUNNERS = {"static": run_static, "fixed_interval": run_fixed_interval, "driftdb": run_driftdb}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", choices=list(_RUNNERS.keys()), required=True)
    parser.add_argument("--out", default=None)
    parser.add_argument("--parquet-dir", required=True,
                         help="Directory containing store_sales/date_dim/item Parquet "
                              "(see docs/tpcds_methodology.md to generate).")
    parser.add_argument("--scenario", choices=["abrupt", "gradual", "both"], default="both",
                         help="ANALYZE TABLE on the real 2.88M-row store_sales table costs "
                              "~15-20s per call, so fixed_interval's repeated real adaptations "
                              "can make 'both' scenarios exceed a single run's time budget; "
                              "run them separately with this flag when that matters.")
    args = parser.parse_args()

    if args.scenario == "abrupt":
        SCENARIOS = [("abrupt", abrupt_drift_queries, DRIFT_N)]
    elif args.scenario == "gradual":
        SCENARIOS = [("gradual", gradual_drift_queries, GRADUAL_N)]

    tmp = tempfile.mkdtemp(prefix=f"driftdb_tpcds_{args.config}_")
    try:
        result = _RUNNERS[args.config](tmp, args.parquet_dir, out_path=args.out)
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
    print(json.dumps(result, indent=2))
    os._exit(0)