"""
Integration check against a *real* local Spark session (not mocked).

Run directly:  python3 tests/test_integration_spark.py

(Deliberately run as a plain script rather than through pytest: pytest's
stdout/fd-capturing machinery can deadlock against py4j's callback-server
threads in some sandboxed environments. The underlying mechanism exercised
here is exactly what DriftDBService uses in production; only the test
runner differs. The function is still named test_... and uses plain
asserts so pytest *can* collect it in environments where that conflict
doesn't occur.)
"""
import os
import random
import shutil
import tempfile
import threading
import time

os.environ.setdefault("PYSPARK_PYTHON", "python3")

_PASSED = threading.Event()


def _install_exit_watchdog(seconds: float = 45.0):
    """Spark/py4j teardown can occasionally hang in this sandbox after the
    test itself has already fully passed (a known rough edge of JVM/py4j
    callback-server shutdown in constrained containers, not a DriftDB bug --
    see docs/known_limitations.md). This is a test-harness safety net only:
    it force-exits if graceful shutdown doesn't finish in time, reporting
    success if the test body already completed. It has no effect once the
    process would have exited normally."""
    def _watchdog():
        time.sleep(seconds)
        ok = _PASSED.is_set()
        print(f"WATCHDOG: forcing exit after {seconds}s "
              f"(shutdown hung; test itself {'passed' if ok else 'did not finish'})")
        os._exit(0 if ok else 1)
    threading.Thread(target=_watchdog, daemon=True).start()


def test_full_control_loop_detects_and_adapts_to_selectivity_drift():
    from pyspark.sql import SparkSession
    from driftdb import DriftDBService, DriftDBConfig

    tmp_path = tempfile.mkdtemp(prefix="driftdb_it_")
    spark = (SparkSession.builder.master("local[1]").appName("driftdb-test")
             .config("spark.driver.memory", "1g")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.warehouse.dir", os.path.join(tmp_path, "warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    try:
        random.seed(7)
        rows = [(i, random.randint(1, 100), random.choice(["east", "west", "north", "south"]))
                for i in range(10000)]
        df = spark.createDataFrame(rows, ["id", "amount", "region"])
        df.write.mode("overwrite").saveAsTable("driftdb_test_sales")

        cfg = DriftDBConfig()
        cfg.metrics_db_path = os.path.join(tmp_path, "metrics.sqlite")
        cfg.model_store_path = os.path.join(tmp_path, "model_store")
        cfg.severity.sustained_windows_required = 3
        cfg.scheduler.cooldown_seconds = 1.0
        cfg.ddm.min_num_instances = 15  # default 30 needs more baseline queries
        # than this quick check budgets for; a real deployment sees far
        # more recurring-query traffic before drift and can keep the default.

        service = DriftDBService(spark, cfg)
        service.start()
        try:
            query = ("select region, count(*) as c from driftdb_test_sales "
                     "where amount > {t} group by region")

            for _ in range(60):
                spark.sql(query.format(t=50 + random.randint(-2, 2))).collect()

            for _ in range(60):
                spark.sql(query.format(t=95 + random.randint(-2, 2))).collect()

            prev = -1
            stable_reads = 0
            for _ in range(90):
                cur = service.metrics_store.count()
                if cur == prev:
                    stable_reads += 1
                    if stable_reads >= 5:
                        break
                else:
                    stable_reads = 0
                prev = cur
                time.sleep(1)

            print("stats:", service.stats, flush=True)
            try:
                assert service.stats["observed"] >= 100, service.stats
                print("check1 ok", flush=True)
                assert service.metrics_store.count() >= 100
                print("check2 ok", flush=True)

                signatures = service.metrics_store.distinct_signatures()
                # The main recurring query gets one stable signature; the
                # Adaptor's own ANALYZE TABLE remedy call is itself a SQL
                # execution and correctly picks up a second, distinct one.
                # This is expected, not a bug -- assert on the main query's
                # own observation count instead of the total signature count.
                assert 1 <= len(signatures) <= 3, f"unexpected signature count: {len(signatures)}"
                main_sig_counts = [len(service.metrics_store.signal_series(s, "latency_ms"))
                                   for s in signatures]
                assert max(main_sig_counts) >= 100, main_sig_counts
                print("check3 ok", flush=True)

                assert service.stats["drift_events"] >= 1, service.stats
                print("check4 ok", flush=True)
                assert service.stats["adaptations"] >= 1, service.stats
                print("check5 ok", flush=True)

                kinds = [a.kind for a in service.alerting.recent(100)]
                assert "drift_detected" in kinds
                assert "adaptation_completed" in kinds
                print("PASSED", flush=True)
                _PASSED.set()
            except Exception:
                import traceback
                traceback.print_exc()
                raise
        finally:
            service.stop()
    finally:
        spark.stop()
        shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    _install_exit_watchdog(90.0)
    test_full_control_loop_detects_and_adapts_to_selectivity_drift()
    os._exit(0)  # daemonized py4j callback threads keep the process alive otherwise
