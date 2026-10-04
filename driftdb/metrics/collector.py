"""
driftdb.metrics.collector
===========================
Live metrics collection off a *real* Spark SQL session.

This registers a Python implementation of Spark's
`org.apache.spark.sql.util.QueryExecutionListener` through a py4j callback
server, and on every completed query walks the *executed* physical plan tree
to read out the SQLMetric accumulators Spark already attaches to every node
(numOutputRows, filesSize, shuffle bytes, join strategy, etc). Nothing here is
synthetic: these are the same counters that back the Spark UI's SQL tab.

Metric mapping (semantics kept separate on purpose):
  - shuffle_read_bytes       = localBytesRead + remoteBytesRead   (exact keys)
  - shuffle_write_bytes      = shuffleBytesWritten                (exact key)
  - shuffle_partition_bytes  = partitionDataSize (AQE, post-coalesce size)
  - selectivity_by_relation  = Filter output rows / rows of the Scan below it,
                               per table (JSON)
  - selectivity              = geometric mean of the per-relation values

Known limitation (documented, not hidden): peak memory and operator times are
read from plan-level SQLMetrics rather than a full SparkListener task feed.
That is a reasonable approximation for drift *detection* (relative change over
time), but not exact executor-level accounting.
"""
from __future__ import annotations

import json
import logging
import math
import re
import threading
from typing import Callable, List, Optional

from py4j.java_gateway import CallbackServerParameters

from .schema import QueryMetric
from .signature import extract_literals, query_signature, raw_plan_digest
from .store import MetricsStore

logger = logging.getLogger("driftdb.collector")

# Node tuple: (node_name, metrics_dict, simple_string, depth)
Node = tuple

# Heuristic (substring, case-insensitive) buckets
_BYTES_KEYS = ("filessize", "size of files read")
_PEAK_MEM_KEYS = ("peakmemory", "peak memory")
_TASK_TIME_KEYS = ("scantime", "aggtime", "sorttime", "jointime")

# Exact metric names (exact match, so remoteBytesReadToDisk is NOT counted)
_SHUFFLE_READ_EXACT = ("localBytesRead", "remoteBytesRead")
_SHUFFLE_WRITE_EXACT = ("shuffleBytesWritten",)
_PARTITION_BYTES_EXACT = ("partitionDataSize",)

_SCAN_PREFIXES = ("scan ", "range", "batchscan")
_JOIN_SUBSTR = "join"

_LIMIT_RE = re.compile(r"limit=(\d+)")
# Location: InMemoryFileIndex(1 paths)[file:/.../tpcds_parquet/item] -> "item"
_LOCATION_RE = re.compile(r"Location:[^\[]*\[[^\]]*?/([^/\]]+)\]")


class _PyQueryExecutionListener:
    """Implements Spark's `QueryExecutionListener` interface via py4j.

    Spark calls these synchronously after each action, so keep the work small.
    """

    def __init__(self, on_query, on_failure):
        self._on_query = on_query
        self._on_failure = on_failure

    def onSuccess(self, funcName, qe, durationNs):  # noqa: N802
        try:
            self._on_query(funcName, qe, durationNs)
        except Exception:
            logger.exception("driftdb collector failed processing a query")

    def onFailure(self, funcName, qe, exception):  # noqa: N802
        try:
            self._on_failure(funcName, qe, exception)
        except Exception:
            logger.exception("driftdb collector failed processing a query failure")

    class Java:
        implements = ["org.apache.spark.sql.util.QueryExecutionListener"]


class SparkMetricsCollector:
    """Attaches DriftDB's metrics collection to a live SparkSession.

    Usage:
        collector = SparkMetricsCollector(spark, store)
        collector.start()
        ... run queries as normal ...
        collector.stop()
    """

    def __init__(self, spark, store: MetricsStore,
                 on_metric: Optional[Callable[[QueryMetric], None]] = None):
        self.spark = spark
        self.store = store
        self._on_metric = on_metric
        self._gw = spark.sparkContext._gateway
        self._jvm = self._gw.jvm
        self._listener = None
        self._started = False
        self._lock = threading.Lock()
        # Workload context stamped onto every metric (see set_context).
        self._ctx = {"run_id": "", "phase": "", "is_warmup": False}
        self._cb_event = threading.Event()

    # -- Workload context ---------------------------------------------------

    def set_context(self, run_id: str = "", phase: str = "",
                    is_warmup: bool = False) -> None:
        """Label the NEXT queries (ground truth for evaluation)."""
        self._ctx = {"run_id": run_id, "phase": phase, "is_warmup": is_warmup}

    def run_and_wait(self, sql: str, timeout: float = 60.0):
        """Run one SQL query and block until its metric is stored.

        Spark's listener callback can arrive AFTER the next query starts, which
        would stamp the wrong context on the metric. Running one query at a
        time and waiting for its callback keeps the labels correct."""
        self._cb_event.clear()
        rows = self.spark.sql(sql).collect()
        if not self._cb_event.wait(timeout):
            logger.warning("Timed out waiting for metric of query: %.80s", sql)
        return rows

    # -- Scala <-> Java collection bridging -------------------------------

    def _seq_to_list(self, seq) -> list:
        conv = self._jvm.scala.collection.JavaConverters.seqAsJavaListConverter(seq).asJava()
        return list(conv)

    def _map_to_dict(self, m) -> dict:
        conv = self._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(m).asJava()
        return dict(conv)

    # -- Plan tree walk -----------------------------------------------------

    def _walk_plan(self, plan, acc: List[Node], depth: int = 0) -> None:
        """Depth-first walk of the executed SparkPlan tree. Appends
        (node_name, metrics, simple_string, depth) for every node, so the
        tree shape is preserved (needed to pair a Filter with its Scan).
        Handles AQE's QueryStageExec.plan() indirection."""
        try:
            jmetrics = self._map_to_dict(plan.metrics())
            node_metrics = {}
            for k, sql_metric in jmetrics.items():
                try:
                    node_metrics[k] = sql_metric.value()
                except Exception:
                    pass
            try:
                simple = plan.simpleString(200)
            except Exception:
                simple = ""
            acc.append((plan.nodeName(), node_metrics, simple, depth))
        except Exception as e:
            acc.append(("__error__", {}, str(e), depth))
            return

        try:
            for child in self._seq_to_list(plan.children()):
                self._walk_plan(child, acc, depth + 1)
        except Exception:
            pass

        # AQE wraps the real plan behind .plan(), invisible to .children()
        try:
            self._walk_plan(plan.plan(), acc, depth + 1)
        except Exception:
            pass

    @staticmethod
    def _match_any(name: str, keys: tuple) -> bool:
        low = name.lower()
        return any(k in low for k in keys)

    def _extract(self, nodes: List[Node]) -> dict:
        """Turn the depth-annotated node list into QueryMetric signal fields."""

        for name, metrics, simple, depth in nodes:
            logger.debug("%sNODE: %s | %s", "  " * depth, name, metrics)

        input_rows = bytes_scanned = peak_mem = 0
        shuffle_read = shuffle_write = partition_bytes = 0
        exec_time_units = 0.0
        join_strategies: list = []
        scan_relations: list = []
        first_row_val = None

        for name, metrics, simple, depth in nodes:
            if name == "__error__":
                continue
            lname = name.lower()

            for mk, mv in metrics.items():
                if mk in _SHUFFLE_READ_EXACT:
                    shuffle_read += mv
                if mk in _SHUFFLE_WRITE_EXACT:
                    shuffle_write += mv
                if mk in _PARTITION_BYTES_EXACT:
                    partition_bytes += mv
                if self._match_any(mk, _BYTES_KEYS):
                    bytes_scanned += mv
                if self._match_any(mk, _PEAK_MEM_KEYS):
                    peak_mem = max(peak_mem, mv)
                if self._match_any(mk, _TASK_TIME_KEYS):
                    exec_time_units += mv

            rv = metrics.get("numOutputRows")
            if first_row_val is None and rv is not None:
                first_row_val = rv

            if any(lname.startswith(p) for p in _SCAN_PREFIXES):
                if rv is not None:
                    input_rows += rv
                m = _LOCATION_RE.search(simple)
                scan_relations.append(m.group(1) if m else lname.strip())

            if _JOIN_SUBSTR in lname:
                join_strategies.append(name)

        # ---- output_rows: respect LIMIT on the root node ----
        output_rows = first_row_val or 0
        if nodes:
            lim = _LIMIT_RE.search(nodes[0][2])
            if lim:
                output_rows = min(output_rows, int(lim.group(1)))

        # ---- selectivity per table: Filter rows / rows of the Scan below it ----
        sel_by_rel: dict = {}
        for i, (name, metrics, simple, depth) in enumerate(nodes):
            if not name.lower().startswith("filter"):
                continue
            f_rows = metrics.get("numOutputRows")
            if f_rows is None:
                continue
            for j in range(i + 1, len(nodes)):
                n2, m2, s2, d2 = nodes[j]
                if d2 <= depth:
                    break  # left this filter's subtree
                if n2.lower().startswith("scan"):
                    s_rows = m2.get("numOutputRows")
                    loc = _LOCATION_RE.search(s2)
                    if s_rows and loc:
                        sel_by_rel.setdefault(loc.group(1), f_rows / s_rows)
                    break

        if sel_by_rel:
            vals = [max(v, 1e-9) for v in sel_by_rel.values()]
            selectivity = math.exp(sum(math.log(v) for v in vals) / len(vals))
        else:
            selectivity = 1.0

        return dict(
            input_rows=input_rows,
            output_rows=output_rows,
            bytes_scanned=bytes_scanned,
            bytes_read=bytes_scanned,
            selectivity=min(selectivity, 1.0),
            selectivity_by_relation=json.dumps(sel_by_rel, sort_keys=True),
            shuffle_read_bytes=shuffle_read,
            shuffle_write_bytes=shuffle_write,
            shuffle_partition_bytes=partition_bytes,
            peak_executor_memory_bytes=peak_mem,
            executor_run_time_ms=exec_time_units,
            join_strategies=",".join(sorted(set(join_strategies))),
            scan_relations=",".join(sorted(set(scan_relations))),
        )

    def _active_executor_count(self) -> int:
        try:
            return int(self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size())
        except Exception:
            return 0

    # -- Callback handlers ---------------------------------------------------

    def _handle_success(self, funcName, qe, durationNs):
        try:
            ep = qe.executedPlan()
            try:
                target = ep.executedPlan()  # unwrap AdaptiveSparkPlanExec
            except Exception:
                target = ep
            plan_text = target.toString()
            nodes: List[Node] = []
            self._walk_plan(target, nodes)
            signals = self._extract(nodes)

            try:
                exec_id = qe.id()
            except Exception:
                exec_id = None

            try:
                analyzed_text = qe.analyzed().toString()
            except Exception:
                analyzed_text = plan_text

            metric = QueryMetric(
                signature=query_signature(analyzed_text),
                raw_plan_digest=raw_plan_digest(plan_text),
                execution_id=exec_id,
                func_name=str(funcName),
                latency_ms=durationNs / 1e6,
                cluster_active_executors=self._active_executor_count(),
                literal_values=extract_literals(analyzed_text),
                **self._ctx,
                **signals,
            )
            self.store.write(metric)
            if self._on_metric:
                self._on_metric(metric)
        except Exception:
            logger.exception("Failed to process successful query execution")
        finally:
            self._cb_event.set()

    def _handle_failure(self, funcName, qe, exception):
        try:
            analyzed_text = qe.analyzed().toString()
        except Exception:
            analyzed_text = funcName
        try:
            metric = QueryMetric(
                signature=query_signature(analyzed_text),
                raw_plan_digest=raw_plan_digest(analyzed_text),
                execution_id=None,
                func_name=str(funcName),
                literal_values=extract_literals(analyzed_text),
                failed=True,
                error=str(exception)[:2000],
                **self._ctx,
            )
            self.store.write(metric)
            if self._on_metric:
                self._on_metric(metric)
        finally:
            self._cb_event.set()

    # -- Lifecycle -----------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._gw.start_callback_server(
                callback_server_parameters=CallbackServerParameters(
                    daemonize=True, daemonize_connections=True
                )
            )
            # The JVM side's callback client must be told the Python port.
            python_port = self._gw._callback_server.server_socket.getsockname()[1]
            self._gw.java_gateway_server.resetCallbackClient(
                self._gw.java_gateway_server.getCallbackClient().getAddress(),
                python_port,
            )
            self._listener = _PyQueryExecutionListener(
                self._handle_success, self._handle_failure
            )
            self.spark._jsparkSession.listenerManager().register(self._listener)
            self._started = True
            logger.info("DriftDB metrics collector attached to Spark session")

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            # Unregister in a background thread with a timeout: the JVM calls
            # back into Python during unregister, which can deadlock if run on
            # the main thread. shutdown_callback_server() is deliberately not
            # called (it blocks); its daemon threads die with the process.
            t = threading.Thread(
                target=lambda: self.spark._jsparkSession
                .listenerManager().unregister(self._listener),
                daemon=True,
            )
            t.start()
            t.join(timeout=3)
            self._started = False