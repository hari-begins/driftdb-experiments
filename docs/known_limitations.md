# Known limitations and operational notes

Honest documentation of rough edges discovered while building and testing
DriftDB, so operators aren't surprised by them in production.

## 1. Spark/py4j JVM shutdown can hang

`SparkMetricsCollector` registers a Python `QueryExecutionListener` through
a py4j **callback server** -- a socket the JVM calls back into for every
query. This is the same low-level mechanism PySpark itself uses for
Structured Streaming listeners, and it works reliably for the collector's
actual job (see `tests/test_integration_spark.py`, which exercises the full
monitor->detect->adapt loop against a real local Spark session).

Where it gets rough: tearing down a JVM that has an active py4j callback
server occasionally hangs, in some environments, even after
`SparkMetricsCollector.stop()` has cleanly unregistered the listener and
shut down the callback server on the Python side. This appears to be a
JVM/py4j-level shutdown-ordering issue, not something under DriftDB's
control, and it is intermittent (most runs shut down cleanly; some don't).

**This does not affect DriftDB in production.** A monitoring service is
meant to run indefinitely, attached to a long-lived Spark session -- you
are not tearing the JVM down after every query. The issue only surfaces
when you deliberately stop+restart Spark itself (e.g. in a test harness),
which is why `tests/test_integration_spark.py` installs a watchdog thread
that force-exits if teardown hangs past a timeout, *after* recording
whether the test itself passed. If you write your own short-lived
scripts against DriftDB (a one-off notebook cell, a CI job), consider the
same pattern:

```python
import threading, os, time
def _exit_after(seconds):
    time.sleep(seconds)
    os._exit(0)
threading.Thread(target=_exit_after, args=(30,), daemon=True).start()
```

## 2. pytest's fd-capturing vs. py4j's callback-server threads

In this project's sandbox, running the Spark integration check *through
pytest* (`pytest tests/test_integration_spark.py`) intermittently hung in
a way that running the exact same code as a plain script
(`python3 tests/test_integration_spark.py`) did not. The suspected cause is
an interaction between pytest's stdout/fd-capturing machinery and the
background threads py4j's callback server spawns. This is why
`test_integration_spark.py` is structured to be run directly, while still
being pytest-collectible (plain `test_...` function, plain asserts) for
environments where the conflict doesn't reproduce.

If this bites you in your own CI, run the Spark integration check as a
separate `python3` invocation (as the Makefile/CI config in this repo
does) rather than folding it into the main `pytest` invocation.

## 3. Query-signature attribute-ID normalization

Spark's analyzer assigns fresh attribute expression IDs (e.g. `amount#123L`)
on every single query compilation, even for two executions of the exact
same recurring query text. `driftdb.metrics.signature.normalize_plan_text`
strips these (along with literals) before hashing, specifically so that a
recurring query maps to one stable signature across executions instead of
fragmenting into a new signature every time. If you extend the signature
logic, keep a test like `tests/test_signature.py::test_normalizes_attribute_ids_and_literals`
around this -- it is easy to reintroduce (e.g. by hashing
`qe.executedPlan()` instead of `qe.analyzed()`, which carries additional
per-execution scheduling metadata beyond just attribute IDs).

## 4. Adaptation remedies also generate their own query-execution events

`Adaptor`'s `refresh_statistics` remedy runs `ANALYZE TABLE ... COMPUTE
STATISTICS`, which is itself a SQL execution the `SparkMetricsCollector`
observes like any other query, under its own signature. This is expected
and harmless (it will show up as a low-frequency, near-instant-selectivity
signature in the metrics store), but don't be surprised to see it if you're
inspecting `distinct_signatures()` output directly.

## 5. Auxiliary cardinality/cost model is advisory, not wired into Catalyst

See `driftdb/adaptation/remedies.py`'s module docstring and
`docs/catalyst_extension_point.md`: the incremental/full-retrain remedies
train a real scikit-learn model on real collected features, but that model
is consumed by DriftDB's own scheduler and alerting, not injected into
Spark's Catalyst cost-based optimizer. Doing the latter requires a compiled
Scala `SparkSessionExtensions` planner-strategy/optimizer-rule, which is a
legitimate and documented Spark extension point, but is out of scope for a
pure-Python package. `refresh_statistics` and `tune_config`, by contrast,
*do* directly change what Spark's built-in optimizer sees and decides.

## 6. Resource sizing for the local demo

The included smoke test / integration check / demo were built and
validated against a 1-vCPU, ~4GB local Spark session (`local[1]`). On such
a small machine, per-query py4j round-trip overhead (walking the executed
plan tree) is a non-trivial fraction of a query's own latency for
tiny/synthetic queries. In a real cluster this overhead is negligible
relative to actual query execution time; if you're benchmarking DriftDB's
own overhead on a similarly small machine, expect the collector's overhead
to look proportionally larger than it would in production.


## 7. `selectivity`'s multi-join heuristic can be swamped by an unrelated filter

Discovered building the TPC-DS evaluation (`docs/tpcds_methodology.md`
Section 5): `driftdb/metrics/collector.py`'s `selectivity` signal sums the
output-row-count of every `Filter`-named node in the executed plan and
divides by total scanned rows. On a three-way join where one table's
filter output count is orders of magnitude larger than another's (e.g. a
~2.9M-row fact-table null-check filter alongside an ~18-55-row
dimension-table equality filter), the large, constant filter dominates the
composite number and hides real, large relative changes in the small
filter's selectivity. Verified case: `selectivity` stayed at 0.9276 across
a ~3.5x change in matching dimension rows and a ~4x change in final result
row count. Workaround used in the TPC-DS evaluation: monitor `output_rows`
(a plain, unaggregated SQLMetric) instead of/alongside `selectivity` for
multi-join queries; `signals_to_monitor`'s default now includes it. A real
fix -- attributing each `Filter` node's selectivity to its own subtree's
scanned rows rather than pooling across the whole plan -- is scoped but
not yet implemented.

## 8. `shuffle_bytes` config typo (fixed)

The default `DriftDBConfig.signals_to_monitor`, the severity classifier's
component-attribution rule, and the tuned-config remedy's trigger table
all referenced a signal name, `shuffle_bytes`, that does not exist on
`QueryMetric` (the real fields are `shuffle_read_bytes` /
`shuffle_write_bytes`). Because `service.py` reads signals via
`getattr(metric, signal, None)` and silently skips `None`, shuffle-based
drift detection had been a complete no-op since the feature was written,
with no error or warning. Fixed in all three places; regression coverage
added in `tests/test_severity.py`.

## 9. A slow adaptation can make a count-based "settled" check fire early

Discovered running the TPC-DS evaluation: `Adaptor.adapt()` runs
synchronously on the same single py4j listener-bus thread that processes
every query's `QueryExecutionListener` callback (see limitation #1's
mechanism). A slow remedy -- a real `ANALYZE TABLE` on a multi-million-row
table measured at ~15-20s in this evaluation -- blocks that thread while
it runs, so a burst of still-pending callbacks for other recently-completed
queries can appear artificially "quiet" to a naive count-based drain
heuristic (`MetricsStore.count()` not changing for several seconds) even
though more activity, including the adaptation's own completion, is still
coming. Verified directly: a genuinely correct adaptation (approved by the
cost-aware scheduler, a real `ANALYZE TABLE` call) was observed in logs
completing about 20 seconds after a count-based drain had already
concluded the metrics store had settled. The evaluation harness
(`examples/run_tpcds_demo.py`) works around this with a flat extra wait
after its drain check; a more precise fix would have the scheduler/adaptor
expose a "processing complete" signal the harness can wait on directly
instead of inferring it from write activity.
