# Going further: wiring a learned model into Catalyst directly

DriftDB's `refresh_statistics` and `tune_config` remedies (see
`driftdb/adaptation/remedies.py`) directly change what Spark's built-in
cost-based optimizer sees: `ANALYZE TABLE ... COMPUTE STATISTICS` updates
the catalog statistics Catalyst's cardinality estimation already consumes,
and `tune_config` changes real `spark.sql.*` session configuration. Those
are genuine, in-production-today optimizer levers.

The `incremental_update` / `full_retrain` remedies go a step further and
train DriftDB's own auxiliary scikit-learn model (a selectivity/latency
bias-correction model per query signature). As shipped, that model is
consumed by DriftDB itself (the cost-aware scheduler's benefit estimate,
the alerting interface), not by Spark's planner. If you want Spark's own
optimizer to consult a custom learned cardinality or cost estimator when
choosing a plan -- not just when deciding *whether to adapt* -- that
requires a small compiled Spark extension, because Catalyst's planner
internals aren't exposed to pure-Python code. This is a well-documented,
first-class Spark extension point, and it is where a production
deployment of DriftDB's "component-level adaptation" would eventually
plug in a custom estimator, not a hack:

```scala
// A minimal sketch of the extension point -- not a complete implementation.
// See: org.apache.spark.sql.SparkSessionExtensions

class DriftDBCardinalityRule(session: SparkSession) extends Rule[LogicalPlan] {
  // DriftDB's Python service exposes its latest per-signature bias-correction
  // model via the ModelStore (driftdb/store/model_store.py). A thin Scala
  // client (HTTP, or reading the same pickle-adjacent metadata DriftDB
  // writes) loads the current correction factor for a matching query
  // signature and adjusts the Statistics.rowCount / sizeInBytes Catalyst
  // uses during cardinality estimation, before the physical planner picks
  // a join strategy.
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case f @ Filter(_, child) =>
        // adjust child.stats using DriftDB's latest correction for this
        // query's signature, computed the same way
        // driftdb.metrics.signature.query_signature() does in Python
        f
    }
  }
}

class DriftDBExtensions extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(session => new DriftDBCardinalityRule(session))
  }
}
```

Registered via `spark.sql.extensions=com.yourorg.DriftDBExtensions` on
cluster startup. This is genuinely a separate engineering project (a
compiled Scala artifact deployed alongside your Spark distribution) from
the Python monitoring/adaptation service in this repository, which is why
it's documented here rather than shipped: DriftDB's Python side already
does the hard part (detecting *when* and attributing *what* changed), and
this extension point is where the correction actually gets applied inside
the optimizer, if your organization wants that last mile.
