# TPC-DS experimental methodology (reproducible)

This documents exactly how the TPC-DS-grounded evaluation was built, so it
can be reproduced or scaled up for publication-grade numbers.

## 1. Obtaining and building the official toolkit

```bash
git clone https://github.com/gregrahn/tpcds-kit.git
cd tpcds-kit/tools
sudo apt-get install -y bison flex build-essential
# Modern GCC (10+) defaults to -fno-common, which breaks this ~20-year-old
# C code's cross-file tentative definitions (e.g. `int nItemIndex;` repeated
# in multiple .c files). Add -fcommon to the Linux build flags:
sed -i 's/LINUX_CFLAGS\t= -g -Wall/LINUX_CFLAGS\t= -g -Wall -fcommon/' Makefile.suite
make -f Makefile.suite
```

This produces real `dsdgen` (data generator) and `dsqgen` (query generator)
binaries -- Version 2.10.0, TPC copyright 2001-2018, from the tool the TPC
itself publishes for generating TPC-DS benchmark data and queries.

**Licensing note:** the TPC-DS toolkit is distributed under the TPC's own
EULA (`tpcds-kit/EULA.txt`), which does not grant redistribution rights.
This repository does **not** bundle the toolkit source, the binaries, or
any generated data/query files -- only this document, describing how to
regenerate them yourself, plus the loader code that consumes their output.

## 2. Generating data

```bash
mkdir -p tpcds_data
./dsdgen -SCALE 1 -DIR tpcds_data -FORCE Y -TERMINATE N
```

`-SCALE 1` is TPC-DS's smallest defined scale factor (~1GB across all 24
tables; dsdgen itself prints "This scale factor is valid for QUALIFICATION
ONLY", i.e. it's below the minimum scale TPC considers a valid *benchmark*
result, which is expected and fine for a research validation run -- **for
a publication's headline numbers, regenerate at SCALE 100 or SCALE 1000 on
real infrastructure**; the pipeline is unchanged, only the `-SCALE` value
and the machine it runs on).

At SCALE 1 this produced (verified by direct row count after loading):

| table | rows |
|---|---|
| store_sales | 2,880,404 |
| date_dim | 73,049 |
| item | 18,000 |

matching TPC-DS's published SF1 row counts.

## 3. Generating queries with dsqgen

```bash
cd tools
./dsqgen -DIRECTORY ../query_templates -TEMPLATE query3.tpl \
    -DIALECT netezza -SCALE 1 -COUNT 5 -RNGSEED 12345 \
    -OUTPUT_DIR /tmp/out -QUIET Y
```

`-DIALECT netezza` (not `ansi`, despite the name) is used because
`query_templates/netezza.tpl` defines its `LIMIT` clause as `limit %d`,
which happens to be valid Spark SQL; `ansi.tpl` actually emits SQL
Server's `TOP %d` syntax despite its filename.

Query 3 (`query_templates/query3.tpl`) was chosen because it is one of the
simpler three-way joins (`store_sales` + `date_dim` + `item`) in the
official 99-query set, keeping the tables needed small enough to load and
query repeatedly on a resource-constrained validation machine, while still
being a real, unmodified TPC-DS query template.

## 4. Turning Query 3 into a controlled drift workload

Query 3's drift-relevant predicate is `item.i_manufact_id = [MANUFACT]`,
templated in the `.tpl` file as `define MANUFACT = random(1,1000,uniform)`.
This is an **equality** filter, and real generated SF1 `item.dat` has a
fairly flat per-manufacturer-ID item count (mostly 10-20 items out of
18,000 total) with a long thin tail up to ~55. Two independent uniform
draws from `[1,1000]` are *not* guaranteed to differ systematically in
selectivity -- for a controlled experiment we need the independent
variable (selectivity) to move in a known direction, so:

```bash
awk -F'|' '{c[$14]++} END{for (k in c) print k, c[k]}' tpcds_data/item.dat \
    | sort -k2 -n > manufact_counts.txt
```

was used to compute the *real* item count per `i_manufact_id` value from
the actual generated data, and manufacturer IDs were picked from verified
count bands (typical ~15 items, high ~50-55 items) -- see
`examples/tpcds_drift_workload.py` for the exact IDs and counts used, each
one checked against this real distribution, not assumed. Every SQL string
used in the experiment is otherwise byte-for-byte real `dsqgen` output for
`query3.tpl`; only the numeric literal is substituted, which is exactly
the "same recurring query, different literal value" pattern DriftDB's
signature normalization is designed to collapse to a single signature.

Two scenarios are built from this (matching the proposal's Section 7):
- **Abrupt drift**: stable phase draws from the typical-count IDs, then
  steps immediately to the high-count IDs.
- **Gradual drift**: stable phase, then a slow walk through 8 stages of
  increasing verified item count.

## 5. A real bug this experiment surfaced in the core library

Running this scenario against a genuine multi-table join exposed a real
flaw in `driftdb/metrics/collector.py`'s generic `selectivity` heuristic:
it sums the output-row-count of *every* `Filter`-named node in the
executed plan and divides by total scanned rows. For a single-table query
that's fine. For this three-way join, Spark's plan has a `Filter` node on
the *fact* table (`store_sales`, ~2.97M rows, filtering out null join
keys -- a large, constant count) and a separate, much smaller `Filter`
node on the *dimension* table (`item`, filtering to the ~15-55 rows
matching a given `i_manufact_id`). Summing both and dividing by total
scanned rows produces a composite number completely dominated by the
constant, uninteresting fact-table filter (measured: selectivity stayed
at 0.9276 across both the "typical" and "high-cardinality" manufacturer
IDs, even though the actual matching item count changed by ~3.5x and the
final result row count changed from ~30 to ~123 rows). The signal DriftDB
was supposed to detect was real; the aggregate heuristic just couldn't see
it under a much larger, unrelated filter on a different table.

This is documented as a known, real limitation (see
`docs/known_limitations.md`), not hidden: for this evaluation, drift
detection is run against `output_rows` (the query's own final result row
count, a plain SQLMetric already collected verbatim, no heuristic
aggregation involved) rather than the composite `selectivity` signal, and
the config default `signals_to_monitor` now includes `output_rows`
alongside `selectivity`. A proper fix to `selectivity` for multi-join
plans -- attributing each `Filter` node's selectivity to *its own*
subtree's scanned rows rather than pooling across the whole plan -- is
noted as follow-up work rather than rushed in under time pressure for a
result that needs to be trustworthy.

Also fixed while building this experiment: the default
`signals_to_monitor` referenced a signal name, `"shuffle_bytes"`, that
does not exist on `QueryMetric` (the real field is
`shuffle_read_bytes`/`shuffle_write_bytes`), so shuffle-based drift
detection was silently a no-op via `getattr(..., default=None)` since the
feature was first written. Same typo existed in the severity classifier's
component-attribution rule and the tuned-config remedy's trigger table.
All three are fixed and covered by `tests/test_severity.py`.

## 6. An honest limitation of this specific drift design

Because the "drift" here is which manufacturer ID a *fixed* query
structure filters on (not a change to the underlying data volume or a
stale-vs-fresh catalog-statistics mismatch), refreshing catalog statistics
does not change Spark's chosen join strategy in this case -- `item` and
`date_dim` are small enough to broadcast regardless, with or without
`ANALYZE TABLE`. The added latency in the "drift" phase is *genuine
additional computation* (more matching rows to aggregate for a
higher-cardinality manufacturer), not a fixable optimizer misestimation.

This is a real, useful finding, not a shortcoming to hide: it means the
"Recovery Time" metric can legitimately come back `None` / "does not
recover" for this scenario, and the paper should say so plainly. DriftDB's
demonstrated value in this scenario is in **correctly detecting** the
change and **correctly deciding not to over-adapt** (the cost-aware
scheduler shouldn't chase latency increases that no available remedy can
fix) -- not in restoring pre-drift latency, which no version of this
system could honestly do here. A scenario that *would* show a recovery
effect needs a stale-metadata-causes-wrong-plan setup instead (e.g. a join
partner large enough to cross the broadcast threshold after growth) --
worth building as a follow-up, and noted as such in the paper's future
work rather than manufactured into this result.
