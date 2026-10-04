"""
examples/tpcds_drift_workload.py
===================================
Builds the drift-injected query stream for the evaluation harness out of
TPC-DS Query 3 (join of store_sales, date_dim, item; aggregate revenue by
brand for one manufacturer, one month), generated for real via the
official tpcds-kit `dsqgen` tool against `query_templates/query3.tpl`.

Reproducing the dsqgen call this text came from:

    cd tpcds-kit/tools
    ./dsqgen -DIRECTORY ../query_templates -TEMPLATE query3.tpl \
        -DIALECT netezza -SCALE 1 -COUNT 5 -RNGSEED 12345 \
        -OUTPUT_DIR /tmp/out -QUIET Y

`-DIALECT netezza` is used (not `ansi.tpl`, despite the name) because
netezza.tpl's `LIMIT %d` clause is the one dialect file in the kit that
happens to be directly valid Spark SQL; ansi.tpl actually emits SQL
Server's `TOP %d` syntax.

Why the manufacturer ID (`item.i_manufact_id = [MANUFACT]`) is chosen
deliberately rather than left to dsqgen's own `random(1,1000,uniform)`
draw: that predicate is an *equality* filter, and item-per-manufacturer
counts in real generated TPC-DS data are fairly flat (most values match
10-20 items out of 18,000), so two arbitrary uniform draws from
[1,1000] do not reliably produce a *systematic* selectivity difference
between two experimental phases -- an unlucky RNG draw could make the
"drift" phase accidentally less selective than the "stable" phase. For a
controlled experiment we need the *independent variable* (selectivity) to
move in a known direction, so we measured the real per-manufacturer item
count from the generated `item.dat` (see `docs/tpcds_methodology.md` for
the exact reproduction command) and picked manufacturer IDs from verified
count bands. Every SQL string used below is otherwise byte-for-byte the
real dsqgen output for that template -- only the numeric literal is
substituted, exactly the "same recurring query, different literal value"
pattern DriftDB's signature normalization is designed to collapse to one
signature.
"""

# Real, dsqgen-generated TPC-DS Query 3 (netezza dialect), with {mid}
# standing in for the literal dsqgen itself would have substituted.
QUERY3_TEMPLATE = """select  dt.d_year
       ,item.i_brand_id brand_id
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  date_dim dt
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = {mid}
   and dt.d_moy=11
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100"""

# Manufacturer IDs verified (against a real generated SF1 item.dat; see
# docs/tpcds_methodology.md) to have the item-count shown -- i.e. real
# measured cardinalities, not assumed ones. More items matching
# i_manufact_id => more matching store_sales rows => higher output
# selectivity for the same query structure.
STABLE_MANUFACT_IDS = [345, 394, 400, 410, 442]        # ~15 items each (typical)
ABRUPT_DRIFT_MANUFACT_IDS = [158, 207, 313, 116, 253]  # ~50-55 items each (~3.5x typical)

# A verified, real, monotonically-increasing item-count ramp for the
# gradual-drift scenario: one representative manufacturer ID per stage.
GRADUAL_DRIFT_STAGES = [
    (345, 15), (163, 20), (124, 25), (109, 30),
    (134, 35), (100, 40), (110, 45), (116, 52),
]


def stable_queries(n: int):
    """Cycle through the verified-typical manufacturer IDs."""
    return [QUERY3_TEMPLATE.format(mid=STABLE_MANUFACT_IDS[i % len(STABLE_MANUFACT_IDS)])
            for i in range(n)]


def abrupt_drift_queries(n: int):
    """Cycle through the verified-high-cardinality manufacturer IDs --
    an abrupt step-change in selectivity, matching the proposal's
    'sudden slowdown or mix shift' abrupt-drift scenario."""
    return [QUERY3_TEMPLATE.format(mid=ABRUPT_DRIFT_MANUFACT_IDS[i % len(ABRUPT_DRIFT_MANUFACT_IDS)])
            for i in range(n)]


def gradual_drift_queries(n: int):
    """Walk through the verified item-count ramp, spending n/len(stages)
    queries at each stage -- a slow selectivity shift, matching the
    proposal's 'slow selectivity shift' gradual-drift scenario."""
    per_stage = max(n // len(GRADUAL_DRIFT_STAGES), 1)
    queries = []
    for mid, _count in GRADUAL_DRIFT_STAGES:
        queries.extend([QUERY3_TEMPLATE.format(mid=mid)] * per_stage)
    while len(queries) < n:
        queries.append(QUERY3_TEMPLATE.format(mid=GRADUAL_DRIFT_STAGES[-1][0]))
    return queries[:n]
