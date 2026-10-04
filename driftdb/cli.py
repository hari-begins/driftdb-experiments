"""driftdb CLI -- run the demo, inspect the metrics store, or manage
manual overrides without writing Python."""
from __future__ import annotations

import argparse
import json
import sys

from .config import DriftDBConfig
from .metrics.store import MetricsStore


def cmd_status(args):
    store = MetricsStore(args.db)
    sigs = store.distinct_signatures()
    print(f"Metrics DB: {args.db}")
    print(f"Total observations: {store.count()}")
    print(f"Distinct query signatures: {len(sigs)}")
    for sig in sigs[: args.limit]:
        n = len(store.signal_series(sig, "latency_ms"))
        latest = store.latest(sig, 1)
        last_latency = latest[0]["latency_ms"] if latest else None
        print(f"  {sig}  n={n}  last_latency_ms={last_latency}")


def cmd_demo(args):
    from examples.run_demo import main as demo_main
    demo_main()


def main(argv=None):
    parser = argparse.ArgumentParser(prog="driftdb")
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="Summarize a metrics DB")
    p_status.add_argument("--db", default="driftdb_metrics.sqlite")
    p_status.add_argument("--limit", type=int, default=20)
    p_status.set_defaults(func=cmd_status)

    p_demo = sub.add_parser("demo", help="Run the local TPC-DS-style drift demo")
    p_demo.set_defaults(func=cmd_demo)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
