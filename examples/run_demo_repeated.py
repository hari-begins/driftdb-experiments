"""
examples/run_demo_repeated.py
================================
Runs the run_demo.py comparison (static / fixed_interval / driftdb) N times
each, as separate subprocesses (same isolation reasoning as run_demo.py's
own orchestrator: Spark/py4j teardown can occasionally hang, and each
process calls os._exit(0) at the end regardless of teardown state).

Use this before trusting any single-run number (detection latency, false
positive count, etc.) -- one run can't tell you if a number is real or
noise.

Usage:
    python3 examples/run_demo_repeated.py --reps 5
    python3 examples/run_demo_repeated.py --reps 5 --configs driftdb fixed_interval
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RUN_DEMO = os.path.join(HERE, "run_demo.py")

METRICS = [
    "missed_deadlines", "adaptations", "adaptation_cost_s",
    "false_positive_adaptations", "detection_latency_queries",
    "avg_latency_stable_ms", "avg_latency_drift_ms", "wall_time_s",
]


def run_one(config: str, timeout: int = 200) -> dict | None:
    out_file = tempfile.mktemp(prefix=f"driftdb_rep_{config}_", suffix=".json")
    try:
        proc = subprocess.run(
            [sys.executable, RUN_DEMO, "--config", config, "--out", out_file],
            timeout=timeout, capture_output=True, text=True,
        )
        if proc.returncode != 0:
            print(f"    [warn] {config} exited {proc.returncode}")
            print(proc.stdout[-1500:])
            print(proc.stderr[-1500:])
    except subprocess.TimeoutExpired:
        print(f"    [warn] {config} timed out after {timeout}s")

    if os.path.exists(out_file):
        with open(out_file) as f:
            return json.load(f)
    return None


def summarize(values: list) -> str:
    clean = [v for v in values if v is not None]
    if not clean:
        return "n/a (never fired)"
    if len(clean) == 1:
        return f"{clean[0]:.3g} (n=1)"
    mean = statistics.mean(clean)
    std = statistics.pstdev(clean)
    missing = len(values) - len(clean)
    tail = f", {missing} run(s) never fired" if missing else ""
    return f"{mean:.3g} \u00b1 {std:.3g} (n={len(clean)}{tail})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5, help="Repetitions per config")
    ap.add_argument("--configs", nargs="+",
                     default=["static", "fixed_interval", "driftdb"])
    ap.add_argument("--timeout", type=int, default=200)
    ap.add_argument("--out", default=os.path.join(HERE, "demo_results_repeated.json"))
    args = ap.parse_args()

    all_results: dict[str, list[dict]] = {c: [] for c in args.configs}

    for config in args.configs:
        print(f"\n=== {config}: {args.reps} repetitions ===")
        for i in range(args.reps):
            t0 = time.time()
            r = run_one(config, timeout=args.timeout)
            dt = time.time() - t0
            if r is None:
                print(f"  run {i+1}/{args.reps}: FAILED (no result file) [{dt:.0f}s]")
            else:
                print(f"  run {i+1}/{args.reps}: adaptations={r['adaptations']} "
                      f"fp={r['false_positive_adaptations']} "
                      f"detect_lat={r['detection_latency_queries']} "
                      f"missed_dl={r['missed_deadlines']} [{dt:.0f}s]")
                all_results[config].append(r)

    print("\n" + "=" * 100)
    print(f"SUMMARY over {args.reps} repetitions (mean \u00b1 population std)")
    print("=" * 100)
    for config in args.configs:
        runs = all_results[config]
        print(f"\n{config}  (n={len(runs)}/{args.reps} successful runs)")
        if not runs:
            print("  no successful runs -- see warnings above")
            continue
        for m in METRICS:
            vals = [r[m] for r in runs]
            print(f"  {m:<28} {summarize(vals)}")

    with open(args.out, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nRaw per-run results written to {args.out}")


if __name__ == "__main__":
    main()