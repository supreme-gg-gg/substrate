# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Summarize repeated runner.py runs into per-RPC medians, and (once both
backends have runs) a side-by-side comparison.

runner.py produces one stats.jsonl per invocation, uploaded under
<dest>/runs/<name>/run_date=.../run_ts=.../run_tag=.../stats.jsonl. This
script reads every repeat for one or more `--name`s (local disk only; a
`--dest` under gs:// would need downloading first) and reports the median,
min, and max across repeats for each RPC's throughput and latency, plus
total failure count. Names ending in `_redis` / `_postgres` are grouped so
running it with both a redis and a postgres name prints them side by side.

Usage:
    python3 summarize_results.py --dest /path/to/results \\
        --name storage_mixed_crud_redis --name storage_mixed_crud_postgres
"""

import argparse
import json
import statistics
from pathlib import Path

# Locust CSV columns kept as strings by runner.py's stats_to_jsonl; these are
# the ones worth summarizing per RPC.
NUMERIC_FIELDS = [
    "request_count",
    "failure_count",
    "requests_per_s",
    "average_response_time",
    "p50",
    "p95",
    "p99",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dest", required=True, help="Local root passed as runner.py's --dest")
    p.add_argument(
        "--name", action="append", dest="names", default=[],
        help="A runner.py --name value; repeat for multiple (e.g. one per backend)",
    )
    p.add_argument(
        "--prefix",
        help="Include every test directory under <dest>/runs whose name starts with this prefix",
    )
    p.add_argument(
        "--metric", action="append", dest="metrics", default=[],
        help="Only print this metric; repeat for multiple metrics (e.g. Aggregated)",
    )
    return p.parse_args()


def backend_of(name: str) -> str:
    for suffix in ("_redis", "_postgres"):
        if name.endswith(suffix):
            return suffix.lstrip("_")
    return "unknown"


def load_runs(dest: str, name: str) -> list[dict]:
    """Return one dict of per-metric rows per run, for every stats.jsonl
    found under <dest>/runs/<name>/."""
    runs = []
    for stats_path in sorted(Path(dest, "runs", name).glob("*/*/*/stats.jsonl")):
        rows = {}
        for line in stats_path.read_text().splitlines():
            entry = json.loads(line)
            rows[entry["metric"]] = entry["measurements"]
        runs.append(rows)
    return runs


def summarize(runs: list[dict]) -> dict:
    """metric -> field -> {median, min, max} across runs, ignoring runs/fields
    that are missing or non-numeric."""
    metrics = sorted({m for run in runs for m in run})
    summary = {}
    for metric in metrics:
        summary[metric] = {}
        for field in NUMERIC_FIELDS:
            values = []
            for run in runs:
                raw = run.get(metric, {}).get(field)
                if raw in (None, ""):
                    continue
                try:
                    values.append(float(raw))
                except ValueError:
                    continue
            if values:
                summary[metric][field] = {
                    "median": statistics.median(values),
                    "min": min(values),
                    "max": max(values),
                }
    return summary


def print_summary(name: str, runs: list[dict], summary: dict, metrics: list[str]) -> None:
    print(f"\n=== {name} ({backend_of(name)}, {len(runs)} run(s)) ===")
    if not runs:
        print("  no runs found")
        return
    for metric, fields in summary.items():
        if metrics and metric not in metrics:
            continue
        print(f"  {metric}:")
        for field in NUMERIC_FIELDS:
            stats = fields.get(field)
            if not stats:
                continue
            print(
                f"    {field:22s} median={stats['median']:.2f} "
                f"min={stats['min']:.2f} max={stats['max']:.2f}"
            )


def main() -> None:
    args = parse_args()
    names = list(args.names)
    if args.prefix:
        runs_dir = Path(args.dest, "runs")
        names.extend(
            p.name for p in sorted(runs_dir.glob(f"{args.prefix}*")) if p.is_dir()
        )
    names = list(dict.fromkeys(names))
    if not names:
        raise SystemExit("provide at least one --name or --prefix")

    by_backend: dict[str, list[str]] = {}
    for name in names:
        runs = load_runs(args.dest, name)
        summary = summarize(runs)
        print_summary(name, runs, summary, args.metrics)
        by_backend.setdefault(backend_of(name), []).append(name)

    if len(by_backend) > 1:
        print(f"\n=== backends compared: {', '.join(sorted(by_backend))} ===")
        print("Re-run with matching case names per backend for a side-by-side diff.")


if __name__ == "__main__":
    main()
