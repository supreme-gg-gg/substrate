#!/usr/bin/env python3
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

"""Run benchmarking/automation/tests.yaml against the cluster your current
kubectl context points at, using the substrate/locust already deployed
there. No CronJob, no orchestration cluster, no image build/push.

Reuses deploy_substrate()/deploy_workloads() from orchestrator.py, so
backend switching (redis <-> postgres) matches exactly what the scheduled
automation does. Runs runner.py via `kubectl exec` in the existing `locust`
Deployment instead of submitting a Kubernetes Job per test, so results land
on local disk without needing a GCS bucket + IAM binding first -- see
benchmarking/automation/README.md for the Job-per-test path that mirrors
CI exactly, if you already have a --dest bucket set up.

Usage:
    python3 benchmarking/automation/run_local.py  # runs every test in tests.yaml
    python3 benchmarking/automation/run_local.py --tests test-storage.yaml --only storage_mixed_crud
    python3 benchmarking/automation/run_local.py --tests tests-capacity.yaml \
        --tests tests-worker-contention.yaml --backend redis
"""

import argparse
import subprocess
import sys
import tarfile
from io import BytesIO
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))
import orchestrator as orch  # noqa: E402  (path insert must come first)

REMOTE_DEST = "/tmp/bench-results"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--tests",
        action="append",
        dest="test_files",
        help="Tests YAML to load; repeat to combine matrices (default: tests.yaml)",
    )
    p.add_argument("--only", help="Only run tests whose name contains this substring")
    p.add_argument(
        "--backend",
        choices=("redis", "postgres"),
        help="Only run entries for this backend",
    )
    p.add_argument(
        "--reuse-current-backend",
        action="store_true",
        help="Do not restart ateapi for the initial backend; requires --backend and assumes the cluster is already configured for it",
    )
    p.add_argument("--namespace", default="benchmarking")
    p.add_argument("--deployment", default="locust")
    p.add_argument("--container", default="locust-master")
    p.add_argument("--out", default="./bench-results", help="Local directory to copy results into when done")
    return p.parse_args()


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def copy_results(namespace: str, deployment: str, container: str, out_dir: Path) -> None:
    """Copy REMOTE_DEST out of the pod and extract it under out_dir.

    Not `kubectl cp`: that execs `tar` inside the container, and the locust
    image doesn't have one (minimal Python base). Stream a tar archive out
    via python's stdlib tarfile module instead -- python3 is guaranteed to
    be there since it's what runner.py itself runs under.
    """
    tar_bytes = subprocess.run(
        ["kubectl", "exec", "-n", namespace, f"deployment/{deployment}", "-c", container, "--",
         "python3", "-c",
         "import tarfile, sys; "
         f"tarfile.open(fileobj=sys.stdout.buffer, mode='w|').add({REMOTE_DEST!r}, arcname={Path(REMOTE_DEST).name!r})"],
        check=True, capture_output=True,
    ).stdout
    with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r|") as tf:
        tf.extractall(out_dir)


def main() -> None:
    args = parse_args()
    if args.reuse_current_backend and not args.backend:
        raise SystemExit("--reuse-current-backend requires --backend")

    test_files = args.test_files or [str(Path(__file__).parent / "tests.yaml")]
    tests = []
    for test_file in test_files:
        tests.extend(yaml.safe_load(Path(test_file).read_text())["tests"])
    if args.only:
        tests = [t for t in tests if args.only in t["name"]]
    if args.backend:
        tests = [
            t for t in tests
            if t.get("storeBackend", "redis") == args.backend
        ]
    if not tests:
        sys.exit("No tests matched the requested filters")

    last_backend = None
    workloads_deployed = False
    last_worker_count = None
    for i, test in enumerate(tests):
        backend = test.get("storeBackend", "redis")
        print(f"\n=== {i + 1}/{len(tests)}: {test['name']} (backend={backend}) ===", flush=True)

        if backend != last_backend:
            if last_backend is None and args.reuse_current_backend:
                print(
                    f"Reusing currently deployed {backend} backend without restarting ateapi",
                    flush=True,
                )
            else:
                # Workers register themselves in the selected store. Leaving
                # existing worker pods alive across a backend switch can give
                # the new backend an empty workers table until those pods
                # happen to restart, invalidating lifecycle/contention tests.
                # Do this on the initial switch too: a prior run_local process
                # may have left workloads registered in the other backend.
                orch.teardown_workloads()
                workloads_deployed = False
                last_worker_count = None
                # Not deploy_substrate(): this assumes substrate is already up
                # and only needs ate-api-server pointed at a different backend
                # (skips the full --deploy-ate-system rebuild/reapply cycle).
                orch.switch_store_backend(backend, test.get("postgresConnectionString", ""))
            last_backend = backend

        needs_workloads = test.get("deployWorkloads", True)
        worker_count = test.get("workerCount", 1)
        if test.get("resetStoreBeforeWorkloads", False):
            # DebugClear removes worker rows too, so no worker pods may remain
            # alive across this reset: existing pods would not necessarily
            # emit another registration event. This also removes stale rows
            # for pods from an earlier run before the fresh workers start.
            orch.teardown_workloads()
            workloads_deployed = False
            last_worker_count = None
            run([
                "kubectl", "exec", "-n", args.namespace,
                f"deployment/{args.deployment}", "-c", args.container, "--",
                "python3", "-m", "common.dbadmin", "--reset",
            ])
        if needs_workloads and test.get("recreateWorkloads", False) and workloads_deployed:
            # Storage capacity runs clear the store, including worker rows.
            # Recreate worker pods before lifecycle/contention cases so every
            # live pod emits a fresh registration event.
            orch.teardown_workloads()
            workloads_deployed = False
            last_worker_count = None
        if needs_workloads and (
            not workloads_deployed or worker_count != last_worker_count
        ):
            orch.deploy_workloads(worker_count)
            workloads_deployed = True
            last_worker_count = worker_count

        repeat = test.get("repeat", 1)
        for rep in range(repeat):
            # Same name every repeat, like orchestrator.py's run_test(): each
            # invocation stamps its own run_ts, so repeats land side by side
            # under runs/<name>/ instead of splitting into separate "tests"
            # that summarize_results.py can no longer group together.
            run_name = test["name"]
            print(f"  rep {rep + 1}/{repeat}", flush=True)
            run([
                "kubectl", "exec", "-n", args.namespace,
                f"deployment/{args.deployment}", "-c", args.container, "--",
                "python3", "runner.py",
                "-f", test["file"],
                "-t", test["duration"],
                "-u", str(test["users"]),
                "--tag", "local",
                "--name", run_name,
                "--dest", REMOTE_DEST,
            ] + test.get("flags", []))

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    copy_results(args.namespace, args.deployment, args.container, out_dir)
    results_dir = out_dir / Path(REMOTE_DEST).name
    print(f"\nResults copied to {results_dir}")
    print(f"Summarize with: python3 benchmarking/automation/summarize_results.py --dest {results_dir} --name <test_name>")


if __name__ == "__main__":
    main()
