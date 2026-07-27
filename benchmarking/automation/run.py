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

"""Run a benchmark matrix from this workstation against the cluster selected
by the current kubectl context.

Reuses deploy_substrate()/deploy_workloads() from orchestrator.py, so
backend switching (redis <-> postgres) matches exactly what the scheduled
automation does. The default and recommended ``pod`` execution uses an
isolated runner Pod and copies results back to local disk. ``job`` execution
submits the Kubernetes Job used by scheduled automation and writes results to
GCS.

Usage:
    python3 benchmarking/automation/run.py  # runs every test in tests.yaml
    python3 benchmarking/automation/run.py --tests tests-storage.yaml --only storage_mixed_crud
    python3 benchmarking/automation/run.py --tests tests-capacity.yaml --backend redis
"""

import argparse
import json
import subprocess
import sys
import tarfile
import uuid
from io import BytesIO
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))
import orchestrator as orch  # noqa: E402  (path insert must come first)

REMOTE_DEST = "/tmp/bench-results"
RUNNER_JOB_TEMPLATE = Path(__file__).parent / "manifests/runner-job.yaml.tmpl"


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
    p.add_argument(
        "--execution",
        choices=("job", "pod"),
        default="pod",
        help="Run each case as a Kubernetes Job writing to --dest, or as an "
        "isolated Pod copied to --out (default: pod).",
    )
    p.add_argument(
        "--dest",
        help="Result destination for --execution=job, normally gs://BUCKET/PREFIX.",
    )
    p.add_argument(
        "--tag",
        help="Result tag for --execution=job (default: current git commit).",
    )
    p.add_argument("--namespace", default="benchmarking")
    p.add_argument(
        "--image",
        help="Runner image. By default, use the locust-master image from "
        "--deployment in --namespace.",
    )
    p.add_argument(
        "--deployment",
        default="locust",
        help="Deployment used only to discover the default runner image.",
    )
    p.add_argument(
        "--container",
        default="locust-master",
        help="Container used only to discover the default runner image.",
    )
    p.add_argument("--out", default="./bench-results", help="Local directory to copy results into when done")
    return p.parse_args()


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def resolve_runner_image(args: argparse.Namespace) -> str:
    if args.image:
        return args.image
    try:
        raw = subprocess.run(
            [
                "kubectl",
                "get",
                "deployment",
                args.deployment,
                "-n",
                args.namespace,
                "-o",
                "json",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as e:
        raise SystemExit(
            "Could not discover the runner image from "
            f"deployment/{args.deployment}; deploy it first or pass --image."
        ) from e
    deployment = json.loads(raw)
    for container in deployment["spec"]["template"]["spec"]["containers"]:
        if container["name"] == args.container:
            image = container.get("image")
            if image:
                print(
                    f"Using runner image {image} from "
                    f"deployment/{args.deployment} container {args.container}",
                    flush=True,
                )
                return image
    raise SystemExit(
        f"Container {args.container!r} was not found in "
        f"deployment/{args.deployment}; pass --image explicitly."
    )


def render_runner_pod(
    test: dict,
    image: str,
    namespace: str,
    pod_name: str,
) -> str:
    """Derive a local, idle runner Pod from the scheduled Job template."""
    substitutions = {
        "JOB_NAME": pod_name,
        "IMAGE": image,
        "TEST_FILE": test["file"],
        "DURATION": test["duration"],
        "USERS": test["users"],
        "TAG": "local",
        "NAME": test["name"],
        "DEST": REMOTE_DEST,
    }
    rendered = orch.render_template(
        str(RUNNER_JOB_TEMPLATE), substitutions, test.get("flags", [])
    )
    documents = list(yaml.safe_load_all(rendered))
    job = next(doc for doc in documents if doc and doc.get("kind") == "Job")
    pod_template = job["spec"]["template"]
    metadata = pod_template.get("metadata", {})
    metadata["name"] = pod_name
    metadata["namespace"] = namespace
    spec = pod_template["spec"]
    spec.pop("restartPolicy", None)
    spec["restartPolicy"] = "Never"
    runner = spec["containers"][0]
    runner["command"] = [
        "python3",
        "-c",
        "import time; time.sleep(86400)",
    ]
    runner["args"] = []

    namespace_doc = next(
        doc for doc in documents if doc and doc.get("kind") == "Namespace"
    )
    namespace_doc["metadata"]["name"] = namespace
    service_account = next(
        doc for doc in documents if doc and doc.get("kind") == "ServiceAccount"
    )
    service_account["metadata"]["namespace"] = namespace
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": spec,
    }
    return yaml.safe_dump_all([namespace_doc, service_account, pod])


def create_runner_pod(
    test: dict,
    image: str,
    namespace: str,
) -> str:
    pod_name = f"runner-local-{orch.sanitize(test['name'])[:35]}-{uuid.uuid4().hex[:6]}"
    manifest = render_runner_pod(
        test,
        image,
        namespace,
        pod_name,
    )
    print(f"Creating isolated runner Pod {pod_name}", flush=True)
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        text=True,
        check=True,
    )
    run(
        [
            "kubectl",
            "wait",
            "--for=condition=Ready",
            f"pod/{pod_name}",
            "-n",
            namespace,
            "--timeout=180s",
        ]
    )
    return pod_name


def delete_runner_pod(namespace: str, pod_name: str) -> None:
    subprocess.run(
        [
            "kubectl",
            "delete",
            "pod",
            pod_name,
            "-n",
            namespace,
            "--ignore-not-found",
        ],
        check=False,
    )


def copy_results(namespace: str, pod_name: str, out_dir: Path) -> None:
    """Copy REMOTE_DEST out of the pod and extract it under out_dir.

    Not `kubectl cp`: that execs `tar` inside the container, and the locust
    image doesn't have one (minimal Python base). Stream a tar archive out
    via python's stdlib tarfile module instead -- python3 is guaranteed to
    be there since it's what runner.py itself runs under.
    """
    tar_bytes = subprocess.run(
        ["kubectl", "exec", "-n", namespace, f"pod/{pod_name}", "-c", "runner", "--",
         "python3", "-c",
         "import tarfile, sys; "
         f"tarfile.open(fileobj=sys.stdout.buffer, mode='w|').add({REMOTE_DEST!r}, arcname={Path(REMOTE_DEST).name!r})"],
        check=True, capture_output=True,
    ).stdout
    with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r|") as tf:
        tf.extractall(out_dir)


def run_runner_job(
    test: dict,
    image: str,
    dest: str,
    tag: str,
) -> None:
    """Submit one runner Job and require successful completion."""
    status = orch.run_test(
        test,
        image,
        dest,
        tag,
        runner_job_template=str(RUNNER_JOB_TEMPLATE),
    )
    if status != "complete":
        raise RuntimeError(
            f"runner Job for {test['name']} ended with status {status}"
        )


def main() -> None:
    args = parse_args()
    if args.reuse_current_backend and not args.backend:
        raise SystemExit("--reuse-current-backend requires --backend")
    if args.execution == "job" and not args.dest:
        raise SystemExit("--execution=job requires --dest")
    if args.execution == "job" and args.namespace != "benchmarking":
        raise SystemExit(
            "--execution=job currently requires --namespace=benchmarking"
        )

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

    runner_image = resolve_runner_image(args)
    out_dir = None
    if args.execution == "pod":
        out_dir = Path(args.out).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
    job_tag = args.tag
    if args.execution == "job" and not job_tag:
        job_tag = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()

    last_store_config = None
    workloads_deployed = False
    last_worker_count = None
    lifecycle_scale_deployed = False
    for i, test in enumerate(tests):
        backend = test.get("storeBackend", "redis")
        store_config = (
            backend,
            test.get("postgresConnectionString", ""),
            test.get("ateletSimulatorAddress", ""),
        )
        print(f"\n=== {i + 1}/{len(tests)}: {test['name']} (backend={backend}) ===", flush=True)

        if store_config != last_store_config:
            if last_store_config is None and args.reuse_current_backend:
                print(
                    f"Reusing currently deployed {backend} backend without restarting ateapi",
                    flush=True,
                )
            else:
                # Workers register themselves in the selected store. Leaving
                # existing worker pods alive across a backend switch can give
                # the new backend an empty workers table until those pods
                # happen to restart, invalidating lifecycle/contention tests.
                # Do this on the initial switch too: a prior run.py process
                # may have left workloads registered in the other backend.
                orch.teardown_workloads()
                workloads_deployed = False
                last_worker_count = None
                # benchmarking/workloads includes and deletes the shared
                # benchmark-workloads Namespace, which also contains the
                # lifecycle-scale ActorTemplate and WorkerPool.
                lifecycle_scale_deployed = False
                # Not deploy_substrate(): this assumes substrate is already up
                # and only needs ate-api-server pointed at a different backend
                # (skips the full --deploy-ate-system rebuild/reapply cycle).
                orch.switch_store_backend(
                    backend,
                    test.get("postgresConnectionString", ""),
                    test.get("ateletSimulatorAddress", ""),
                )
            last_store_config = store_config

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
            lifecycle_scale_deployed = False
            admin_pod = create_runner_pod(
                test, runner_image, args.namespace
            )
            try:
                run([
                    "kubectl", "exec", "-n", args.namespace,
                    f"pod/{admin_pod}", "-c", "runner", "--",
                    "python3", "-m", "common.dbadmin", "--reset",
                ])
            finally:
                delete_runner_pod(args.namespace, admin_pod)
        if needs_workloads and test.get("recreateWorkloads", False) and workloads_deployed:
            # Storage capacity runs clear the store, including worker rows.
            # Recreate worker pods before lifecycle/contention cases so every
            # live pod emits a fresh registration event.
            orch.teardown_workloads()
            workloads_deployed = False
            last_worker_count = None
            lifecycle_scale_deployed = False
        if needs_workloads and (
            not workloads_deployed or worker_count != last_worker_count
        ):
            orch.deploy_workloads(worker_count)
            workloads_deployed = True
            last_worker_count = worker_count

        # Deploy this last: teardown_workloads deletes the Namespace shared by
        # regular and lifecycle-scale benchmark fixtures.
        if test.get("deployLifecycleScale", False) and not lifecycle_scale_deployed:
            orch.deploy_lifecycle_scale()
            lifecycle_scale_deployed = True

        repeat = test.get("repeat", 1)
        for rep in range(repeat):
            # Same name every repeat, like orchestrator.py's run_test(): each
            # invocation stamps its own run_ts, so repeats land side by side
            # under runs/<name>/ instead of splitting into separate "tests"
            # that summarize_results.py can no longer group together.
            run_name = test["name"]
            print(f"  rep {rep + 1}/{repeat}", flush=True)
            if test.get("deployLifecycleScale", False):
                orch.restart_lifecycle_scale_simulator()
            if args.execution == "job":
                run_runner_job(
                    test,
                    runner_image,
                    args.dest,
                    job_tag,
                )
                continue
            runner_pod = create_runner_pod(
                test,
                runner_image,
                args.namespace,
            )
            try:
                run([
                    "kubectl", "exec", "-n", args.namespace,
                    f"pod/{runner_pod}", "-c", "runner", "--",
                    "python3", "/app/runner.py",
                    "-f", test["file"],
                    "-t", test["duration"],
                    "-u", str(test["users"]),
                    "--tag", "local",
                    "--name", run_name,
                    "--dest", REMOTE_DEST,
                ] + test.get("flags", []))
                copy_results(args.namespace, runner_pod, out_dir)
            finally:
                delete_runner_pod(args.namespace, runner_pod)

    if args.execution == "job":
        print(f"\nResults uploaded below {args.dest.rstrip('/')}/runs/")
    else:
        results_dir = out_dir / Path(REMOTE_DEST).name
        print(f"\nResults copied to {results_dir}")
        print(
            "Summarize with: python3 "
            "benchmarking/automation/summarize_results.py "
            f"--dest {results_dir} --name <test_name>"
        )


if __name__ == "__main__":
    main()
