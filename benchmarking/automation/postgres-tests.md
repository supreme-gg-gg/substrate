# Store backend comparison (docs/postgres-store.md)

`tests.yaml` has `_redis` and `_postgres` twins of every storage/lifecycle
case (point-read-heavy, mixed CRUD, list load at increasing actor counts,
lifecycle with sufficient/oversubscribed workers), each repeated 5x via
`repeat`. Postgres entries set `storeBackend: postgres` and
`postgresConnectionString` (a YAML anchor, `*postgres_dsn`, defined once at
the top of the file) — the orchestrator passes both through to
`hack/install-ate.sh` as `ATE_API_STORE_BACKEND` /
`ATE_API_POSTGRES_CONNECTION_STRING` before deploying, running
`--deploy-postgres` first when the backend is postgres. Because
`ensure_apiserver_prerequisites` only creates the `ate-api-server-envvars`
ConfigMap if it's missing, `switch_store_backend()` in `orchestrator.py`
recreates it and does a `kubectl rollout restart deployment/ate-api-server`
whenever the backend changes between tests, without touching the rest of
substrate (CRDs, atenet, atelet, valkey) — `deploy_substrate()` calls this
too, as one step of a full from-scratch deploy.

Two `runner.py` flags support this matrix:

- `--reset-before` truncates all store state (via the `Debug/DebugClear` RPC)
  before the run, so leftover actors from a previous case don't bias list or
  create behavior. Works identically for both backends — `DebugClearAll` is
  implemented in both `ateredis` and `atepg`.
- `--preload N` creates `N` actors in the benchmark atespace before the run,
  for the list-at-scale cases.

You can also run either manually, any time, without a test run:

```bash
kubectl exec -n benchmarking deploy/locust -c locust-master -- \
  python3 -m common.dbadmin --reset --preload 1000
```

After a matrix has run for a backend, summarize repeats with:

```bash
python3 benchmarking/automation/summarize_results.py --dest <local-dest> \
  --name storage_mixed_crud_redis --name storage_mixed_crud_postgres
```

Passing both `_redis` and `_postgres` names prints them side by side.

## Store capacity sweep

`tests-capacity.yaml` is a separate zero-wait mixed-CRUD matrix at 50 and 200
concurrent users. Each level runs three times for both backends.
Keeping it separate avoids rerunning the original latency and lifecycle matrix.
The runner uses a spawn rate of 50 users/second, so even the 200-user case
spends most of its 60-second window at full concurrency.
Because these CRUD calls never resume actors, this matrix also skips workload
deployment and the ActorTemplate readiness wait entirely. Its measured load is
12 minutes total, or six minutes per backend.

Against an already-deployed cluster, run it with:

```bash
# Rebuild/redeploy once so the in-pod runner includes Aggregated RPS output.
./benchmarking/deploy_locust.sh --deploy

python3 benchmarking/automation/run.py \
  --tests benchmarking/automation/tests-capacity.yaml \
  --out ./bench-results-capacity
```

Summarize achieved aggregate RPS, latency, and failures with:

```bash
python3 benchmarking/automation/summarize_results.py \
  --dest ./bench-results-capacity/bench-results \
  --prefix capacity_crud_ \
  --metric Aggregated
```

For the scheduled orchestrator path, generate a ConfigMap whose mounted key
remains `tests.yaml`, then apply it in place of the ordinary test list:

```bash
kubectl create configmap substrate-benchmark-tests \
  --namespace substrate-benchmark \
  --from-file=tests.yaml=benchmarking/automation/tests-capacity.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

The saturation sweep intentionally records only the metrics already emitted by
Locust. CPU, memory, and database I/O collection are not required to run it.

## Worker contention and reuse

`tests-worker-contention.yaml` runs 30 actors against 10 workers. Successful
actors hold workers for five seconds, so the pool becomes genuinely exhausted;
after suspension, released workers are reused by waiting actors. Expected
`no free workers available` responses are reported separately and do not count
as Locust failures. Unexpected resume errors still fail normally.
The actors use `benchmark-workloads/sleep`; that template's
`workload=benchmark-ateom` selector isolates the test to the benchmark pool.
Actor atespaces do not provide worker isolation because pool selection is
cluster-wide. Before each backend case, the local runner tears down workloads,
clears that backend's store, and recreates the ten workers; ordering the reset
this way prevents stale worker rows without deleting registrations belonging
to live pods.

Rebuild the Locust image once to include `worker_contention.py`, then run the
matrix against an already-deployed cluster:

```bash
./benchmarking/deploy_locust.sh --deploy

python3 benchmarking/automation/run.py \
  --tests benchmarking/automation/tests-worker-contention.yaml \
  --out ./bench-results-worker-contention
```

Summarize the important metrics with:

```bash
python3 benchmarking/automation/summarize_results.py \
  --dest ./bench-results-worker-contention/bench-results \
  --prefix worker_contention_ \
  --metric grpc_ResumeActorSuccess \
  --metric grpc_ResumeActorNoWorker \
  --metric grpc_ResumeActorUnexpectedFailure \
  --metric grpc_SuspendActor \
  --metric worker_WorkerReacquireWait
```

Every valid contention run should contain both `ResumeActorSuccess` and
`ResumeActorNoWorker`, demonstrating that the pool was exhausted and later
made progress. `ResumeActorUnexpectedFailure` should be absent (or have zero
failures), and `WorkerReacquireWait` compares release/reuse behavior between
backends.

## Select backend-specific tests by name

`run.py` accepts multiple `--tests` files and filters on test-name
substrings. Run all Redis capacity and worker cases together:

```bash
python3 benchmarking/automation/run.py \
  --tests benchmarking/automation/tests-capacity.yaml \
  --tests benchmarking/automation/tests-worker-contention.yaml \
  --only _redis \
  --out ./bench-results-redis
```

Then run all PostgreSQL cases together:

```bash
python3 benchmarking/automation/run.py \
  --tests benchmarking/automation/tests-capacity.yaml \
  --tests benchmarking/automation/tests-worker-contention.yaml \
  --only _postgres \
  --out ./bench-results-postgres
```

Each invocation configures the backend of its first matched test, switching
again only if later matched tests name a different backend. If ateapi is
already configured for the first test's backend, add
`--reuse-current-backend` to skip the initial rollout restart. Only use that
option when the backend is known to be ready; the runner cannot infer
ateapi's current store safely.

## Running without the CronJob/orchestration cluster

If you already have substrate deployed on your current kubectl context (e.g.
a personal GKE dev cluster), you don't need the CronJob or a separate
orchestration cluster. The recommended isolated-Pod mode copies results to
local disk before deleting each Pod:

```bash
python3 benchmarking/automation/run.py \
  --execution pod \
  --image <runner-image> \
  --out ./bench-results
```

For unattended automation, `run.py` can instead submit the same per-test
Kubernetes Jobs used by the scheduled orchestrator and upload results to GCS:

```bash
python3 benchmarking/automation/run.py \
  --execution job \
  --image <runner-image> \
  --dest gs://<bucket>/<prefix>
```

If `--image` is omitted, `run.py` discovers it from the existing
`locust-master` Deployment. That Deployment does not participate in either
execution mode.

It's deliberately cheap between tests: workloads are reused while their worker
count is unchanged, tests with `deployWorkloads: false` skip them entirely, and
`switch_store_backend()` — the lightweight ConfigMap-recreate + rollout-restart,
*not* the full `deploy_substrate()`/`--deploy-ate-system` rebuild-and-reapply
cycle — only runs when the backend changes. The CronJob in "Setup" above is
only a *scheduler* for periodic/CI runs; nothing requires it for an ad hoc pass
against a cluster you already have open.
