# Substrate benchmark automation

Scheduled, repeatable benchmark runs of a substrate branch. A CronJob on an
**orchestration cluster** drives the full build/deploy/run/teardown cycle
against a separate **test cluster**, once per entry in [tests.yaml](tests.yaml).
Each run uploads results to GCS via `benchmarking/locust/runner.py`.

## How it works

1. CronJob fires on the orchestration cluster; pod starts (orchestrator +
   DIND sidecar).
2. Orchestrator waits for DIND, copies the appropriate `ate-dev-env.sh`, runs
   `gcloud container clusters get-credentials` for the test cluster.
3. Shallow-clones `--repo` at `--branch`, captures the commit hash.
4. `docker build && docker push` builds the locust image tagged with the commit
   hash and pushes it to `${KO_DOCKER_REPO}/locust-test:<commit>`.
5. `hack/install-ate.sh --deploy-ate-system` + `benchmarking/workloads/deploy.sh
   --deploy` (these build & push substrate / workload images via `ko` as part
   of their deploy steps — there's no separate `make build-images` step).
6. For each test in `tests.yaml`:
   - Submits a Job using the just-built locust image; the Job runs
     `runner.py -f <file> -t <duration> -u <users> --tag <commit> --name <name>
     --dest <dest>`.
   - Polls until complete/failed/timeout; tails logs; deletes the Job.
   - Tears down substrate + workloads.
   - If not the last test, redeploys them so the next run starts clean.

## Setup

```bash
./benchmarking/automation/setup.sh
```

The wizard treats the repo's `.ate-dev-env.sh` as the source of truth for the
target cluster's environment and snapshots it to
`scratch/target-clusters/<name>.sh`. It only prompts for the target cluster
name (the routing key in `tests.yaml`) and the GCP project ID of the
orchestrator image registry. It then builds + pushes the orchestrator image
to `gcr.io/<ORCH_PROJECT_ID>/ate-images/substrate-benchmark-orchestrator:<short-commit>`
(with a `-dirty` suffix if `benchmarking/automation/` has uncommitted
changes) and renders `scratch/cronjob.yaml`, `scratch/test-list.yaml`, and
`scratch/target-clusters.yaml`. You can edit the `.ate-dev-env.sh` for your
workload cluster directly in the config map. 

Then edit the `--repo / --branch / --dest` args in `scratch/cronjob.yaml` and
apply:

```bash
kubectl --context=<orchestration-cluster> apply -f scratch/cronjob.yaml
```

To trigger immediately instead of waiting for the schedule:

```bash
kubectl --context=<orchestration-cluster> -n substrate-benchmark \
  create job --from=cronjob/substrate-benchmark manual-$(date +%s)
```

To change the schedule, edit `spec.schedule` in `scratch/cronjob.yaml` (the
default is `0 3 * * *`, 3am UTC).

## Test cluster prerequisites

Create the test cluster with the substrate-required beta APIs and Workload
Identity enabled. The control plane must be on Kubernetes 1.36+ so
`certificates.k8s.io/v1beta1` is available:

```bash
gcloud container clusters create <CLUSTER_NAME> \
  --location=<CLUSTER_LOCATION> \
  --num-nodes=5 \
  --workload-pool=<PROJECT_ID>.svc.id.goog \
  --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles
```

The orchestration cluster needs Workload Identity but no special APIs. It only
ever runs one pod (the orchestrator + DIND sidecar), so a single zonal node
keeps costs to the minimum:

```bash
gcloud container clusters create <ORCH_CLUSTER_NAME> \
  --location=<ORCH_ZONE> \
  --workload-pool=<ORCH_PROJECT_ID>.svc.id.goog \
  --num-nodes=1
```

## IAM prerequisites

This setup assumes both clusters and the destination GCS bucket already exist.
Two Workload Identity bindings are needed.

Both ServiceAccounts are created by the manifests (`cronjob.yaml` for the
orchestrator KSA, `runner-job.yaml.tmpl` for the runner KSA — applied by
`orchestrator.py` at runtime), so no `kubectl create serviceaccount` steps are
needed. Grant IAM roles directly to each KSA's Workload Identity principal
(no GSA / annotation required). The principal format is:

```
principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/<NAMESPACE>/sa/<KSA>
```

**Orchestrator pod** (KSA `substrate-benchmark-orchestrator` in namespace
`substrate-benchmark` on the orchestration cluster's project) needs:

- `roles/container.admin` on the test cluster's project — required to manage
  cluster-scoped resources (CRDs, ClusterRoles, ClusterRoleBindings,
  Namespaces) that `hack/install-ate.sh --deploy-ate-system` creates.
  `container.developer` is not enough — it intentionally omits the
  `container.clusterRoles.*` and `container.customResourceDefinitions.*`
  permissions.
- `roles/artifactregistry.writer` on `KO_DOCKER_REPO` — for `ko` (substrate
  images) and `docker push` (locust image).

```bash
ORCH_PRINCIPAL="principal://iam.googleapis.com/projects/<ORCH_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<ORCH_PROJECT_ID>.svc.id.goog/subject/ns/substrate-benchmark/sa/substrate-benchmark-orchestrator"
gcloud projects add-iam-policy-binding <TEST_PROJECT_ID> \
  --role=roles/container.admin --member="${ORCH_PRINCIPAL}"
gcloud projects add-iam-policy-binding <TEST_PROJECT_ID> \
  --role=roles/artifactregistry.writer --member="${ORCH_PRINCIPAL}"
```

**Runner Job pod** (KSA `benchmark-runner` in namespace `benchmarking` on the
test cluster's project) needs `roles/storage.objectUser` on the destination
bucket so `runner.py` can upload results:

```bash
RUNNER_PRINCIPAL="principal://iam.googleapis.com/projects/<TEST_PROJECT_NUMBER>/locations/global/workloadIdentityPools/<TEST_PROJECT_ID>.svc.id.goog/subject/ns/benchmarking/sa/benchmark-runner"
gcloud storage buckets add-iam-policy-binding gs://<DEST_BUCKET> \
  --role=roles/storage.objectCreator --member="${RUNNER_PRINCIPAL}"
```

## Updating tests

`tests.yaml` is delivered to the orchestrator via a ConfigMap mounted at
`/etc/orchestrator/tests.yaml`, so the image doesn't need to be rebuilt when
the test list changes. Just reapply the config map.

## Store backend comparison (docs/postgres-store-prototype.md)

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

### Store capacity sweep

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

python3 benchmarking/automation/run_local.py \
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

### Worker contention and reuse

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

python3 benchmarking/automation/run_local.py \
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

### Run both matrices with only one backend switch

`run_local.py` accepts multiple `--tests` files and a backend filter. Run all
Redis capacity and worker cases together:

```bash
python3 benchmarking/automation/run_local.py \
  --tests benchmarking/automation/tests-capacity.yaml \
  --tests benchmarking/automation/tests-worker-contention.yaml \
  --backend redis \
  --out ./bench-results-redis
```

Then switch once and run all PostgreSQL cases together:

```bash
python3 benchmarking/automation/run_local.py \
  --tests benchmarking/automation/tests-capacity.yaml \
  --tests benchmarking/automation/tests-worker-contention.yaml \
  --backend postgres \
  --out ./bench-results-postgres
```

Each invocation configures its selected backend once. If ateapi is already
configured for that backend, add `--reuse-current-backend` to skip even the
initial rollout restart. Only use that option when the selected backend is
known to be ready; the runner cannot infer ateapi's current store safely.

Note: this only captures throughput, per-RPC latency (p50/p95/p99), and
error/failure counts — nothing in the repo currently scrapes container-level
CPU/memory/network/storage I/O for `ateapi` or the database, which the doc's
benchmark plan also calls for. Adding that would mean extending
`manifests/ate-install/kind/prometheus.yaml` (or the GKE equivalent) to
scrape cAdvisor and querying Prometheus for those metrics after each run —
left as follow-up. `valkey.yaml` and `postgres.yaml` do now set CPU/memory
requests+limits (previously unset on both), so at least resource
*allocation* is fixed and comparable per the doc's requirement to record it
— actual usage still needs to be read manually (`kubectl top pod -n
ate-system`) until that scraping exists.

## Running without the CronJob/orchestration cluster

If you already have substrate + locust deployed on your current kubectl
context (e.g. a personal GKE dev cluster), you don't need the CronJob,
the separate orchestration cluster, or a fresh image build to run
`tests.yaml` — `run_local.py` drives the same `tests.yaml`, but runs
`runner.py` via `kubectl exec` in the existing `locust` Deployment instead
of submitting a Kubernetes Job per test. That sidesteps the GCS bucket +
Workload Identity binding the Job-based `--dest` normally needs (see "IAM
prerequisites" above) — results just land in the pod's
`/tmp/bench-results`, which `run_local.py` copies out for you at the end:

```bash
python3 benchmarking/automation/run_local.py                       # every test in tests.yaml
python3 benchmarking/automation/run_local.py --only storage_mixed_crud  # substring filter, matches both backend twins
```

It's deliberately cheap between tests: workloads are reused while their worker
count is unchanged, tests with `deployWorkloads: false` skip them entirely, and
`switch_store_backend()` — the lightweight ConfigMap-recreate + rollout-restart,
*not* the full `deploy_substrate()`/`--deploy-ate-system` rebuild-and-reapply
cycle — only runs when the backend changes. The CronJob in "Setup" above is
only a *scheduler* for periodic/CI runs; nothing requires it for an ad hoc pass
against a cluster you already have open.
