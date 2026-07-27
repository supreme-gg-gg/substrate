# Lifecycle-scale benchmark

This benchmark measures the `ateapi` lifecycle and persistence path with
100,000 registered actors and 1,000 synthetic workers. It does not create
1,000 worker Pods or run gVisor for every lifecycle operation. A single atelet
simulator implements the worker-runtime gRPC boundary with a fixed 1 ms delay.

The reportable matrix is
[`tests-lifecycle-scale-comparison.yaml`](../automation/tests-lifecycle-scale-comparison.yaml).
It runs matched Redis and PostgreSQL cases for:

- controlled lifecycle latency at 100 cycles per second;
- sustained use of approximately 1,000 workers; and
- zero-wait lifecycle saturation.

PostgreSQL uses the selected final configuration: a 16-connection client pool,
a 1-CPU request, and a 2-CPU limit.

The Kubernetes Job workflow is the recommended way to run the matrix. See
[Reproducing the ATEPG benchmarks](../REPRODUCING_ATEPG_BENCHMARKS.md) for the
complete GKE setup, execution, and result-validation procedure.

## Components

- `deploy.sh` and `manifests.yaml` deploy the simulator, bootstrap
  `WorkerPool`, and `ActorTemplate`.
- `cmd/benchmarking/atelet-simulator` implements the synthetic runtime.
- `cmd/benchmarking/boomer-lifecycle-scale` is the Go load-generator worker.
- `internal/benchmarking/boomer/lifecyclescale` implements the workload state
  machine.
- `DebugSeedScale` creates the deterministic actor and worker population
  outside the measured window.
- `DebugVerifyScale` validates actor/worker consistency after each run.
