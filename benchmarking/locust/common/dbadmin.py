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

"""Plain-gRPC dataset setup for runner.py, run before the timed test.

This runs in runner.py's own process, not inside locust's gevent-patched
runtime, so it uses blocking grpc calls directly (no traced_grpc/gevent) --
this is setup, not measured workload.
"""

import uuid
from concurrent.futures import ThreadPoolExecutor

from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.ateapi_channel import DEFAULT_HOST, open_channel

HOST = DEFAULT_HOST
ATESPACE = "benchmark"


def build_stubs(host: str = HOST):
    """Open an mTLS channel and return (channel, ControlStub, DebugStub)."""
    channel = open_channel(host)
    return channel, ateapi_pb2_grpc.ControlStub(channel), ateapi_pb2_grpc.DebugStub(channel)


def reset_database(debug_stub) -> None:
    """Truncate all store state via the Debug/DebugClear RPC."""
    debug_stub.DebugClear(ateapi_pb2.DebugClearRequest())


def seed_scale(
    debug_stub,
    actor_count: int,
    worker_count: int,
    atespace: str = ATESPACE,
) -> None:
    """Create the deterministic lifecycle-scale dataset inside ateapi."""
    response = debug_stub.DebugSeedScale(
        ateapi_pb2.DebugSeedScaleRequest(
            actor_count=actor_count,
            worker_count=worker_count,
            atespace=atespace,
            actor_template_namespace="benchmark-workloads",
            actor_template_name="lifecycle-scale",
        ),
        timeout=900,
    )
    if (
        response.actors_created != actor_count
        or response.workers_created != worker_count
    ):
        raise RuntimeError(
            "scale seed returned unexpected counts: "
            f"actors={response.actors_created}, workers={response.workers_created}"
        )


def verify_scale(
    debug_stub,
    actor_count: int,
    worker_count: int,
    atespace: str = ATESPACE,
):
    """Return scale counts and fail if actor/worker invariants are violated."""
    response = debug_stub.DebugVerifyScale(
        ateapi_pb2.DebugVerifyScaleRequest(
            atespace=atespace,
            expected_actor_count=actor_count,
            expected_worker_count=worker_count,
        ),
        timeout=300,
    )
    if response.violations:
        raise RuntimeError(
            "scale consistency verification failed:\n  "
            + "\n  ".join(response.violations)
        )
    return response


def _create_one_actor(control_stub, atespace: str) -> None:
    name = f"sb-{uuid.uuid4()}"
    control_stub.CreateActor(
        ateapi_pb2.CreateActorRequest(
            actor=ateapi_pb2.Actor(
                metadata=ateapi_pb2.ResourceMetadata(atespace=atespace, name=name),
                actor_template_namespace="ate-demo-counter",
                actor_template_name="counter",
            )
        )
    )


def preload_actors(
    control_stub, atespace: str = ATESPACE, count: int = 0, concurrency: int = 20
) -> None:
    """Ensure `atespace` exists, then create `count` actors in it concurrently."""
    if count <= 0:
        return
    try:
        control_stub.CreateAtespace(
            ateapi_pb2.CreateAtespaceRequest(
                atespace=ateapi_pb2.Atespace(
                    metadata=ateapi_pb2.ResourceMetadata(name=atespace)
                )
            )
        )
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(_create_one_actor, control_stub, atespace)
            for _ in range(count)
        ]
        for f in futures:
            f.result()


def _main() -> None:
    """Ad hoc CLI for manual use, e.g. from a running locust pod:
    kubectl exec -n benchmarking deploy/locust -c locust-master -- \\
        python3 -m common.dbadmin --reset --preload 100
    """
    import argparse

    p = argparse.ArgumentParser(description=_main.__doc__)
    p.add_argument("--reset", action="store_true", help="Truncate all store state.")
    p.add_argument("--preload", type=int, default=0, help="Number of actors to create.")
    p.add_argument("--scale-actors", type=int, default=0)
    p.add_argument("--scale-workers", type=int, default=0)
    p.add_argument("--verify-scale", action="store_true")
    p.add_argument("--atespace", default=ATESPACE)
    args = p.parse_args()

    channel, control_stub, debug_stub = build_stubs()
    try:
        if args.reset:
            print("Resetting store state...")
            reset_database(debug_stub)
            print("Reset done.")
        if args.preload > 0:
            print(f"Preloading {args.preload} actors into {args.atespace!r}...")
            preload_actors(control_stub, atespace=args.atespace, count=args.preload)
            print("Preload done.")
        if args.scale_actors > 0 or args.scale_workers > 0:
            if args.scale_actors <= 0 or args.scale_workers <= 0:
                p.error("--scale-actors and --scale-workers must be used together")
            print(
                f"Seeding {args.scale_actors} scale actors and "
                f"{args.scale_workers} synthetic workers..."
            )
            seed_scale(
                debug_stub,
                actor_count=args.scale_actors,
                worker_count=args.scale_workers,
                atespace=args.atespace,
            )
            print("Scale seed done.")
        if args.verify_scale:
            response = verify_scale(
                debug_stub,
                actor_count=args.scale_actors,
                worker_count=args.scale_workers,
                atespace=args.atespace,
            )
            print(response)
        if (
            not args.reset
            and args.preload <= 0
            and args.scale_actors <= 0
            and not args.verify_scale
        ):
            p.print_help()
    finally:
        channel.close()


if __name__ == "__main__":
    _main()
