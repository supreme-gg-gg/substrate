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

"""Worker exhaustion and reuse workload for store-backend comparisons.

Each Locust user owns one actor. A successful ResumeActor holds its worker for
``--worker-hold-time`` seconds before suspending it. With more users than
workers, the remaining users repeatedly receive the expected
FAILED_PRECONDITION/no-free-workers response. Once workers are released, those
users compete to resume.

Expected exhaustion is recorded as ``ResumeActorNoWorker`` and is not counted
as a Locust failure. Any other resume error is recorded as
``ResumeActorUnexpectedFailure`` and does count as a failure. After a user has
released a worker, ``WorkerReacquireWait`` measures how long it waits before a
later resume succeeds; this includes contention and worker-cache propagation.
"""

import logging
import time
import uuid

from common.grpc_setup import init_grpc_gevent

# Patch gRPC before creating a channel.
init_grpc_gevent()

import grpc
import gevent
from locust import User, events, task
from locust.argument_parser import LocustArgumentParser

from common import ateapi_pb2, ateapi_pb2_grpc
from common.atespace import ATESPACE, ensure_atespace
from common.grpc_tracing import _read_server_elapsed_ms
from common.metrics import init_metrics, update_user_count
from common.wait_time import dynamic_wait_time, init_wait_time

logger = logging.getLogger(__name__)

ACTOR_TEMPLATE_NAMESPACE = "benchmark-workloads"
ACTOR_TEMPLATE_NAME = "sleep"

init_metrics()
init_wait_time()


@events.init_command_line_parser.add_listener
def add_worker_contention_args(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--worker-hold-time",
        type=float,
        default=5.0,
        help="Seconds a successful resume holds its worker before suspending",
        include_in_web_ui=True,
    )


def _open_channel():
    with open("/run/servicedns-ca/ca.crt", "rb") as f:
        ca_cert = f.read()
    options = [("grpc.ssl_target_name_override", "api.ate-system.svc")]
    return grpc.secure_channel(
        "api.ate-system.svc.cluster.local:443",
        grpc.ssl_channel_credentials(root_certificates=ca_cert),
        options=options,
    )


def _elapsed_ms(start: float, call: grpc.Call | None) -> float:
    server_ms = _read_server_elapsed_ms(call)
    if server_ms is not None:
        return server_ms
    return (time.monotonic() - start) * 1000


def _record(name: str, start: float, call=None, exception=None) -> None:
    events.request.fire(
        request_type="grpc",
        name=name,
        response_time=_elapsed_ms(start, call),
        response_length=0,
        exception=exception,
        user_class="WorkerContentionUser",
    )


def _is_no_worker(err: grpc.RpcError) -> bool:
    return (
        err.code() == grpc.StatusCode.FAILED_PRECONDITION
        and "no free workers available" in (err.details() or "").lower()
    )


class WorkerContentionUser(User):
    wait_time = dynamic_wait_time
    host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = _open_channel()
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)
        self.running_since: float | None = None
        self.waiting_since: float | None = None
        self.hold_time = self.environment.parsed_options.worker_hold_time

        ensure_atespace(self.stub, self.__class__.__name__)
        self.actor_name = f"worker-contention-{uuid.uuid4()}"
        self.actor_ref = ateapi_pb2.ObjectRef(
            atespace=ATESPACE, name=self.actor_name
        )
        self.stub.CreateActor(
            ateapi_pb2.CreateActorRequest(
                actor=ateapi_pb2.Actor(
                    metadata=ateapi_pb2.ResourceMetadata(
                        atespace=ATESPACE, name=self.actor_name
                    ),
                    # The template selector is workload=benchmark-ateom, which
                    # confines these actors to the worker pool deployed by the
                    # benchmark. Worker selection is intentionally
                    # cluster-wide, so using a demo template here would select
                    # that demo's pool even though the actor atespace is named
                    # "benchmark".
                    actor_template_namespace=ACTOR_TEMPLATE_NAMESPACE,
                    actor_template_name=ACTOR_TEMPLATE_NAME,
                )
            )
        )

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        # Cleanup is deliberately outside timed Locust metrics.
        # Locust can stop a user just as its task RPC is completing. Retry an
        # ABORTED "another operation is in progress" response rather than
        # leaking an assigned actor (and therefore a worker) into the next
        # repeat.
        cleanup_error = None
        for _ in range(10):
            try:
                self.stub.SuspendActor(
                    ateapi_pb2.SuspendActorRequest(actor=self.actor_ref)
                )
                cleanup_error = None
                break
            except grpc.RpcError as err:
                if err.code() == grpc.StatusCode.NOT_FOUND:
                    self.channel.close()
                    return
                cleanup_error = err
                if err.code() != grpc.StatusCode.ABORTED:
                    break
                gevent.sleep(0.1)

        if cleanup_error is None:
            for _ in range(10):
                try:
                    self.stub.DeleteActor(
                        ateapi_pb2.DeleteActorRequest(actor=self.actor_ref)
                    )
                    cleanup_error = None
                    break
                except grpc.RpcError as err:
                    if err.code() == grpc.StatusCode.NOT_FOUND:
                        cleanup_error = None
                        break
                    cleanup_error = err
                    if err.code() != grpc.StatusCode.ABORTED:
                        break
                    gevent.sleep(0.1)

        if cleanup_error is not None:
            logger.warning("worker contention cleanup failed: %s", cleanup_error)
        self.channel.close()

    def _resume(self) -> None:
        start = time.monotonic()
        try:
            _, call = self.stub.ResumeActor.with_call(
                ateapi_pb2.ResumeActorRequest(actor=self.actor_ref)
            )
        except grpc.RpcError as err:
            if _is_no_worker(err):
                # Exhaustion is the condition this workload is intended to
                # create, not a benchmark malfunction.
                _record("ResumeActorNoWorker", start, call=err)
            else:
                _record(
                    "ResumeActorUnexpectedFailure",
                    start,
                    call=err,
                    exception=err,
                )
            return

        _record("ResumeActorSuccess", start, call=call)
        now = time.monotonic()
        if self.waiting_since is not None:
            events.request.fire(
                request_type="worker",
                name="WorkerReacquireWait",
                response_time=(now - self.waiting_since) * 1000,
                response_length=0,
                exception=None,
                user_class=self.__class__.__name__,
            )
            self.waiting_since = None
        self.running_since = now

    def _suspend(self) -> None:
        start = time.monotonic()
        try:
            _, call = self.stub.SuspendActor.with_call(
                ateapi_pb2.SuspendActorRequest(actor=self.actor_ref)
            )
        except grpc.RpcError as err:
            _record("SuspendActor", start, call=err, exception=err)
            return

        _record("SuspendActor", start, call=call)
        self.running_since = None
        self.waiting_since = time.monotonic()

    @task
    def contend_for_worker(self) -> None:
        if self.running_since is None:
            self._resume()
            return
        if time.monotonic() - self.running_since >= self.hold_time:
            self._suspend()
