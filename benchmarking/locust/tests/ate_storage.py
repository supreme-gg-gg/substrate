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

"""Storage-focused workload: create/get/update/list/delete actor.

This is the "storage-focused API workload" from
docs/postgres-store.md's benchmark workloads. Unlike AteAPIUser
(ate_api.py), it never calls ResumeActor/SuspendActor, so it exercises the
store backend (ateredis today, atepg later) without worker scheduling or
snapshot overhead mixed in.

StorageCrudUser covers the "mixed CRUD load" and (combined with runner.py's
--preload flag) "list load at increasing actor counts" matrix cases.
StorageReadUser covers the "point-read-heavy load" case.
"""

import uuid

from common.grpc_setup import init_grpc_gevent

# Patch gRPC to cooperate with locust's gevent loop before any channel exists.
init_grpc_gevent()

import grpc
from locust import User, task
from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.ateapi_channel import open_channel as open_ateapi_channel
from common.atespace import ATESPACE, ensure_atespace
from common.grpc_tracing import traced_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing
from common.wait_time import init_wait_time, dynamic_wait_time
import logging

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()


def _open_channel():
    return open_ateapi_channel()


class StorageCrudUser(User):
    """Each iteration: create, get, update, list, delete one actor."""

    wait_time = dynamic_wait_time
    host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = _open_channel()
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)
        self._iteration = 0
        try:
            ensure_atespace(self.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure atespace {ATESPACE}: {e}")

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        self.channel.close()

    @task
    def crud_cycle(self) -> None:
        self._iteration += 1
        actor_name = f"sb-{uuid.uuid4()}"
        actor_ref = ateapi_pb2.ObjectRef(atespace=ATESPACE, name=actor_name)

        try:
            with traced_grpc("CreateActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.CreateActor.with_call(
                    ateapi_pb2.CreateActorRequest(
                        actor=ateapi_pb2.Actor(
                            metadata=ateapi_pb2.ResourceMetadata(
                                atespace=ATESPACE, name=actor_name
                            ),
                            actor_template_namespace="ate-demo-counter",
                            actor_template_name="counter",
                        )
                    ),
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Failed to create actor {actor_name}: {e}")
            return

        try:
            with traced_grpc("GetActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.GetActor.with_call(
                    ateapi_pb2.GetActorRequest(actor=actor_ref), metadata=metadata
                )
        except Exception:
            pass

        try:
            with traced_grpc("UpdateActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.UpdateActor.with_call(
                    ateapi_pb2.UpdateActorRequest(
                        actor=actor_ref,
                        worker_selector=ateapi_pb2.Selector(
                            match_labels={"iteration": str(self._iteration)}
                        ),
                    ),
                    metadata=metadata,
                )
        except Exception:
            pass

        try:
            with traced_grpc("ListActors", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.ListActors.with_call(
                    ateapi_pb2.ListActorsRequest(atespace=ATESPACE, page_size=50),
                    metadata=metadata,
                )
        except Exception:
            pass

        try:
            with traced_grpc("DeleteActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.DeleteActor.with_call(
                    ateapi_pb2.DeleteActorRequest(actor=actor_ref), metadata=metadata
                )
        except Exception as e:
            logger.error(f"Failed to delete actor {actor_name}: {e}")


class StorageReadUser(User):
    """Point-read-heavy: create one actor, then hammer GetActor on it."""

    wait_time = dynamic_wait_time
    host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = _open_channel()
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)

        try:
            ensure_atespace(self.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure atespace {ATESPACE}: {e}")

        self.actor_name = f"sb-{uuid.uuid4()}"
        self.actor_ref = ateapi_pb2.ObjectRef(atespace=ATESPACE, name=self.actor_name)
        try:
            with traced_grpc("CreateActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.CreateActor.with_call(
                    ateapi_pb2.CreateActorRequest(
                        actor=ateapi_pb2.Actor(
                            metadata=ateapi_pb2.ResourceMetadata(
                                atespace=ATESPACE, name=self.actor_name
                            ),
                            actor_template_namespace="ate-demo-counter",
                            actor_template_name="counter",
                        )
                    ),
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_name}: {e}")

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        try:
            with traced_grpc("DeleteActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.DeleteActor.with_call(
                    ateapi_pb2.DeleteActorRequest(actor=self.actor_ref),
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Failed to delete actor {self.actor_name}: {e}")
        self.channel.close()

    @task
    def get_actor(self) -> None:
        try:
            with traced_grpc("GetActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.GetActor.with_call(
                    ateapi_pb2.GetActorRequest(actor=self.actor_ref),
                    metadata=metadata,
                )
        except Exception:
            pass
