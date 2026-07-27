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

"""Locust-master declaration for the Go lifecycle-scale worker."""

import os

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_lifecycle_scale_args(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--lifecycle-mode",
        choices=("sufficient", "oversubscribed"),
        default="sufficient",
    )
    parser.add_argument("--worker-hold-time", type=float, default=0.25)
    parser.add_argument("--hot-resumes", type=int, default=1)
    parser.add_argument(
        "--lifecycle-cycle-rate",
        type=float,
        default=0,
        help="Global sufficient-mode lifecycle cycle starts per second; zero is unpaced.",
    )


if os.environ.get("LOCUST_NO_GLUTTON_USER") != "1":
    from locust import User, task

    from common.boomer_config import init_boomer_config

    init_boomer_config()

    class LifecycleScaleUser(User):
        host = "api.ate-system.svc.cluster.local:443"

        @task
        def noop(self) -> None:
            pass
