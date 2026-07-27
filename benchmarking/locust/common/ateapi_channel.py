# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Authenticated gRPC channel setup shared by benchmark clients."""

import grpc

DEFAULT_HOST = "api.ate-system.svc.cluster.local:443"
SERVER_NAME = "api.ate-system.svc"
SERVER_CA_PATH = "/run/servicedns-ca/ca.crt"
CLIENT_CREDENTIAL_BUNDLE_PATH = (
    "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
)


def open_channel(host: str = DEFAULT_HOST):
    """Open an mTLS channel using the runner Pod's projected identity."""
    with open(SERVER_CA_PATH, "rb") as f:
        ca_cert = f.read()
    with open(CLIENT_CREDENTIAL_BUNDLE_PATH, "rb") as f:
        client_bundle = f.read()
    credentials = grpc.ssl_channel_credentials(
        root_certificates=ca_cert,
        private_key=client_bundle,
        certificate_chain=client_bundle,
    )
    return grpc.secure_channel(
        host,
        credentials,
        options=[("grpc.ssl_target_name_override", SERVER_NAME)],
    )
