#!/usr/bin/env bash
#
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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

case "${1:-}" in
  --deploy)
    hack/run-tool.sh ko apply -f benchmarking/lifecycle-scale/manifests.yaml
    kubectl rollout restart deployment/atelet-simulator -n ate-system
    kubectl rollout status deployment/atelet-simulator -n ate-system --timeout=120s
    kubectl rollout status deployment/lifecycle-scale \
      -n benchmark-workloads --timeout=180s

    # The ActorTemplate controller does not watch Worker changes. Updating an
    # annotation forces a retry after the bootstrap worker has registered.
    kubectl annotate actortemplate/lifecycle-scale \
      -n benchmark-workloads \
      "benchmark.ate.dev/bootstrap-trigger=$(date +%s)" \
      --overwrite >/dev/null
    kubectl wait --for=condition=Ready actortemplate/lifecycle-scale \
      -n benchmark-workloads --timeout=180s

    # Keep the WorkerPool configuration required by ResumeActor, but remove
    # the real bootstrap worker before synthetic workers are seeded.
    kubectl scale workerpool/lifecycle-scale \
      -n benchmark-workloads --replicas=0 \
      >/dev/null
    kubectl rollout status deployment/lifecycle-scale \
      -n benchmark-workloads --timeout=120s
    ;;
  --delete)
    hack/run-tool.sh ko delete --ignore-not-found \
      -f benchmarking/lifecycle-scale/manifests.yaml
    ;;
  *)
    echo "Usage: $0 --deploy|--delete" >&2
    exit 1
    ;;
esac
