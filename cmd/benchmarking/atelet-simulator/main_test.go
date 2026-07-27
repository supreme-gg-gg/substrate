// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSimulatorEnforcesWorkerAssignment(t *testing.T) {
	s := &simulator{assignments: make(map[string]string)}
	run := func(actor string) error {
		_, err := s.Run(t.Context(), &ateletpb.RunRequest{
			TargetAteomUid: "worker-1",
			ActorUid:       actor,
		})
		return err
	}
	checkpoint := func(actor string) error {
		_, err := s.Checkpoint(t.Context(), &ateletpb.CheckpointRequest{
			TargetAteomUid: "worker-1",
			ActorUid:       actor,
		})
		return err
	}

	if err := run("actor-1"); err != nil {
		t.Fatalf("initial Run() error = %v", err)
	}
	if err := run("actor-1"); err != nil {
		t.Fatalf("idempotent Run() error = %v", err)
	}
	if err := run("actor-2"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflicting Run() error = %v, want FailedPrecondition", err)
	}
	if err := checkpoint("actor-2"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong-actor Checkpoint() error = %v, want FailedPrecondition", err)
	}
	if err := checkpoint("actor-1"); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if err := run("actor-2"); err != nil {
		t.Fatalf("Run() after release error = %v", err)
	}
}
