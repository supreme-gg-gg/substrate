// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecyclescale

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

type cleanupClient struct {
	ateapipb.ControlClient

	mu              sync.Mutex
	calls           []string
	actorNames      []string
	resumeFailures  int
	suspendFailures int
}

func (c *cleanupClient) ResumeActor(_ context.Context, req *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "resume")
	c.actorNames = append(c.actorNames, req.GetActor().GetName())
	if c.resumeFailures > 0 {
		c.resumeFailures--
		return nil, errors.New("interrupted assignment")
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

func (c *cleanupClient) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "suspend")
	c.actorNames = append(c.actorNames, req.GetActor().GetName())
	if c.suspendFailures > 0 {
		c.suspendFailures--
		return nil, errors.New("assignment still locked")
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func TestCleanupActorAdoptsAssignmentBeforeSuspending(t *testing.T) {
	client := &cleanupClient{resumeFailures: 1, suspendFailures: 1}
	rt := &runtimeState{cfg: &Config{
		APIStub:  client,
		Atespace: "benchmark",
	}}

	if err := rt.cleanupActor(t.Context(), "scale-actor-000001"); err != nil {
		t.Fatalf("cleanupActor: %v", err)
	}

	want := []string{"resume", "resume", "suspend", "suspend"}
	if len(client.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", client.calls, want)
	}
	for i := range want {
		if client.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", client.calls, want)
		}
	}
}

func TestShutdownReadsActorNameUnderUserLock(t *testing.T) {
	client := &cleanupClient{}
	rt := &runtimeState{
		cfg: &Config{
			APIStub:  client,
			Atespace: "benchmark",
		},
		actorNames: make(chan string, 1),
	}
	user := &lifecycleUser{actorName: "scale-actor-000001", running: true}
	rt.users.Store(int64(1), user)

	// Model an iteration changing user state while shutdown begins. Shutdown
	// must wait and then use one stable, non-empty actor name for both RPCs.
	user.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		rt.shutdown(t.Context())
	}()
	user.actorName = "scale-actor-000002"
	user.mu.Unlock()
	<-done

	want := []string{"scale-actor-000002", "scale-actor-000002"}
	if len(client.actorNames) != len(want) {
		t.Fatalf("actor names = %v, want %v", client.actorNames, want)
	}
	for i := range want {
		if client.actorNames[i] != want[i] {
			t.Fatalf("actor names = %v, want %v", client.actorNames, want)
		}
	}
}
