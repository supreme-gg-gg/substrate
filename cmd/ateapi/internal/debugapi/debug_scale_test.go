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

package debugapi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type scaleSeedStore struct {
	store.Interface

	mu      sync.Mutex
	actors  []*ateapipb.Actor
	workers []*ateapipb.Worker
}

func (s *scaleSeedStore) CreateAtespace(_ context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	return atespace, nil
}

func (s *scaleSeedStore) CreateActor(_ context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actors = append(s.actors, actor)
	return actor, nil
}

func (s *scaleSeedStore) CreateWorker(_ context.Context, worker *ateapipb.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers = append(s.workers, worker)
	return nil
}

func (s *scaleSeedStore) ListWorkers(_ context.Context, _ int32, _ string) ([]*ateapipb.Worker, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*ateapipb.Worker(nil), s.workers...), "", nil
}

func TestDebugSeedScaleRefreshesWorkerCacheBeforeReturning(t *testing.T) {
	persistence := &scaleSeedStore{}
	cache := workercache.New(persistence, time.Hour)
	service := NewService(persistence, cache)

	response, err := service.DebugSeedScale(t.Context(), &ateapipb.DebugSeedScaleRequest{
		ActorCount:             10,
		WorkerCount:            3,
		Atespace:               "benchmark",
		ActorTemplateNamespace: "benchmark-workloads",
		ActorTemplateName:      "lifecycle-scale",
	})
	if err != nil {
		t.Fatalf("DebugSeedScale() error = %v", err)
	}
	if response.GetActorsCreated() != 10 || response.GetWorkersCreated() != 3 {
		t.Fatalf("DebugSeedScale() response = %v", response)
	}

	workers, err := cache.Workers()
	if err != nil {
		t.Fatalf("worker cache is not ready: %v", err)
	}
	if len(workers) != 3 {
		t.Fatalf("worker cache contains %d workers, want 3", len(workers))
	}
}
