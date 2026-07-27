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

package debugapi

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxScaleActors     = 1_000_000
	maxScaleWorkers    = 100_000
	scaleSeedWorkers   = 32
	scalePageSize      = 1000
	scaleWorkerNS      = "benchmark-workloads"
	scaleWorkerPool    = "lifecycle-scale"
	maxViolationReport = 100
)

// DebugSeedScale creates deterministic suspended actors and idle worker
// records through store.Interface. It is setup-only and is not timed by the
// benchmark.
func (s *Service) DebugSeedScale(ctx context.Context, req *ateapipb.DebugSeedScaleRequest) (*ateapipb.DebugSeedScaleResponse, error) {
	if err := validateDebugSeedScaleRequest(req); err != nil {
		return nil, err
	}

	_, err := s.persistence.CreateAtespace(ctx, &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: req.GetAtespace()},
	})
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return nil, fmt.Errorf("creating scale atespace: %w", err)
	}

	if err := s.seedActors(ctx, req); err != nil {
		return nil, err
	}
	for i := int32(0); i < req.GetWorkerCount(); i++ {
		worker := &ateapipb.Worker{
			WorkerNamespace: scaleWorkerNS,
			WorkerPool:      scaleWorkerPool,
			WorkerPod:       scaleWorkerName(i),
			Ip:              fmt.Sprintf("198.18.%d.%d", (i/254)%256, i%254+1),
			WorkerPodUid:    fmt.Sprintf("scale-worker-uid-%06d", i),
			NodeName:        fmt.Sprintf("scale-node-%03d", i%100),
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"workload": "lifecycle-scale"},
		}
		if err := s.persistence.CreateWorker(ctx, worker); err != nil {
			return nil, fmt.Errorf("creating scale worker %d: %w", i, err)
		}
	}
	if s.workerCache != nil {
		if err := s.workerCache.Refresh(ctx); err != nil {
			return nil, fmt.Errorf("refreshing worker cache after scale seed: %w", err)
		}
	}

	return &ateapipb.DebugSeedScaleResponse{
		ActorsCreated:  req.GetActorCount(),
		WorkersCreated: req.GetWorkerCount(),
	}, nil
}

func (s *Service) seedActors(ctx context.Context, req *ateapipb.DebugSeedScaleRequest) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int32)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	workerCount := scaleSeedWorkers
	if int(req.GetActorCount()) < workerCount {
		workerCount = int(req.GetActorCount())
	}
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				actor := &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{
						Atespace: req.GetAtespace(),
						Name:     scaleActorName(i),
					},
					Status:                 ateapipb.Actor_STATUS_SUSPENDED,
					ActorTemplateNamespace: req.GetActorTemplateNamespace(),
					ActorTemplateName:      req.GetActorTemplateName(),
					LatestSnapshotInfo: &ateapipb.SnapshotInfo{
						Data: &ateapipb.SnapshotInfo_External{
							External: &ateapipb.ExternalSnapshotInfo{
								SnapshotUriPrefix: fmt.Sprintf("gs://synthetic-lifecycle-scale/%s/%06d", req.GetAtespace(), i),
							},
						},
					},
				}
				if _, err := s.persistence.CreateActor(ctx, actor); err != nil {
					select {
					case errCh <- fmt.Errorf("creating scale actor %d: %w", i, err):
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		defer close(jobs)
		for i := int32(0); i < req.GetActorCount(); i++ {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	<-feedDone
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

// DebugVerifyScale scans the benchmark population and checks the bidirectional
// actor/worker assignment invariant.
func (s *Service) DebugVerifyScale(ctx context.Context, req *ateapipb.DebugVerifyScaleRequest) (*ateapipb.DebugVerifyScaleResponse, error) {
	if req == nil || req.GetAtespace() == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace is required")
	}

	actors, err := s.listScaleActors(ctx, req.GetAtespace())
	if err != nil {
		return nil, err
	}
	workers, err := s.listScaleWorkers(ctx)
	if err != nil {
		return nil, err
	}

	resp := &ateapipb.DebugVerifyScaleResponse{
		ActorCount:  int32(len(actors)),
		WorkerCount: int32(len(workers)),
	}
	if req.GetExpectedActorCount() > 0 && resp.GetActorCount() != req.GetExpectedActorCount() {
		addViolation(resp, "actor count is %d, want %d", resp.GetActorCount(), req.GetExpectedActorCount())
	}
	if req.GetExpectedWorkerCount() > 0 && resp.GetWorkerCount() != req.GetExpectedWorkerCount() {
		addViolation(resp, "worker count is %d, want %d", resp.GetWorkerCount(), req.GetExpectedWorkerCount())
	}

	actorByKey := make(map[string]*ateapipb.Actor, len(actors))
	for _, actor := range actors {
		key := actorKey(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())
		actorByKey[key] = actor
		switch actor.GetStatus() {
		case ateapipb.Actor_STATUS_SUSPENDED:
			resp.SuspendedActorCount++
		case ateapipb.Actor_STATUS_RUNNING:
			resp.RunningActorCount++
		case ateapipb.Actor_STATUS_RESUMING, ateapipb.Actor_STATUS_SUSPENDING:
			resp.TransitionalActorCount++
		}
		if actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
			addViolation(resp, "actor %s has status %s after benchmark cleanup", key, actor.GetStatus())
		}
		if actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED && actor.GetAteomPodName() != "" {
			addViolation(resp, "suspended actor %s still points to worker %s/%s", key, actor.GetAteomPodNamespace(), actor.GetAteomPodName())
		}
	}

	workerByKey := make(map[string]*ateapipb.Worker, len(workers))
	for _, worker := range workers {
		key := workerKey(worker.GetWorkerNamespace(), worker.GetWorkerPod())
		workerByKey[key] = worker
		if worker.GetAssignment() == nil {
			continue
		}
		resp.AssignedWorkerCount++
		addViolation(resp, "worker %s remains assigned after benchmark cleanup", key)
		ref := worker.GetAssignment().GetActor()
		actor := actorByKey[actorKey(ref.GetAtespace(), ref.GetName())]
		if actor == nil {
			addViolation(resp, "worker %s points to missing actor %s/%s", key, ref.GetAtespace(), ref.GetName())
			continue
		}
		if actor.GetAteomPodNamespace() != worker.GetWorkerNamespace() || actor.GetAteomPodName() != worker.GetWorkerPod() {
			addViolation(resp, "worker %s and actor %s/%s do not point to each other", key, ref.GetAtespace(), ref.GetName())
		}
	}

	for _, actor := range actors {
		if actor.GetAteomPodName() == "" {
			continue
		}
		worker := workerByKey[workerKey(actor.GetAteomPodNamespace(), actor.GetAteomPodName())]
		if worker == nil {
			addViolation(resp, "actor %s/%s points to missing worker %s/%s", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), actor.GetAteomPodNamespace(), actor.GetAteomPodName())
			continue
		}
		ref := worker.GetAssignment().GetActor()
		if ref.GetAtespace() != actor.GetMetadata().GetAtespace() || ref.GetName() != actor.GetMetadata().GetName() {
			addViolation(resp, "actor %s/%s and worker %s/%s do not point to each other", actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), worker.GetWorkerNamespace(), worker.GetWorkerPod())
		}
	}

	return resp, nil
}

func (s *Service) listScaleActors(ctx context.Context, atespace string) ([]*ateapipb.Actor, error) {
	var result []*ateapipb.Actor
	token := ""
	for {
		page, next, err := s.persistence.ListActors(ctx, atespace, scalePageSize, token)
		if err != nil {
			return nil, fmt.Errorf("listing scale actors: %w", err)
		}
		result = append(result, page...)
		if next == "" {
			return result, nil
		}
		token = next
	}
}

func (s *Service) listScaleWorkers(ctx context.Context) ([]*ateapipb.Worker, error) {
	var result []*ateapipb.Worker
	token := ""
	for {
		page, next, err := s.persistence.ListWorkers(ctx, scalePageSize, token)
		if err != nil {
			return nil, fmt.Errorf("listing scale workers: %w", err)
		}
		for _, worker := range page {
			if worker.GetWorkerNamespace() == scaleWorkerNS && worker.GetWorkerPool() == scaleWorkerPool {
				result = append(result, worker)
			}
		}
		if next == "" {
			return result, nil
		}
		token = next
	}
}

func validateDebugSeedScaleRequest(req *ateapipb.DebugSeedScaleRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetActorCount() < 1 || req.GetActorCount() > maxScaleActors {
		return status.Errorf(codes.InvalidArgument, "actor_count must be between 1 and %d", maxScaleActors)
	}
	if req.GetWorkerCount() < 1 || req.GetWorkerCount() > maxScaleWorkers {
		return status.Errorf(codes.InvalidArgument, "worker_count must be between 1 and %d", maxScaleWorkers)
	}
	if req.GetAtespace() == "" || req.GetActorTemplateNamespace() == "" || req.GetActorTemplateName() == "" {
		return status.Error(codes.InvalidArgument, "atespace and actor template identity are required")
	}
	return nil
}

func scaleActorName(i int32) string {
	return fmt.Sprintf("scale-actor-%06d", i)
}

func scaleWorkerName(i int32) string {
	return fmt.Sprintf("scale-worker-%06d", i)
}

func actorKey(atespace, name string) string {
	return atespace + "/" + name
}

func workerKey(namespace, pod string) string {
	return namespace + "/" + pod
}

func addViolation(resp *ateapipb.DebugVerifyScaleResponse, format string, args ...any) {
	if len(resp.Violations) < maxViolationReport {
		resp.Violations = append(resp.Violations, fmt.Sprintf(format, args...))
	}
}
