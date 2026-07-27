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

// atelet-simulator implements the worker-runtime boundary used by ateapi
// without running gVisor or writing snapshots. It is only for control-plane
// and persistence benchmarks.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type simulator struct {
	ateletpb.UnimplementedAteomHerderServer

	delay time.Duration
	mu    sync.Mutex
	// target ateom UID -> actor UID
	assignments map[string]string
}

func (s *simulator) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	if err := s.claim(ctx, req.GetTargetAteomUid(), req.GetActorUid()); err != nil {
		return nil, err
	}
	return &ateletpb.RunResponse{}, nil
}

func (s *simulator) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	if err := s.claim(ctx, req.GetTargetAteomUid(), req.GetActorUid()); err != nil {
		return nil, err
	}
	return &ateletpb.RestoreResponse{}, nil
}

func (s *simulator) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	assignedActor, found := s.assignments[req.GetTargetAteomUid()]
	if found && assignedActor != req.GetActorUid() {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"worker %q is occupied by actor UID %q, not %q",
			req.GetTargetAteomUid(),
			assignedActor,
			req.GetActorUid(),
		)
	}
	if found {
		delete(s.assignments, req.GetTargetAteomUid())
	}
	// Missing is accepted: Checkpoint is idempotent when ateapi retries after
	// an ambiguous response.
	return &ateletpb.CheckpointResponse{}, nil
}

func (s *simulator) claim(ctx context.Context, workerUID, actorUID string) error {
	if workerUID == "" || actorUID == "" {
		return status.Error(codes.InvalidArgument, "target_ateom_uid and actor_uid are required")
	}
	if err := s.wait(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if assignedActor, found := s.assignments[workerUID]; found {
		if assignedActor == actorUID {
			return nil
		}
		return status.Errorf(
			codes.FailedPrecondition,
			"worker %q is already occupied by actor UID %q",
			workerUID,
			assignedActor,
		)
	}
	s.assignments[workerUID] = actorUID
	return nil
}

func (s *simulator) wait(ctx context.Context) error {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		return nil
	}
}

func main() {
	grpcAddress := flag.String("grpc-address", ":8085", "Address for the synthetic atelet gRPC server.")
	delay := flag.Duration("delay", time.Millisecond, "Fixed processing delay for each simulated runtime RPC.")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	lis, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		logger.Error("failed to listen", slog.String("err", err.Error()))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(grpcServer, &simulator{
		delay:       *delay,
		assignments: make(map[string]string),
	})

	logger.Info("atelet simulator listening",
		slog.String("grpc_address", *grpcAddress),
		slog.Duration("delay", *delay),
	)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("gRPC server stopped", slog.String("err", fmt.Sprint(err)))
		os.Exit(1)
	}
}
