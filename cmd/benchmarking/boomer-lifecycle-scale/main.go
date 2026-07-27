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

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/glutton"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/lifecyclescale"
	btrace "github.com/agent-substrate/substrate/internal/benchmarking/boomer/trace"
	"github.com/myzhan/boomer"
)

func main() {
	apiEndpoint := flag.String("api-endpoint", "api.ate-system.svc.cluster.local:443", "ateapi gRPC endpoint.")
	atespace := flag.String("atespace", "benchmark", "Pre-seeded lifecycle-scale atespace.")
	mode := flag.String("mode", string(lifecyclescale.ModeSufficient), "sufficient|oversubscribed")
	actorCount := flag.Int("actor-count", 100_000, "Number of deterministic pre-seeded actors.")
	workerHold := flag.Duration("worker-hold-time", 250*time.Millisecond, "How long oversubscribed users retain a worker.")
	hotResumes := flag.Int("hot-resumes", 1, "Already-running ResumeActor calls per sufficient lifecycle.")
	cycleRate := flag.Float64("lifecycle-cycle-rate", 0, "Global sufficient-mode lifecycle cycle starts per second; zero disables pacing.")
	configJSON := flag.String("config-json", "", "Initial dynamic wait/trace configuration.")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if *actorCount < 1 {
		slog.Error("--actor-count must be positive")
		os.Exit(2)
	}
	if *cycleRate < 0 {
		slog.Error("--lifecycle-cycle-rate must not be negative")
		os.Exit(2)
	}
	selectedMode := lifecyclescale.Mode(*mode)
	if selectedMode != lifecyclescale.ModeSufficient && selectedMode != lifecyclescale.ModeOversubscribed {
		slog.Error("--mode must be sufficient or oversubscribed")
		os.Exit(2)
	}

	initialConfig, err := dynconfig.Parse(
		[]byte(*configJSON),
		dynconfig.Config{MinWait: 100 * time.Millisecond, MaxWait: 250 * time.Millisecond},
	)
	if err != nil {
		slog.Error("failed to parse --config-json", slog.String("err", err.Error()))
		os.Exit(1)
	}

	ctx := context.Background()
	sampler := btrace.NewUpdatableSampler(initialConfig.TraceProbability)
	tp, err := btrace.Init(ctx, "substrate-boomer-lifecycle-scale", sampler)
	if err != nil {
		slog.Error("failed to initialize tracing", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()

	conn, apiStub, err := glutton.DialControl(*apiEndpoint)
	if err != nil {
		slog.Error("failed to dial ateapi", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	task, shutdown := lifecyclescale.Register(&lifecyclescale.Config{
		APIStub:        apiStub,
		Dyn:            dynconfig.NewHolder(initialConfig),
		Atespace:       *atespace,
		ActorCount:     *actorCount,
		Mode:           selectedMode,
		WorkerHold:     *workerHold,
		HotResumeCount: *hotResumes,
		CycleRate:      *cycleRate,
	})

	boomer.Run(&boomer.Task{Name: "LifecycleScaleUser", Weight: 1, Fn: task})

	// Leave headroom before runner.py's 90-second hard process timeout so the
	// worker can log cleanup failures, flush telemetry, and exit normally.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdown(shutdownCtx)
}
