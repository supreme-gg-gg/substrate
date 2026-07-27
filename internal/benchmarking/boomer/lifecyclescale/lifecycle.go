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

// Package lifecyclescale implements the synthetic-worker lifecycle benchmark.
package lifecyclescale

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const userClass = "LifecycleScaleUser"

// Mode selects the sufficient-worker or oversubscribed state machine.
type Mode string

const (
	ModeSufficient     Mode = "sufficient"
	ModeOversubscribed Mode = "oversubscribed"
)

// Config contains immutable workload settings and shared dependencies.
type Config struct {
	APIStub        ateapipb.ControlClient
	Dyn            *dynconfig.Holder
	Tracer         trace.Tracer
	Atespace       string
	ActorCount     int
	Mode           Mode
	WorkerHold     time.Duration
	HotResumeCount int
	// CycleRate globally limits sufficient-mode lifecycle cycle starts. Zero
	// leaves the workload unpaced for saturation runs.
	CycleRate float64
}

// Register returns the boomer task and its cleanup hook.
func Register(cfg *Config) (func(), func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/lifecycle-scale")
	}
	rt := &runtimeState{
		cfg:        cfg,
		actorNames: make(chan string, cfg.ActorCount),
	}
	if cfg.CycleRate > 0 {
		rt.cycleLimiter = rate.NewLimiter(rate.Limit(cfg.CycleRate), 1)
	}
	for i := range cfg.ActorCount {
		rt.actorNames <- fmt.Sprintf("scale-actor-%06d", i)
	}
	return rt.iterate, rt.shutdown
}

type runtimeState struct {
	cfg          *Config
	actorNames   chan string
	users        sync.Map
	cycleLimiter *rate.Limiter
}

type lifecycleUser struct {
	mu           sync.Mutex
	actorName    string
	running      bool
	runningSince time.Time
}

func (r *runtimeState) iterate() {
	gid := goroutineID()
	value, loaded := r.users.Load(gid)
	if !loaded {
		var stored bool
		value, stored = r.users.LoadOrStore(gid, &lifecycleUser{})
		if !stored {
			bmetrics.UpdateUsers(userClass, 1)
		}
	}
	user := value.(*lifecycleUser)
	user.mu.Lock()
	defer user.mu.Unlock()
	if user.actorName == "" {
		user.actorName = <-r.actorNames
	}

	switch r.cfg.Mode {
	case ModeSufficient:
		r.runSufficient(user)
	case ModeOversubscribed:
		r.runOversubscribed(user)
	default:
		panic(fmt.Sprintf("unknown lifecycle scale mode %q", r.cfg.Mode))
	}
}

func (r *runtimeState) runSufficient(user *lifecycleUser) {
	ctx := context.Background()
	if r.cycleLimiter != nil {
		if err := r.cycleLimiter.Wait(ctx); err != nil {
			return
		}
	}
	if !user.running {
		if err := r.resume(ctx, user, "ResumeActorCold"); err != nil {
			r.dynamicWait()
			return
		}
		for range r.cfg.HotResumeCount {
			if err := r.call(ctx, "ResumeActorRunning", func(callCtx context.Context, trailer *metadata.MD) error {
				_, err := r.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
					Actor: r.actorRef(user.actorName),
				}, grpc.Trailer(trailer))
				return err
			}); err != nil {
				break
			}
		}
	}
	if r.suspend(ctx, user) {
		r.releaseActor(user)
	}
	r.dynamicWait()
}

func (r *runtimeState) runOversubscribed(user *lifecycleUser) {
	ctx := context.Background()
	if !user.running {
		err := r.resume(ctx, user, "ResumeActorSuccess")
		if err != nil {
			r.dynamicWait()
		}
		return
	}
	if time.Since(user.runningSince) < r.cfg.WorkerHold {
		r.dynamicWait()
		return
	}
	if r.suspend(ctx, user) {
		r.releaseActor(user)
	}
	r.dynamicWait()
}

func (r *runtimeState) resume(ctx context.Context, user *lifecycleUser, successName string) error {
	start := time.Now()
	var trailer metadata.MD
	_, err := r.cfg.APIStub.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: r.actorRef(user.actorName),
	}, grpc.Trailer(&trailer))
	latency := elapsedFromTrailer(trailer, time.Since(start))
	if isNoWorker(err) {
		bmetrics.RecordSuccess("grpc", "ResumeActorNoWorker", userClass, latency, 0)
		return err
	}
	if err != nil {
		bmetrics.RecordFailure("grpc", successName, userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", successName, userClass, latency, 0)
	user.running = true
	user.runningSince = time.Now()
	return nil
}

func (r *runtimeState) suspend(ctx context.Context, user *lifecycleUser) bool {
	err := r.call(ctx, "SuspendActor", func(callCtx context.Context, trailer *metadata.MD) error {
		_, err := r.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: r.actorRef(user.actorName),
		}, grpc.Trailer(trailer))
		return err
	})
	if err != nil {
		return false
	}
	user.running = false
	return true
}

func (r *runtimeState) call(ctx context.Context, name string, invoke func(context.Context, *metadata.MD) error) error {
	ctx, span := r.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var trailer metadata.MD
	err := invoke(ctx, &trailer)
	latency := elapsedFromTrailer(trailer, time.Since(start))
	span.SetAttributes(attribute.Float64("server.elapsed_ms", float64(latency.Nanoseconds())/1e6))
	if err != nil {
		span.RecordError(err)
		bmetrics.RecordFailure("grpc", name, userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, userClass, latency, 0)
	return nil
}

func (r *runtimeState) releaseActor(user *lifecycleUser) {
	name := user.actorName
	user.actorName = ""
	r.actorNames <- name
}

func (r *runtimeState) actorRef(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: name}
}

func (r *runtimeState) dynamicWait() {
	cfg := r.cfg.Dyn.Load()
	delay := cfg.MinWait
	if cfg.MaxWait > cfg.MinWait {
		delay += time.Duration(rand.Float64() * float64(cfg.MaxWait-cfg.MinWait))
	}
	if delay > 0 {
		time.Sleep(delay)
	}
}

func (r *runtimeState) shutdown(ctx context.Context) {
	var wg sync.WaitGroup
	limit := make(chan struct{}, 64)
	r.users.Range(func(_, value any) bool {
		user := value.(*lifecycleUser)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-ctx.Done():
				return
			}
			user.mu.Lock()
			defer user.mu.Unlock()
			actorName := user.actorName
			if actorName != "" {
				if err := r.cleanupActor(ctx, actorName); err != nil {
					slog.WarnContext(ctx, "lifecycle actor cleanup failed",
						slog.String("actor", actorName),
						slog.Any("err", err),
					)
				} else {
					user.running = false
					r.releaseActor(user)
				}
			}
			bmetrics.UpdateUsers(userClass, -1)
		}()
		return ctx.Err() == nil
	})
	wg.Wait()
}

// cleanupActor completes both sides of a possibly interrupted assignment.
// A failed AssignWorker attempt can leave a worker pointing at an actor that
// still appears suspended, so resuming before suspending is intentional: the
// resume workflow adopts that assignment and the suspend workflow then frees
// it deterministically.
func (r *runtimeState) cleanupActor(ctx context.Context, actorName string) error {
	var lastErr error
	for attempt := range 5 {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, lastErr = r.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
			Actor: r.actorRef(actorName),
		})
		cancel()
		if lastErr == nil {
			break
		}
		if !waitForCleanupRetry(ctx, attempt) {
			return lastErr
		}
	}
	if lastErr != nil {
		return fmt.Errorf("resume before cleanup: %w", lastErr)
	}

	for attempt := range 5 {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, lastErr = r.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: r.actorRef(actorName),
		})
		cancel()
		if lastErr == nil {
			return nil
		}
		if !waitForCleanupRetry(ctx, attempt) {
			return lastErr
		}
	}
	return fmt.Errorf("suspend during cleanup: %w", lastErr)
}

func waitForCleanupRetry(ctx context.Context, attempt int) bool {
	delay := 10 * time.Millisecond * time.Duration(1<<attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func isNoWorker(err error) bool {
	return status.Code(err) == codes.FailedPrecondition &&
		strings.Contains(strings.ToLower(status.Convert(err).Message()), "no free workers")
}

func elapsedFromTrailer(trailer metadata.MD, fallback time.Duration) time.Duration {
	values := trailer.Get(ateinterceptors.ServerElapsedTrailer)
	if len(values) == 0 {
		return fallback
	}
	microseconds, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return fallback
	}
	return time.Duration(microseconds) * time.Microsecond
}

func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	line := string(buf[:n])
	const prefix = "goroutine "
	if !strings.HasPrefix(line, prefix) {
		return 0
	}
	end := strings.IndexByte(line[len(prefix):], ' ')
	if end < 0 {
		return 0
	}
	id, _ := strconv.ParseInt(line[len(prefix):len(prefix)+end], 10, 64)
	return id
}

func init() {
	slog.SetLogLoggerLevel(slog.LevelInfo)
}
