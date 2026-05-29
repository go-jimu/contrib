package asynq

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type schedulerRuntime interface {
	Register(string, *hibiken.Task, ...hibiken.Option) (string, error)
	Start() error
	Shutdown()
}

// Scheduler adapts asynq periodic task scheduling to taskqueue periodic tasks.
type Scheduler struct {
	runtime schedulerRuntime

	mu      sync.Mutex
	entries map[string]string
}

var (
	_ taskqueue.PeriodicTaskRegistrar = (*Scheduler)(nil)
	_ taskqueue.Runner                = (*Scheduler)(nil)
	_ taskqueue.Worker                = (*Scheduler)(nil)
)

// NewScheduler wraps an asynq scheduler.
func NewScheduler(backend *hibiken.Scheduler) *Scheduler {
	var runtime schedulerRuntime
	if backend != nil {
		runtime = backend
	}
	return newScheduler(runtime)
}

// NewRedisScheduler constructs an asynq scheduler backed by Redis.
func NewRedisScheduler(conn hibiken.RedisConnOpt, opts *hibiken.SchedulerOpts) *Scheduler {
	return NewScheduler(hibiken.NewScheduler(conn, opts))
}

func newScheduler(runtime schedulerRuntime) *Scheduler {
	return &Scheduler{
		runtime: runtime,
		entries: make(map[string]string),
	}
}

// RegisterPeriodicTask registers one periodic task producer.
func (s *Scheduler) RegisterPeriodicTask(periodic taskqueue.PeriodicTask) error {
	if err := periodic.Validate(); err != nil {
		return err
	}
	if s.runtime == nil {
		return ErrNilScheduler
	}

	spec, err := scheduleSpec(periodic.Schedule())
	if err != nil {
		return err
	}
	task, opts, err := taskToAsynqWithPolicy(periodic.Task(), periodic.EnqueuePolicy())
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[periodic.Name()]; ok {
		return taskqueue.ErrDuplicatePeriodicTask
	}
	entryID, err := s.runtime.Register(spec, task, opts...)
	if err != nil {
		return err
	}
	s.entries[periodic.Name()] = entryID
	return nil
}

// Start starts the underlying asynq scheduler.
func (s *Scheduler) Start(ctx context.Context) error {
	if s.runtime == nil {
		return ErrNilScheduler
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.runtime.Start()
}

// Shutdown stops the underlying asynq scheduler and waits for it to finish
// until ctx is canceled.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	if s.runtime == nil {
		return ErrNilScheduler
	}
	done := make(chan struct{})
	go func() {
		s.runtime.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the scheduler until ctx is canceled, then shuts it down.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.runtime == nil {
		return ErrNilScheduler
	}
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	if err := s.Start(context.Background()); err != nil {
		return err
	}
	<-ctx.Done()
	return s.Shutdown(context.Background())
}

func scheduleSpec(schedule taskqueue.Schedule) (string, error) {
	if err := schedule.Validate(); err != nil {
		return "", err
	}
	switch schedule.Kind() {
	case taskqueue.ScheduleKindCron:
		if location := schedule.Location(); location != "" {
			return "CRON_TZ=" + location + " " + schedule.Spec(), nil
		}
		return schedule.Spec(), nil
	case taskqueue.ScheduleKindInterval:
		return fmt.Sprintf("@every %s", schedule.Interval()), nil
	default:
		return "", taskqueue.ErrEmptySchedule
	}
}
