package asynq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: Scheduler should adapt a component periodic task into an asynq
// scheduler registration while preserving task envelope and enqueue policy.
func TestSchedulerRegisterPeriodicTaskMapsTaskAndPolicy(t *testing.T) {
	fake := &fakeSchedulerRuntime{}
	scheduler := newScheduler(fake)
	task, err := taskqueue.New(
		taskqueue.Definition{Type: "billing.generate_invoices.v1", Queue: "billing"},
		[]byte(`{"date":"2026-05-29"}`),
		taskqueue.WithKey("billing:2026-05-29"),
		taskqueue.WithHeader("trace-id", "trace-1"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	schedule, err := taskqueue.CronSchedule("0 2 * * *", taskqueue.WithLocation("Asia/Shanghai"))
	if err != nil {
		t.Fatalf("CronSchedule: %v", err)
	}
	periodic, err := taskqueue.NewPeriodicTask(
		"billing.daily_invoices",
		schedule,
		task,
		taskqueue.WithUnique(25*time.Hour),
		taskqueue.WithMaxRetry(3),
	)
	if err != nil {
		t.Fatalf("NewPeriodicTask: %v", err)
	}

	err = scheduler.RegisterPeriodicTask(periodic)

	if err != nil {
		t.Fatalf("RegisterPeriodicTask: %v", err)
	}
	if len(fake.registrations) != 1 {
		t.Fatalf("registrations = %d, want 1", len(fake.registrations))
	}
	got := fake.registrations[0]
	if got.spec != "CRON_TZ=Asia/Shanghai 0 2 * * *" {
		t.Fatalf("spec = %q", got.spec)
	}
	if got.task.Type() != "billing.generate_invoices.v1" {
		t.Fatalf("type = %q", got.task.Type())
	}
	if string(got.task.Payload()) != `{"date":"2026-05-29"}` {
		t.Fatalf("payload = %s", got.task.Payload())
	}
	if got.task.Headers()["trace-id"] != "trace-1" {
		t.Fatalf("headers = %#v", got.task.Headers())
	}

	values := optionValues(got.opts)
	if values[hibiken.QueueOpt] != "billing" {
		t.Fatalf("queue option = %#v", values[hibiken.QueueOpt])
	}
	if values[hibiken.TaskIDOpt] != "billing:2026-05-29" {
		t.Fatalf("task id option = %#v", values[hibiken.TaskIDOpt])
	}
	if values[hibiken.UniqueOpt] != 25*time.Hour {
		t.Fatalf("unique option = %#v", values[hibiken.UniqueOpt])
	}
	if values[hibiken.MaxRetryOpt] != 3 {
		t.Fatalf("max retry option = %#v", values[hibiken.MaxRetryOpt])
	}
}

// Intent: Interval schedules should map to asynq's supported @every syntax
// instead of requiring application code to know provider cron extensions.
func TestSchedulerRegisterPeriodicTaskMapsInterval(t *testing.T) {
	fake := &fakeSchedulerRuntime{}
	scheduler := newScheduler(fake)
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.reconcile.v1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	schedule, err := taskqueue.IntervalSchedule(15 * time.Minute)
	if err != nil {
		t.Fatalf("IntervalSchedule: %v", err)
	}
	periodic, err := taskqueue.NewPeriodicTask("document.reconcile", schedule, task)
	if err != nil {
		t.Fatalf("NewPeriodicTask: %v", err)
	}

	err = scheduler.RegisterPeriodicTask(periodic)

	if err != nil {
		t.Fatalf("RegisterPeriodicTask: %v", err)
	}
	if fake.registrations[0].spec != "@every 15m0s" {
		t.Fatalf("spec = %q", fake.registrations[0].spec)
	}
}

// Intent: The asynq adapter should rely on the component contract to reject
// periodic schedules and policies that do not have stable repeated-enqueue
// semantics before any provider registration happens.
func TestSchedulerPeriodicContractRejectsUnsupportedOptions(t *testing.T) {
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.reconcile.v1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := taskqueue.IntervalSchedule(time.Minute, taskqueue.WithLocation("Asia/Shanghai")); !errors.Is(err, taskqueue.ErrInvalidScheduleOption) {
		t.Fatalf("interval location error = %v, want ErrInvalidScheduleOption", err)
	}

	schedule, err := taskqueue.IntervalSchedule(time.Minute)
	if err != nil {
		t.Fatalf("IntervalSchedule: %v", err)
	}
	if _, err := taskqueue.NewPeriodicTask(
		"document.reconcile",
		schedule,
		task,
		taskqueue.WithProcessAt(time.Now().Add(time.Minute)),
	); !errors.Is(err, taskqueue.ErrInvalidPeriodicTaskPolicy) {
		t.Fatalf("process-at error = %v, want ErrInvalidPeriodicTaskPolicy", err)
	}
	if _, err := taskqueue.NewPeriodicTask(
		"document.reconcile",
		schedule,
		task,
		taskqueue.WithDeadline(time.Now().Add(time.Minute)),
	); !errors.Is(err, taskqueue.ErrInvalidPeriodicTaskPolicy) {
		t.Fatalf("deadline error = %v, want ErrInvalidPeriodicTaskPolicy", err)
	}
}

// Intent: Scheduler registration should fail clearly for duplicate semantic
// jobs within one scheduler instance or missing runtime instead of creating
// ambiguous periodic producers.
func TestSchedulerRegisterPeriodicTaskRejectsDuplicateAndNilRuntime(t *testing.T) {
	fake := &fakeSchedulerRuntime{}
	scheduler := newScheduler(fake)
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.reconcile.v1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	schedule, err := taskqueue.IntervalSchedule(time.Minute)
	if err != nil {
		t.Fatalf("IntervalSchedule: %v", err)
	}
	periodic, err := taskqueue.NewPeriodicTask("document.reconcile", schedule, task)
	if err != nil {
		t.Fatalf("NewPeriodicTask: %v", err)
	}

	if err := scheduler.RegisterPeriodicTask(periodic); err != nil {
		t.Fatalf("RegisterPeriodicTask first: %v", err)
	}
	if err := scheduler.RegisterPeriodicTask(periodic); !errors.Is(err, taskqueue.ErrDuplicatePeriodicTask) {
		t.Fatalf("duplicate error = %v, want ErrDuplicatePeriodicTask", err)
	}
	if err := newScheduler(nil).RegisterPeriodicTask(periodic); !errors.Is(err, ErrNilScheduler) {
		t.Fatalf("nil runtime error = %v, want ErrNilScheduler", err)
	}
}

// Intent: Scheduler should expose Start/Shutdown lifecycle so service runtime
// hooks can start periodic producers and stop them during graceful shutdown.
func TestSchedulerLifecycleStartsAndShutsDown(t *testing.T) {
	fake := &fakeSchedulerRuntime{}
	scheduler := newScheduler(fake)

	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := scheduler.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if !fake.started {
		t.Fatal("scheduler runtime was not started")
	}
	if !fake.shutdown {
		t.Fatal("scheduler runtime was not shut down")
	}
}

type fakeSchedulerRegistration struct {
	spec string
	task *hibiken.Task
	opts []hibiken.Option
}

type fakeSchedulerRuntime struct {
	registrations []fakeSchedulerRegistration
	started       bool
	shutdown      bool
	err           error
}

func (f *fakeSchedulerRuntime) Register(spec string, task *hibiken.Task, opts ...hibiken.Option) (string, error) {
	f.registrations = append(f.registrations, fakeSchedulerRegistration{
		spec: spec,
		task: task,
		opts: append([]hibiken.Option(nil), opts...),
	})
	return "entry-1", f.err
}

func (f *fakeSchedulerRuntime) Start() error {
	f.started = true
	return f.err
}

func (f *fakeSchedulerRuntime) Shutdown() {
	f.shutdown = true
}
