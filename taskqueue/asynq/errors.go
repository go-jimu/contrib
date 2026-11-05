package asynq

import "errors"

// Errors returned by this package for components built around nil asynq
// values. Compare with errors.Is.
var (
	// ErrNilClient means the Client wraps a nil *asynq.Client.
	ErrNilClient = errors.New("asynq client is nil")
	// ErrNilWorker means the Worker wraps a nil *asynq.Server.
	ErrNilWorker = errors.New("asynq worker is nil")
	// ErrNilTask means ProcessTask received a nil *asynq.Task.
	ErrNilTask = errors.New("asynq task is nil")
	// ErrNilScheduler means the Scheduler wraps a nil *asynq.Scheduler.
	ErrNilScheduler = errors.New("asynq scheduler is nil")
)
