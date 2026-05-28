package asynq

import "errors"

var (
	ErrNilClient = errors.New("asynq client is nil")
	ErrNilWorker = errors.New("asynq worker is nil")
	ErrNilTask   = errors.New("asynq task is nil")
)
