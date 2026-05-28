package asynq

import "errors"

var (
	ErrNilClient = errors.New("asynq client is nil")
	ErrNilServer = errors.New("asynq server is nil")
	ErrNilTask   = errors.New("asynq task is nil")
)
