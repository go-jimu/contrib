package asynq

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type ServeMux struct {
	router *taskqueue.Router
}

var (
	_ taskqueue.Registrar = (*ServeMux)(nil)
	_ hibiken.Handler     = (*ServeMux)(nil)
)

func NewServeMux() *ServeMux {
	return &ServeMux{router: taskqueue.NewRouter()}
}

func (m *ServeMux) Register(processor taskqueue.Processor) error {
	return m.router.Register(processor)
}

func (m *ServeMux) ProcessTask(ctx context.Context, task *hibiken.Task) error {
	componentTask, err := taskFromAsynq(ctx, task)
	if err != nil {
		return err
	}
	err = m.router.Process(ctx, componentTask)
	if err == nil {
		return nil
	}
	if errors.Is(err, taskqueue.ErrSkipRetry) {
		return fmt.Errorf("%w: %w", hibiken.SkipRetry, err)
	}
	return err
}
