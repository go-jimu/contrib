package asynq

import (
	"context"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type serverClient interface {
	Start(hibiken.Handler) error
	Shutdown()
}

type Server struct {
	server serverClient
	mux    *ServeMux
}

var (
	_ taskqueue.Registrar = (*Server)(nil)
	_ taskqueue.Runner    = (*Server)(nil)
	_ hibiken.Handler     = (*Server)(nil)
)

func NewServer(server *hibiken.Server) *Server {
	var runner serverClient
	if server != nil {
		runner = server
	}
	return newServer(runner, NewServeMux())
}

func NewRedisServer(conn hibiken.RedisConnOpt, cfg hibiken.Config) *Server {
	return NewServer(hibiken.NewServer(conn, cfg))
}

func newServer(server serverClient, mux *ServeMux) *Server {
	if mux == nil {
		mux = NewServeMux()
	}
	return &Server{
		server: server,
		mux:    mux,
	}
}

func (s *Server) Register(processor taskqueue.Processor) error {
	return s.mux.Register(processor)
}

func (s *Server) ProcessTask(ctx context.Context, task *hibiken.Task) error {
	return s.mux.ProcessTask(ctx, task)
}

func (s *Server) Run(ctx context.Context) error {
	if s.server == nil {
		return ErrNilServer
	}
	if err := s.server.Start(s.mux); err != nil {
		return err
	}
	<-ctx.Done()
	s.server.Shutdown()
	return ctx.Err()
}
