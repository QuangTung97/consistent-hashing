package hello

import (
	"context"
	domain "sharding/domain/hello"
	rpc "sharding/rpc/hello/v1"
)

// Service for gRPC
type Service struct {
	rpc.UnsafeHelloServer
	port domain.IPort
}

var _ rpc.HelloServer = &Service{}

// NewService create a new Service
func NewService(port domain.IPort) *Service {
	return &Service{
		port: port,
	}
}

// Hello do hello
func (s *Service) Hello(ctx context.Context, req *rpc.HelloRequest,
) (*rpc.HelloResponse, error) {
	return &rpc.HelloResponse{}, nil
}
