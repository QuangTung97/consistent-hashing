package hello

import (
	"context"
	domain "sharding/domain/hello"
	rpc "sharding/rpc/hello/v1"
)

// Service for gRPC
type Service struct {
	rpc.UnsafeHelloServer
	port domain.Port
}

var _ rpc.HelloServer = &Service{}

// NewService create a new Service
func NewService(port domain.Port) *Service {
	return &Service{
		port: port,
	}
}

// Hello do hello
func (s *Service) Hello(ctx context.Context, req *rpc.HelloRequest,
) (*rpc.HelloResponse, error) {

	err := s.port.Increase(ctx, 100)
	if err != nil {
		return nil, err
	}

	return &rpc.HelloResponse{}, nil
}
