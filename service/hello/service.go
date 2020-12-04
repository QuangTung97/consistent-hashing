package hello

import (
	"context"
	domain "sharding/domain/hello"
	rpc "sharding/rpc/hello/v1"
)

// Service for gRPC
type Service struct {
	rpc.UnsafeHelloServer
	port      domain.Port
	closeChan <-chan struct{}
}

var _ rpc.HelloServer = &Service{}

// NewService create a new Service
func NewService(port domain.Port, closeChan <-chan struct{}) *Service {
	return &Service{
		port:      port,
		closeChan: closeChan,
	}
}

// Increase do hello
func (s *Service) Increase(ctx context.Context, req *rpc.IncreaseRequest,
) (*rpc.IncreaseResponse, error) {
	err := s.port.Increase(ctx, domain.CounterID(req.Counter))
	if err != nil {
		return nil, err
	}

	return &rpc.IncreaseResponse{}, nil
}

// Ping for core's watch
func (s *Service) Ping(req *rpc.PingRequest, server rpc.Hello_PingServer) error {
	err := server.Send(&rpc.PingResponse{})
	if err != nil {
		return err
	}

	<-s.closeChan

	return nil
}
