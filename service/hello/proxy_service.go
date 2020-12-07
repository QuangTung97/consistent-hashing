package hello

import (
	"context"
	"fmt"
	"sharding/core"
	"sharding/domain/hello"
	rpc "sharding/rpc/hello/v1"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProxyService for proxy gRPC
type ProxyService struct {
	rpc.UnimplementedHelloServer
	logger *zap.Logger

	mut     sync.RWMutex
	nodes   []core.NodeInfo
	connMap map[string]*grpc.ClientConn
}

var _ rpc.HelloServer = &ProxyService{}

// NewProxyService create a new ProxyService
func NewProxyService() *ProxyService {
	return &ProxyService{
		connMap: make(map[string]*grpc.ClientConn),
	}
}

// Increase do hello
func (s *ProxyService) Increase(ctx context.Context, req *rpc.IncreaseRequest,
) (*rpc.IncreaseResponse, error) {
	hash := core.HashUint32(req.Counter)
	var res *rpc.IncreaseResponse

	err := s.call(ctx, hash, func(ctx context.Context, conn *grpc.ClientConn) error {
		client := rpc.NewHelloClient(conn)

		var err error
		res, err = client.Increase(ctx, req)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Ping for core's watch
func (s *ProxyService) Ping(req *rpc.PingRequest, server rpc.Hello_PingServer) error {
	return nil
}

// Watch for node infos
func (s *ProxyService) Watch(newNodes []core.NodeInfo) {
	fmt.Println(newNodes)

	copyConnMap := make(map[string]*grpc.ClientConn)
	s.mut.RLock()
	for key, val := range s.connMap {
		copyConnMap[key] = val
	}
	oldNodes := s.nodes
	s.mut.RUnlock()

	diff := core.ComputeAddressesDifference(oldNodes, newNodes)

	for _, deleted := range diff.Deleted {
		delete(copyConnMap, deleted)
	}

	for _, addr := range diff.Inserted {
		connectParams := grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  5 * time.Second,
				Multiplier: 1.0,
				Jitter:     0.3,
				MaxDelay:   10 * time.Second,
			},
		}

		conn, err := grpc.Dial(addr,
			grpc.WithConnectParams(connectParams),
			grpc.WithInsecure(),
		)
		if err != nil {
			s.logger.Error("Dial", zap.Error(err))
			continue
		}

		copyConnMap[addr] = conn
	}

	s.mut.Lock()
	s.nodes = newNodes
	s.connMap = copyConnMap
	s.mut.Unlock()
}

func (s *ProxyService) call(ctx context.Context, hash core.Hash,
	fn func(ctx context.Context, conn *grpc.ClientConn) error,
) error {
	retryCount := 0
	for {
		s.mut.RLock()
		serviceNodes := s.nodes
		connMap := s.connMap
		s.mut.RUnlock()

		nullAddress := core.GetNodeAddress(serviceNodes, hash)
		if !nullAddress.Valid {
			retryCount++
			if retryCount > 3 {
				return hello.ErrServiceUnavailable
			}

			fmt.Println("Null Address:", retryCount)

			time.Sleep(time.Duration(retryCount) * 5 * time.Second)
			continue
		}

		addr := nullAddress.Address
		conn, ok := connMap[addr]
		if !ok {
			retryCount++
			if retryCount > 3 {
				return hello.ErrServiceUnavailable
			}

			fmt.Println("No conn")

			time.Sleep(time.Duration(retryCount) * 5 * time.Second)
			continue
		}

		err := fn(ctx, conn)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok {
				return err
			}

			if st.Code() == codes.Aborted || st.Code() == codes.Unavailable {
				retryCount++
				if retryCount > 3 {
					return hello.ErrServiceUnavailable
				}

				fmt.Println("Aborted:", retryCount)

				time.Sleep(time.Duration(retryCount) * 5 * time.Second)
				continue
			}

			return err
		}

		return nil
	}
}
