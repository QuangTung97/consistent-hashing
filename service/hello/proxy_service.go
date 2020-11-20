package hello

import (
	"context"
	"fmt"
	"sharding/core"
	"sharding/domain/hello"
	rpc "sharding/rpc/hello/v1"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Connection struct {
	mut        sync.RWMutex
	clientConn *grpc.ClientConn
}

// ProxyService for proxy gRPC
type ProxyService struct {
	rpc.UnsafeHelloServer

	db *sqlx.DB

	mut    sync.RWMutex
	hashes []core.ConsistentHash

	connMapMut sync.RWMutex
	connMap    map[string]*Connection
}

var _ rpc.HelloServer = &ProxyService{}

// NewProxyService create a new ProxyService
func NewProxyService(db *sqlx.DB) *ProxyService {
	return &ProxyService{
		db:      db,
		connMap: make(map[string]*Connection),
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

// Watch for hashes
func (s *ProxyService) Watch(inputHashes []core.ConsistentHash) {
	hashes := make([]core.ConsistentHash, len(inputHashes))
	copy(hashes, inputHashes)

	core.Sort(hashes)

	s.mut.RLock()
	serviceHashes := s.hashes
	s.mut.RUnlock()

	if !core.Equals(hashes, serviceHashes) {
		fmt.Println(hashes)

		s.mut.Lock()
		s.hashes = hashes
		s.mut.Unlock()
	}
}

func (s *ProxyService) call(ctx context.Context, hash core.Hash,
	fn func(ctx context.Context, conn *grpc.ClientConn) error,
) error {
	retryCount := 0
	for {
		s.mut.RLock()
		serviceHashes := s.hashes
		s.mut.RUnlock()

		nullAddress := core.GetNodeAddress(serviceHashes, hash)
		if !nullAddress.Valid {
			retryCount++
			if retryCount > 3 {
				return hello.ErrServiceUnavailable
			}
			time.Sleep(time.Duration(retryCount) * 5 * time.Second)
			continue
		}

		addr := nullAddress.Address
		s.connMapMut.RLock()
		conn, existed := s.connMap[addr]
		s.connMapMut.RUnlock()

		if !existed {
			conn = &Connection{}

			s.connMapMut.Lock()
			s.connMap[addr] = conn
			s.connMapMut.Unlock()

			var err error
			conn.mut.Lock()
			conn.clientConn, err = grpc.Dial(addr, grpc.WithInsecure())
			conn.mut.Unlock()

			if err != nil {
				s.connMapMut.Lock()
				delete(s.connMap, addr)
				s.connMapMut.Unlock()
				return err
			}
		}

		conn.mut.RLock()
		clientConn := conn.clientConn
		conn.mut.RUnlock()

		err := fn(ctx, clientConn)
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
				time.Sleep(time.Duration(retryCount) * 5 * time.Second)
				continue
			}
		}

		return nil
	}
}
