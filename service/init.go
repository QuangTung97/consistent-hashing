package service

import (
	"context"
	"fmt"
	"sharding/config"
	"sharding/core"
	hello_logic "sharding/domain/hello/logic"
	hello_rpc "sharding/rpc/hello/v1"
	hello_service "sharding/service/hello"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
)

// Root represents the whole app
type Root struct {
	db     *sqlx.DB
	config config.Config

	mutex    sync.RWMutex
	hashDist []core.ConsistentHash
}

// InitRoot creates a Root
func InitRoot(server *grpc.Server) *Root {
	cfg := config.LoadConfig()

	port := hello_logic.NewPort()

	s := hello_service.NewService(port)
	hello_rpc.RegisterHelloServer(server, s)

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")

	return &Root{
		config: cfg,
		db:     db,
	}
}

// Run another processes
func (r *Root) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)

	node := r.config.Node
	core.KeepAlive(ctx, r.db, node.ID, node.Hash, node.ToAddress(), &wg)
	watchChan := core.Watch(ctx, r.db)

	go func() {
		for wr := range watchChan {
			hashes := make([]core.ConsistentHash, 0, len(wr.Hashes))
			for _, h := range wr.Hashes {
				hashes = append(hashes, h)
			}
			fmt.Println(hashes)

			r.mutex.Lock()
			r.hashDist = hashes
			r.mutex.Unlock()
		}
	}()

	wg.Wait()
}

// InitGatewayEndpoints initializes endpoints
func InitGatewayEndpoints(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) {
	ctx := context.Background()

	if err := hello_rpc.RegisterHelloHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
		panic(err)
	}
}
