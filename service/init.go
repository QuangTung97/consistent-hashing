package service

import (
	"context"
	"fmt"
	"sharding/config"
	"sharding/core"
	"sharding/domain/hello"
	hello_logic "sharding/domain/hello/logic"
	hello_repo "sharding/repo/hello"
	hello_rpc "sharding/rpc/hello/v1"
	hello_service "sharding/service/hello"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
)

// Root represents the whole app
type Root struct {
	config config.Config
	db     *sqlx.DB
	port   hello.Port
}

// InitRoot creates a Root
func InitRoot(server *grpc.Server) *Root {
	cfg := config.LoadConfig()

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")
	repo := hello_repo.NewRepo(db)

	port := hello_logic.NewPort(repo)

	s := hello_service.NewService(port)
	hello_rpc.RegisterHelloServer(server, s)

	return &Root{
		config: cfg,
		db:     db,
		port:   port,
	}
}

// Run other processes
func (r *Root) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	node := r.config.Node
	core.KeepAlive(ctx, r.db, node.ID, node.Hash, node.ToAddress(), &wg)
	watchChan := core.Watch(r.db)

	go func() {
		defer wg.Done()
		r.port.Process(ctx, watchChan)
		fmt.Println("Shutdown processor")
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
