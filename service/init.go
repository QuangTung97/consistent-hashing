package service

import (
	"context"
	"sharding/core"
	hello_logic "sharding/domain/hello/logic"
	hello_rpc "sharding/rpc/hello/v1"
	hello_service "sharding/service/hello"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
)

// Root represents the whole app
type Root struct {
	db *sqlx.DB
}

// InitRoot creates a Root
func InitRoot(server *grpc.Server) *Root {
	port := hello_logic.NewPort()

	s := hello_service.NewService(port)
	hello_rpc.RegisterHelloServer(server, s)

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")

	return &Root{
		db: db,
	}
}

// Run another processes
func (r *Root) Run(ctx context.Context) {
	core.KeepAlive(ctx, r.db, 1, 100)
}

// InitGatewayEndpoints initializes endpoints
func InitGatewayEndpoints(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) {
	ctx := context.Background()

	if err := hello_rpc.RegisterHelloHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
		panic(err)
	}
}
