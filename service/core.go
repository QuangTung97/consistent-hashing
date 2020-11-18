package service

import (
	"context"
	hello_logic "sharding/domain/hello/logic"
	hello_rpc "sharding/rpc/hello/v1"
	hello_service "sharding/service/hello"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

func InitServices(server *grpc.Server) {
	port := hello_logic.NewPort()

	s := hello_service.NewService(port)
	hello_rpc.RegisterHelloServer(server, s)
}

func InitGatewayEndpoints(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) {
	ctx := context.Background()

	if err := hello_rpc.RegisterHelloHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
		panic(err)
	}
}
