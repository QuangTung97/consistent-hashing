package service

import (
	"context"
	"fmt"
	"sharding/config"
	"sharding/core"
	hello_rpc "sharding/rpc/hello/v1"
	"sharding/service/hello"
	hello_service "sharding/service/hello"

	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
)

// ProxyRoot for proxy
type ProxyRoot struct {
	proxyConfig config.ProxyConfig
	db          *sqlx.DB
	service     *hello.ProxyService
}

// InitProxyRoot creates a Root
func InitProxyRoot(server *grpc.Server) *ProxyRoot {
	cfg := config.LoadConfig()

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")
	s := hello_service.NewProxyService(db)

	hello_rpc.RegisterHelloServer(server, s)

	return &ProxyRoot{
		proxyConfig: cfg.Proxy,
		db:          db,
		service:     s,
	}
}

// Run ...
func (r *ProxyRoot) Run(ctx context.Context) {
	watchChan := core.Watch(r.db)
	for wr := range watchChan {
		r.service.Watch(wr.Hashes)
	}
}

// GetGRPCAddress ...
func (r *ProxyRoot) GetGRPCAddress() string {
	return fmt.Sprintf("localhost:%d", r.proxyConfig.Port)
}

// GetGPRCListenAddr ...
func (r *ProxyRoot) GetGPRCListenAddr() string {
	return fmt.Sprintf(":%d", r.proxyConfig.Port)
}

// GetGPRCGatewayListenAddr ...
func (r *ProxyRoot) GetGPRCGatewayListenAddr() string {
	return fmt.Sprintf(":%d", r.proxyConfig.Port+100)
}
