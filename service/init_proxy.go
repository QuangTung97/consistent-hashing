package service

import (
	"context"
	"fmt"
	"sharding/config"
	"sharding/core"
	"sharding/core/impl"
	hello_rpc "sharding/rpc/hello/v1"
	"sharding/service/hello"
	hello_service "sharding/service/hello"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ProxyRoot for proxy
type ProxyRoot struct {
	proxyConfig config.ProxyConfig
	core        core.Service
	service     *hello.ProxyService
}

// InitProxyRoot creates a Root
func InitProxyRoot(server *grpc.Server, logger *zap.Logger) *ProxyRoot {
	cfg := config.LoadConfig()

	// db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")
	coreService := impl.NewPeerCoreService(cfg.Nodes, core.NullNodeID{}, logger)

	s := hello_service.NewProxyService()

	hello_rpc.RegisterHelloServer(server, s)

	return &ProxyRoot{
		proxyConfig: cfg.Proxy,
		core:        coreService,
		service:     s,
	}
}

// Run ...
func (r *ProxyRoot) Run(ctx context.Context) {
	watchChan := r.core.Watch(ctx)
	for wr := range watchChan {
		r.service.Watch(wr.Nodes)
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
