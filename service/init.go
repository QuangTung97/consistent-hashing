package service

import (
	"context"
	"fmt"
	"os"
	"sharding/config"
	"sharding/core"
	"sharding/core/impl"
	"sharding/domain/hello"
	hello_logic "sharding/domain/hello/logic"
	hello_repo "sharding/repo/hello"
	hello_rpc "sharding/rpc/hello/v1"
	hello_service "sharding/service/hello"
	"strconv"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Root represents the whole app
type Root struct {
	nodeConfig config.NodeConfig
	core       core.Service
	port       hello.Port
	closeChan  chan<- struct{}
}

func getSelfNodeID() core.NodeID {
	if len(os.Args) <= 1 {
		panic("node id is required")
	}
	n, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		panic(err)
	}
	return core.NodeID(n)
}

func getSelfNodeConfig(nodes []config.NodeConfig, nodeID core.NodeID) config.NodeConfig {
	for _, n := range nodes {
		if n.ID == nodeID {
			return n
		}
	}
	panic("node id not existed")
}

// InitRoot creates a Root
func InitRoot(server *grpc.Server, logger *zap.Logger) *Root {
	cfg := config.LoadConfig()

	selfNodeID := getSelfNodeID()
	nodeConfig := getSelfNodeConfig(cfg.Nodes, selfNodeID)

	fmt.Println("ID:", nodeConfig.ID)
	fmt.Println("Hash:", nodeConfig.Hash)
	fmt.Println("Address:", nodeConfig.ToAddress())

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")

	core := impl.NewPeerCoreService(
		cfg.Nodes,
		core.NullNodeID{Valid: true, NodeID: selfNodeID},
		logger,
	)
	repo := hello_repo.NewRepo(db)

	port := hello_logic.NewPort(nodeConfig, repo)

	closeChan := make(chan struct{})

	s := hello_service.NewService(port, closeChan)
	hello_rpc.RegisterHelloServer(server, s)

	return &Root{
		nodeConfig: nodeConfig,
		core:       core,
		port:       port,
		closeChan:  closeChan,
	}
}

// Run other processes
func (r *Root) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	node := r.nodeConfig
	info := core.NodeInfo{
		NodeID:  node.ID,
		Hash:    node.Hash,
		Address: node.ToAddress(),
	}

	go func() {
		defer wg.Done()
		r.core.KeepAlive(ctx, info)
		close(r.closeChan)
	}()

	time.Sleep(100 * time.Millisecond)
	watchChan := r.core.Watch(ctx)

	go func() {
		defer wg.Done()
		r.port.Process(ctx, watchChan)
		fmt.Println("Shutdown processor")
	}()

	wg.Wait()
}

// GetNodeConfig ...
func (r *Root) GetNodeConfig() config.NodeConfig {
	return r.nodeConfig
}

// InitGatewayEndpoints initializes endpoints
func InitGatewayEndpoints(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) {
	ctx := context.Background()

	if err := hello_rpc.RegisterHelloHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
		panic(err)
	}
}
