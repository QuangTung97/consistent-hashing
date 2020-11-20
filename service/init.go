package service

import (
	"context"
	"fmt"
	"os"
	"sharding/config"
	"sharding/core"
	"sharding/domain/hello"
	hello_logic "sharding/domain/hello/logic"
	hello_repo "sharding/repo/hello"
	hello_rpc "sharding/rpc/hello/v1"
	hello_service "sharding/service/hello"
	"strconv"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
)

// Root represents the whole app
type Root struct {
	nodeConfig config.NodeConfig
	db         *sqlx.DB
	port       hello.Port
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
func InitRoot(server *grpc.Server) *Root {
	cfg := config.LoadConfig()

	selfNodeID := getSelfNodeID()
	nodeConfig := getSelfNodeConfig(cfg.Nodes, selfNodeID)

	fmt.Println("ID:", nodeConfig.ID)
	fmt.Println("Hash:", nodeConfig.Hash)
	fmt.Println("Address:", nodeConfig.ToAddress())

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")
	repo := hello_repo.NewRepo(db)

	port := hello_logic.NewPort(nodeConfig, repo)

	s := hello_service.NewService(port)
	hello_rpc.RegisterHelloServer(server, s)

	return &Root{
		nodeConfig: nodeConfig,
		db:         db,
		port:       port,
	}
}

// Run other processes
func (r *Root) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	node := r.nodeConfig
	core.KeepAlive(ctx, r.db, node.ID, node.Hash, node.ToAddress(), &wg)
	watchChan := core.Watch(r.db)

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
