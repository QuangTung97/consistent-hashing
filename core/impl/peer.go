package impl

import (
	"context"
	"sharding/config"
	"sharding/core"
	"time"

	rpc "sharding/rpc/hello/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type nodeConn struct {
	node core.NodeInfo
	conn *grpc.ClientConn
}

// PeerCoreService ...
type PeerCoreService struct {
	connMap map[core.NodeID]nodeConn
	selfID  core.NullNodeID
	logger  *zap.Logger
}

var _ core.Service = &PeerCoreService{}

// NewPeerCoreService ...
func NewPeerCoreService(nodeConfigs []config.NodeConfig, selfID core.NullNodeID, logger *zap.Logger) *PeerCoreService {
	connMap := make(map[core.NodeID]nodeConn)
	for _, n := range nodeConfigs {
		connectParams := grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  5 * time.Second,
				Multiplier: 1.0,
				Jitter:     0.3,
				MaxDelay:   10 * time.Second,
			},
		}

		conn, err := grpc.Dial(n.ToAddress(),
			grpc.WithConnectParams(connectParams),
			grpc.WithInsecure(),
		)
		if err != nil {
			panic(err)
		}

		connMap[n.ID] = nodeConn{
			node: core.NodeInfo{
				NodeID:  n.ID,
				Hash:    n.Hash,
				Address: n.ToAddress(),
			},
			conn: conn,
		}
	}

	return &PeerCoreService{
		connMap: connMap,
		selfID:  selfID,
		logger:  logger,
	}
}

type actionType int

const (
	actionTypeInsert actionType = 1
	actionTypeDelete actionType = 2
)

type nodeAction struct {
	node   core.NodeInfo
	action actionType
}

func (c *PeerCoreService) watch(ctx context.Context, ch chan<- core.WatchResponse) {
	actionChan := make(chan nodeAction, 1)

	nodeMap := make(map[core.NodeID]core.NodeInfo)
	for _, n := range c.connMap {
		node := n

		if c.selfID.Valid && c.selfID.NodeID == node.node.NodeID {
			actionChan <- nodeAction{
				action: actionTypeInsert,
				node:   node.node,
			}
			continue
		}

		go func() {
			client := rpc.NewHelloClient(node.conn)

			for {
				stream, err := client.Ping(ctx, &rpc.PingRequest{})
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}

				_, err = stream.Recv()
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}

				actionChan <- nodeAction{
					action: actionTypeInsert,
					node:   node.node,
				}

				_, _ = stream.Recv()

				actionChan <- nodeAction{
					action: actionTypeDelete,
					node:   node.node,
				}

				time.Sleep(2 * time.Second)
			}
		}()
	}

	go func() {
		for a := range actionChan {
			if a.action == actionTypeInsert {
				nodeMap[a.node.NodeID] = a.node
			} else if a.action == actionTypeDelete {
				delete(nodeMap, a.node.NodeID)
			}

			nodes := make([]core.NodeInfo, 0, len(nodeMap))
			for _, n := range nodeMap {
				nodes = append(nodes, n)
			}
			core.Sort(nodes)
			ch <- core.WatchResponse{
				Nodes: nodes,
			}
		}
	}()
}

// KeepAliveAndWatch ...
func (c *PeerCoreService) KeepAliveAndWatch(
	ctx context.Context, info core.NodeInfo, ch chan<- core.WatchResponse,
) {
	c.watch(ctx, ch)
	<-ctx.Done()
}

// Watch ..
func (c *PeerCoreService) Watch(ctx context.Context, ch chan<- core.WatchResponse) {
	c.watch(ctx, ch)
}
