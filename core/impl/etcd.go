package impl

import (
	"context"
	"fmt"
	"sharding/core"
	"strconv"
	"strings"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

// EtcdCoreService ...
type EtcdCoreService struct {
	prefix     string
	etcdClient *clientv3.Client
}

var _ core.Service = &EtcdCoreService{}

// NewEtcdCoreService ...
func NewEtcdCoreService() *EtcdCoreService {
	cfg := clientv3.Config{
		Endpoints: []string{
			"localhost:2379",
		},
	}

	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}

	return &EtcdCoreService{
		prefix:     "/sharding/",
		etcdClient: etcdClient,
	}
}

func newSession(ctx context.Context, client *clientv3.Client) (clientv3.LeaseID, error) {
	res, err := client.Grant(ctx, 30)
	if err != nil {
		return 0, err
	}

	ch, err := client.KeepAlive(ctx, res.ID)
	if err != nil || ch == nil {
		return 0, err
	}
	go func() {
		for range ch {
		}
	}()

	return res.ID, nil
}

func nodeInfoToKV(prefix string, info core.NodeInfo) (string, string) {
	key := prefix + fmt.Sprintf("%d", info.NodeID)
	value := fmt.Sprintf("%d/%d/%s", info.NodeID, info.Hash, info.Address)
	return key, value
}

func kvToNodeInfo(prefix string, value []byte) core.NodeInfo {
	s := string(value)
	list := strings.Split(s, "/")
	nodeID, err := strconv.ParseUint(list[0], 10, 32)
	if err != nil {
		panic(err)
	}

	hash, err := strconv.ParseUint(list[1], 10, 32)
	if err != nil {
		panic(err)
	}

	return core.NodeInfo{
		NodeID:  core.NodeID(nodeID),
		Hash:    core.Hash(hash),
		Address: list[2],
	}
}

func handlePut(nodes []core.NodeInfo, prefix string, key []byte, value []byte) []core.NodeInfo {
	nodes = append(nodes, kvToNodeInfo(prefix, value))
	core.Sort(nodes)
	return nodes
}

func handleDelete(input []core.NodeInfo, prefix string, inputKey []byte) []core.NodeInfo {
	key := string(inputKey)
	key = strings.TrimPrefix(key, prefix)

	id, err := strconv.ParseUint(key, 10, 32)
	if err != nil {
		panic(err)
	}
	nodeID := core.NodeID(id)

	nodes := make([]core.NodeInfo, 0, len(input)-1)
	for _, n := range input {
		if n.NodeID == nodeID {
			continue
		}
		nodes = append(nodes, n)
	}

	core.Sort(nodes)
	return nodes
}

func (s *EtcdCoreService) watch(ctx context.Context, ch chan<- core.WatchResponse) error {
	getRes, err := s.etcdClient.Get(ctx, s.prefix,
		clientv3.WithPrefix(),
	)
	if err != nil {
		return err
	}

	var nodes []core.NodeInfo
	for _, kv := range getRes.Kvs {
		nodeInfo := kvToNodeInfo(s.prefix, kv.Value)
		nodes = append(nodes, nodeInfo)
	}
	core.Sort(nodes)
	ch <- core.WatchResponse{
		Nodes: nodes,
	}

	rev := getRes.Header.Revision

	watchChan := s.etcdClient.Watch(ctx, s.prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(rev+1),
	)

	go func() {
		for wr := range watchChan {
			for _, e := range wr.Events {
				if e.Type == mvccpb.PUT {
					nodes = handlePut(nodes, s.prefix, e.Kv.Key, e.Kv.Value)
				} else if e.Type == mvccpb.DELETE {
					nodes = handleDelete(nodes, s.prefix, e.Kv.Key)
				} else {
					panic("Unrecognized type")
				}
			}

			ch <- core.WatchResponse{
				Nodes: nodes,
			}
		}
	}()

	return nil
}

// KeepAliveAndWatch ...
func (s *EtcdCoreService) KeepAliveAndWatch(ctx context.Context, info core.NodeInfo, ch chan<- core.WatchResponse) error {
	leaseID, err := newSession(ctx, s.etcdClient)
	if err != nil {
		return err
	}

	key, value := nodeInfoToKV(s.prefix, info)

	_, err = s.etcdClient.Put(ctx, key, value, clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}

	err = s.watch(ctx, ch)
	if err != nil {
		return err
	}

	<-ctx.Done()

	_, err = s.etcdClient.Revoke(context.Background(), leaseID)
	return err
}

// Watch ...
func (s *EtcdCoreService) Watch(ctx context.Context, ch chan<- core.WatchResponse) error {
	return s.watch(ctx, ch)
}
