package impl

import (
	"context"
	"fmt"
	"sharding/core"
	"time"

	"go.etcd.io/etcd/clientv3"
)

// EtcdCoreService ...
type EtcdCoreService struct {
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
		etcdClient: etcdClient,
	}
}

func grant(ctx context.Context, client *clientv3.Client) clientv3.LeaseID {
	for {
		res, err := client.Grant(ctx, 30)
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)
			continue
		}
		return res.ID
	}
}

// KeepAliveAndWatch ...
func (s *EtcdCoreService) KeepAliveAndWatch(ctx context.Context, info core.NodeInfo, ch chan<- core.WatchResponse) {
}

// Watch ...
func (s *EtcdCoreService) Watch(ctx context.Context, ch chan<- core.WatchResponse) {
}
