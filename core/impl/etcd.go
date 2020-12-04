package impl

import (
	"context"
	"sharding/core"
)

// EtcdCoreService ...
type EtcdCoreService struct {
}

var _ core.Service = &EtcdCoreService{}

// NewEtcdCoreService ...
func NewEtcdCoreService() *EtcdCoreService {
	return &EtcdCoreService{}
}

// KeepAlive ...
func (s *EtcdCoreService) KeepAlive(ctx context.Context, info core.NodeInfo) {
}

// Watch ...
func (s *EtcdCoreService) Watch(ctx context.Context) <-chan core.WatchResponse {
	return nil
}
