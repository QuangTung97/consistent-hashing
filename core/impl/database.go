package impl

import (
	"context"
	"sharding/core"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type dbNodeInfo struct {
	NodeID  core.NodeID `db:"node_id"`
	Hash    core.Hash   `db:"hash"`
	Address string      `db:"address"`
}

// DBCoreService core service using database
type DBCoreService struct {
	db     *sqlx.DB
	logger *zap.Logger
}

var _ core.Service = &DBCoreService{}

// NewDBCoreService ...
func NewDBCoreService(db *sqlx.DB, logger *zap.Logger) *DBCoreService {
	return &DBCoreService{
		db:     db,
		logger: logger,
	}
}

func (c *DBCoreService) deleteHash(nodeID core.NodeID) {
	query := `DELETE FROM consistent_hash WHERE node_id = ?`
	_, err := c.db.Exec(query, nodeID)
	if err != nil {
		c.logger.Error("Delete from consistent_hash", zap.Error(err))
	}
}

var keepAliveQuery = `
INSERT INTO consistent_hash (node_id, hash, address, expired_at)
VALUE (?, ?, ?, TIMESTAMPADD(SECOND, 60, NOW())) AS NEW
ON DUPLICATE KEY UPDATE
    hash = NEW.hash,
    address = NEW.address,
    expired_at = NEW.expired_at
`

func (c *DBCoreService) insert(ctx context.Context, info dbNodeInfo) {
	_, err := c.db.Exec(keepAliveQuery, info.NodeID, info.Hash, info.Address)
	if err != nil {
		c.logger.Error("Insert into consistent_hash", zap.Error(err))
	}
}

func (c *DBCoreService) keepAlive(ctx context.Context, info dbNodeInfo) {
KeepAliveLoop:
	for {
		_, err := c.db.Exec(keepAliveQuery, info.NodeID, info.Hash, info.Address)
		if err != nil {
			c.logger.Error("Insert into consistent_hash", zap.Error(err))

			select {
			case <-ctx.Done():
				c.deleteHash(info.NodeID)
				c.logger.Info("Deleted consistent_hash", zap.Uint("node.id", uint(info.NodeID)))
				return

			case <-time.After(10 * time.Second):
				continue KeepAliveLoop
			}
		}

		select {
		case <-ctx.Done():
			c.deleteHash(info.NodeID)
			c.logger.Info("Deleted consistent_hash", zap.Uint("node.id", uint(info.NodeID)))
			return

		case <-time.After(2 * time.Second):
			continue KeepAliveLoop
		}
	}
}

func dbNodeInfosToCore(nodes []dbNodeInfo) []core.NodeInfo {
	result := make([]core.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, core.NodeInfo{
			NodeID:  n.NodeID,
			Hash:    n.Hash,
			Address: n.Address,
		})
	}
	return result
}

func (c *DBCoreService) watch(ch chan<- core.WatchResponse) {
	var oldNodes []core.NodeInfo
	for {
		query := `
SELECT node_id, hash, address FROM consistent_hash
WHERE NOW() <= expired_at
`
		var nodes []dbNodeInfo
		err := c.db.Select(&nodes, query)
		if err != nil {
			c.logger.Error("Select from consistent_hash", zap.Error(err))
			time.Sleep(10 * time.Second)
			continue
		}

		newNodes := dbNodeInfosToCore(nodes)
		core.Sort(newNodes)

		if !core.Equals(oldNodes, newNodes) {
			ch <- core.WatchResponse{
				Nodes: newNodes,
			}
		}
		oldNodes = newNodes

		time.Sleep(2 * time.Second)
	}
}

// KeepAliveAndWatch ...
func (c *DBCoreService) KeepAliveAndWatch(ctx context.Context, info core.NodeInfo,
	ch chan<- core.WatchResponse,
) error {
	dbInfo := dbNodeInfo{
		NodeID:  info.NodeID,
		Hash:    info.Hash,
		Address: info.Address,
	}

	c.insert(ctx, dbInfo)

	go c.watch(ch)

	c.keepAlive(ctx, dbInfo)
	return nil
}

// Watch ...
func (c *DBCoreService) Watch(ctx context.Context, ch chan<- core.WatchResponse) error {
	go c.watch(ch)
	return nil
}
