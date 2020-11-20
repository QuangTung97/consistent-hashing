package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

// NodeID for node id
type NodeID uint32

// Hash for hash value
type Hash uint32

// ConsistentHash a node in consistent hashing
type ConsistentHash struct {
	NodeID  NodeID `db:"node_id"`
	Hash    Hash   `db:"hash"`
	Address string `db:"address"`
}

func deleteHash(db *sqlx.DB, nodeID NodeID) {
	query := `DELETE FROM consistent_hash WHERE node_id = ?`
	_, err := db.Exec(query, nodeID)
	if err != nil {
		fmt.Println(err)
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

func keepAlive(ctx context.Context, db *sqlx.DB, nodeID NodeID, hash Hash, address string) {
KeepAliveLoop:
	for {
		_, err := db.Exec(keepAliveQuery, nodeID, hash, address)
		if err != nil {
			fmt.Println(err)

			select {
			case <-ctx.Done():
				deleteHash(db, nodeID)
				fmt.Println("Deleted consistent hash")
				return

			case <-time.After(10 * time.Second):
				continue KeepAliveLoop
			}
		}

		select {
		case <-ctx.Done():
			deleteHash(db, nodeID)
			fmt.Println("Deleted consistent hash")
			return

		case <-time.After(2 * time.Second):
			continue KeepAliveLoop
		}
	}
}

// KeepAlive keeps the current node alive
func KeepAlive(ctx context.Context, db *sqlx.DB,
	nodeID NodeID, hash Hash, address string, wg *sync.WaitGroup,
) {
	_, err := db.Exec(keepAliveQuery, nodeID, hash, address)
	if err != nil {
		panic(err)
	}

	go func() {
		defer wg.Done()
		keepAlive(ctx, db, nodeID, hash, address)

	}()
}

// WatchResponse for each watch response
type WatchResponse struct {
	Hashes []ConsistentHash
}

func watch(db *sqlx.DB, ch chan<- WatchResponse) {
	for {
		query := `
SELECT node_id, hash, address FROM consistent_hash
WHERE NOW() <= expired_at
`
		var hashes []ConsistentHash
		err := db.Select(&hashes, query)
		if err != nil {
			fmt.Println(err)

			time.Sleep(10 * time.Second)
			continue
		}

		ch <- WatchResponse{
			Hashes: hashes,
		}
		time.Sleep(2 * time.Second)
	}
}

// Watch get the consistent hashing configure
func Watch(db *sqlx.DB) <-chan WatchResponse {
	ch := make(chan WatchResponse)
	go watch(db, ch)
	return ch
}
