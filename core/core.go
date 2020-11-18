package core

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// ConsistentHash a node in consistent hashing
type ConsistentHash struct {
	NodeID uint32 `db:"node_id"`
	Hash   uint32 `db:"hash"`
}

func deleteHash(db *sqlx.DB, nodeID uint32) {
	query := `DELETE FROM consistent_hash WHERE node_id = ?`
	_, err := db.Exec(query, nodeID)
	if err != nil {
		fmt.Println(err)
	}
}

var keepAliveQuery = `
INSERT INTO consistent_hash (node_id, hash, expired_at)
VALUE (?, ?, TIMESTAMPADD(SECOND, 60, NOW())) AS NEW
ON DUPLICATE KEY UPDATE hash = NEW.hash, expired_at = NEW.expired_at
`

// KeepAlive keeps the current node alive
func KeepAlive(ctx context.Context, db *sqlx.DB, nodeID uint32, hash uint32) {
KeepAliveLoop:
	for {
		_, err := db.Exec(keepAliveQuery, nodeID, hash)
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)

			select {
			case <-ctx.Done():
				deleteHash(db, nodeID)
				return

			case <-time.After(10 * time.Second):
				continue KeepAliveLoop
			}
		}

		select {
		case <-ctx.Done():
			deleteHash(db, nodeID)
			return

		case <-time.After(2 * time.Second):
			continue KeepAliveLoop
		}
	}
}

// WatchResponse for each watch response
type WatchResponse struct {
	Hashes []ConsistentHash
}

func watch(ctx context.Context, db *sqlx.DB, ch chan<- WatchResponse) {
WatchLoop:
	for {
		query := `
SELECT node_id, hash FROM consistent_hash
WHERE NOW() <= expired_at
`
		var hashes []ConsistentHash
		err := db.Select(&hashes, query)
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)

			select {
			case <-ctx.Done():
				close(ch)
				return
			case <-time.After(10 * time.Second):
				continue WatchLoop
			}
		}

		ch <- WatchResponse{
			Hashes: hashes,
		}
		select {
		case <-ctx.Done():
			close(ch)
			return
		case <-time.After(2 * time.Second):
			continue WatchLoop
		}
	}
}

// Watch get the consistent hashing configure
func Watch(ctx context.Context, db *sqlx.DB) <-chan WatchResponse {
	ch := make(chan WatchResponse)
	go watch(ctx, db, ch)
	return ch
}
