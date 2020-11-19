package hello

import (
	"context"
	"sharding/domain/hello"

	"github.com/jmoiron/sqlx"
)

// Repo for hello repository
type Repo struct {
	db *sqlx.DB
}

type txRepo struct {
	tx *sqlx.Tx
}

var _ hello.Repository = &Repo{}

var _ hello.TxRepository = &txRepo{}

// NewRepo creates a Repo
func NewRepo(db *sqlx.DB) *Repo {
	return &Repo{
		db: db,
	}
}

func (r *Repo) Transact(ctx context.Context,
	fn func(ctx context.Context, tx hello.TxRepository) error,
) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	err = fn(ctx, &txRepo{tx: tx})
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (r *txRepo) UpsertCounter(ctx context.Context, id hello.CounterID, value uint32) error {
	query := `
INSERT INTO counter (id, value) VALUE (?, ?) AS NE
ON DUPLICATE KEY UPDATE value = NEW.value
`
	_, err := r.tx.ExecContext(ctx, query, id, value)
	if err != nil {
		return err
	}
	return nil
}
