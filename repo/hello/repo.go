package hello

import (
	"context"
	"fmt"
	"sharding/domain/hello"
	"strings"

	"github.com/go-sql-driver/mysql"
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

type selectCounter struct {
	ID      hello.CounterID `db:"id"`
	Version uint32          `db:"version"`
	Value   uint32          `db:"value"`
}

// GetAllCounters ...
func (r *Repo) GetAllCounters(ctx context.Context) ([]hello.Counter, error) {
	query := `SELECT id, version, value FROM counter`

	var counters []selectCounter
	err := r.db.SelectContext(ctx, &counters, query)
	if err != nil {
		return nil, err
	}

	result := make([]hello.Counter, 0, len(counters))
	for _, c := range counters {
		result = append(result, hello.Counter{
			ID:      c.ID,
			Version: c.Version,
			Value:   c.Value,
		})
	}

	return result, nil
}

// Transact ...
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

func (r *txRepo) UpsertCounters(ctx context.Context, counters []hello.CounterUpsert) error {
	if len(counters) == 0 {
		return nil
	}

	args := make([]interface{}, 0, 3*len(counters))

	var builder strings.Builder
	_, _ = builder.WriteString("(?, ?, ?)")
	for range counters[1:] {
		builder.WriteString(",(?, ?, ?)")
	}

	for _, c := range counters {
		args = append(args, c.ID)
		args = append(args, c.NewVersion)
		args = append(args, c.Value)
	}

	query := `
INSERT INTO counter (id, version, value)
VALUE %s AS new
ON DUPLICATE KEY UPDATE
    value = new.value,
    version = IF(counter.version = new.version - 1, new.version, NULL)`
	query = fmt.Sprintf(query, builder.String())

	_, err := r.tx.ExecContext(ctx, query, args...)
	if err != nil {
		mysqlErr, ok := err.(*mysql.MySQLError)
		if ok {
			if mysqlErr.Number == 1048 {
				return hello.ErrCommandAborted
			}
		}
		return err
	}
	return nil
}
