package hello

import "context"

type (
	CounterID uint32
)

type (
	Repository interface {
		Transact(ctx context.Context, fn func(ctx context.Context, tx TxRepository) error) error
	}

	TxRepository interface {
		UpsertCounter(ctx context.Context, id CounterID, value uint32) error
	}

	Port interface {
	}
)
