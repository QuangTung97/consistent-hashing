package hello

import (
	"context"
	"sharding/core"
	"sharding/domain/errors"
)

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
		Increase(ctx context.Context, id CounterID) error
		Process(ctx context.Context, watchChan <-chan core.WatchResponse)
	}
)

var (
	ErrCommandAborted = errors.New("10000", "Command aborted")
	ErrCommandTimeout = errors.New("10001", "Command timeout")
)
