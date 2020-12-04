package hello

import (
	"context"
	"sharding/core"
	"sharding/domain/errors"
)

type (
	// CounterID for counter id
	CounterID uint32
)

type (
	// Repository interface for db
	Repository interface {
		Transact(ctx context.Context, fn func(ctx context.Context, tx TxRepository) error) error
	}

	// TxRepository interface for transactions
	TxRepository interface {
		UpsertCounter(ctx context.Context, id CounterID, value uint32) error
	}

	// Port interface for core logic
	Port interface {
		Increase(ctx context.Context, id CounterID) error
		Process(ctx context.Context, watchChan <-chan core.WatchResponse)
	}
)

var (
	// ErrCommandAborted ...
	ErrCommandAborted = errors.New("10000", "Command aborted")

	// ErrCommandTimeout ...
	ErrCommandTimeout = errors.New("10001", "Command timeout")

	// ErrServiceUnavailable ...
	ErrServiceUnavailable = errors.New("14001", "Service unavailable")
)
