package hello

import (
	"context"
	"sharding/core"
	"sharding/domain/errors"
)

type (
	// CounterID for counter id
	CounterID uint32

	// Counter model from db
	Counter struct {
		ID      CounterID
		Version uint32
		Value   uint32
	}

	// CounterUpsert for upserting
	CounterUpsert struct {
		ID         CounterID
		NewVersion uint32
		Value      uint32
	}
)

type (
	// Repository interface for db
	Repository interface {
		GetAllCounters(ctx context.Context) ([]Counter, error)

		Transact(ctx context.Context, fn func(ctx context.Context, tx TxRepository) error) error
	}

	// TxRepository interface for transactions
	TxRepository interface {
		UpsertCounters(ctx context.Context, counters []CounterUpsert) error
	}

	// Port interface for core logic
	Port interface {
		// Increase for increasing counter
		Increase(ctx context.Context, id CounterID) error
		// Process process in background
		Process(ctx context.Context, watchChan <-chan core.WatchResponse) error
	}
)

var (
	// ErrCommandAborted ...
	ErrCommandAborted = errors.New("10000", "Command aborted")

	// ErrClientAborted ...
	ErrClientAborted = errors.New("10002", "Client aborted")

	// ErrCommandTimeout ...
	ErrCommandTimeout = errors.New("10001", "Command timeout")

	// ErrServiceUnavailable ...
	ErrServiceUnavailable = errors.New("14001", "Service unavailable")

	// ErrInternal ...
	ErrInternal = errors.New("13001", "Internal server error")
)
