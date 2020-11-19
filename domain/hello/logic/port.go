package logic

import (
	"context"
	"errors"
	"fmt"
	"sharding/core"
	"sharding/domain/hello"
	"time"
)

type Port struct {
	processor   *processor
	commandChan chan<- command
}

var _ hello.Port = &Port{}

func NewPort(repo hello.Repository) *Port {
	cmdChan := make(chan command, maxBatchSize*2)
	return &Port{
		processor:   newProcessor(repo, cmdChan),
		commandChan: cmdChan,
	}
}

func (p *Port) Increase(ctx context.Context, id hello.CounterID) error {
	replyChan := make(chan event, 1)

	p.commandChan <- commandInc{
		counterID: id,
		replyChan: replyChan,
	}

	select {
	case e, more := <-replyChan:
		if !more {
			return errors.New("Aborted")
		}
		ev := e.(eventInc)
		return ev.err

	case <-time.After(10 * time.Second):
		return errors.New("Timeout")
	}
}

func (p *Port) Process(ctx context.Context, watchChan <-chan core.WatchResponse) {
	for {
		err := p.processor.process(ctx, watchChan)
		if err != nil {
			fmt.Println(err)
			continue
		}
		return
	}
}
