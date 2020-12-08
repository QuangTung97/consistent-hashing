package logic

import (
	"context"
	"fmt"
	"sharding/core"
	"sharding/domain/hello"
)

type eventType uint32
type commandType uint32

const (
	commandTypeInc commandType = 1
)

const (
	eventTypeInc eventType = 1
)

type command interface {
	Type() commandType
}

type event interface {
	Type() eventType
	SetError(err error) event
}

// COMMANDS

type commandInc struct {
	counterID hello.CounterID
	replyChan chan<- event
}

var _ command = commandInc{}

func (c commandInc) Type() commandType {
	return commandTypeInc
}

// EVENTS

type eventInc struct {
	err error
}

var _ event = eventInc{}

func (e eventInc) Type() eventType {
	return eventTypeInc
}

func (e eventInc) SetError(err error) event {
	return eventInc{
		err: err,
	}
}

// PROCESSOR

const maxBatchSize = 5000

type processor struct {
	repo       hello.Repository
	cmdChan    <-chan command
	counterMap map[hello.CounterID]hello.Counter

	nodes      []core.NodeInfo
	selfNodeID core.NodeID
}

func newProcessor(selfNodeID core.NodeID, repo hello.Repository, cmdChan <-chan command) *processor {
	return &processor{
		repo:       repo,
		cmdChan:    cmdChan,
		counterMap: make(map[hello.CounterID]hello.Counter),
		selfNodeID: selfNodeID,
	}
}

func (p *processor) process(ctx context.Context, watchChan <-chan core.WatchResponse) error {
	cmds := make([]command, 0, maxBatchSize)
	for {
		select {
		case first := <-p.cmdChan:
			cmds = append(cmds, first)

		case wr := <-watchChan:
			err := p.handleWatch(wr.Nodes)
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}

	BatchLoop:
		for len(cmds) < maxBatchSize {
			select {
			case c, more := <-p.cmdChan:
				if !more {
					return nil
				}
				cmds = append(cmds, c)

			case wr := <-watchChan:
				err := p.handleWatch(wr.Nodes)
				if err != nil {
					return err
				}

			case <-ctx.Done():
				return ctx.Err()

			default:
				break BatchLoop
			}
		}

		err := p.processCommands(cmds)
		if err != nil {
			return err
		}

		for i := range cmds {
			cmds[i] = nil
		}
		cmds = cmds[:0]
	}

}

type replyEvent struct {
	event     event
	replyChan chan<- event
}

type counterUpdate struct {
	oldVersion uint32
	value      uint32
}

type processResponse struct {
	updates     map[hello.CounterID]counterUpdate
	replyEvents []replyEvent
}

func processCommandsPure(
	nodes []core.NodeInfo, selfNodeID core.NodeID,
	counterMap map[hello.CounterID]hello.Counter, commands []command,
) processResponse {
	updates := make(map[hello.CounterID]counterUpdate)
	replyEvents := make([]replyEvent, 0, len(commands))

	for _, cmd := range commands {
		switch cmd.Type() {
		case commandTypeInc:
			cmdInc := cmd.(commandInc)

			hash := hashCounterID(cmdInc.counterID)
			fmt.Println("CounterID:", cmdInc.counterID, "hash:", hash)

			nullNodeID := core.GetNodeID(nodes, hash)
			if !nullNodeID.Valid || nullNodeID.NodeID != selfNodeID {
				replyEvents = append(replyEvents, replyEvent{
					replyChan: cmdInc.replyChan,
					event:     eventInc{err: hello.ErrCommandAborted},
				})
				break
			}

			oldCounter := counterMap[cmdInc.counterID]

			counterMap[cmdInc.counterID] = hello.Counter{
				ID:      cmdInc.counterID,
				Version: oldCounter.Version,
				Value:   oldCounter.Value + 1,
			}

			updates[cmdInc.counterID] = counterUpdate{
				oldVersion: oldCounter.Version,
				value:      oldCounter.Value + 1,
			}

			replyEvents = append(replyEvents, replyEvent{
				replyChan: cmdInc.replyChan,
				event:     eventInc{err: nil},
			})

		default:
			panic("Invalid command type")
		}
	}

	for id, update := range updates {
		counterMap[id] = hello.Counter{
			ID:      id,
			Version: update.oldVersion + 1,
			Value:   update.value,
		}
	}

	return processResponse{
		updates:     updates,
		replyEvents: replyEvents,
	}
}

func (p *processor) processCommands(cmds []command) error {
	res := processCommandsPure(p.nodes, p.selfNodeID, p.counterMap, cmds)

	counters := make([]hello.CounterUpsert, 0, len(res.updates))
	for id, update := range res.updates {
		counters = append(counters, hello.CounterUpsert{
			ID:         id,
			NewVersion: update.oldVersion + 1,
			Value:      update.value,
		})

	}

	// save to database, close all channels if error
	ctx := context.Background()
	err := p.repo.Transact(ctx, func(ctx context.Context, tx hello.TxRepository) error {
		return tx.UpsertCounters(ctx, counters)
	})
	if err == hello.ErrCommandAborted {
		for _, re := range res.replyEvents {
			e := re.event.SetError(hello.ErrCommandAborted)
			re.replyChan <- e
		}
		return nil
	}
	if err != nil {
		for _, re := range res.replyEvents {
			close(re.replyChan)
		}
		return err
	}

	for _, re := range res.replyEvents {
		re.replyChan <- re.event
	}
	return nil
}

func (p *processor) handleWatch(nodes []core.NodeInfo) error {
	counters, err := p.repo.GetAllCounters(context.Background())
	if err != nil {
		return err
	}
	for _, c := range counters {
		p.counterMap[c.ID] = c
	}

	hasSelf := false
	for _, n := range nodes {
		if n.NodeID == p.selfNodeID {
			hasSelf = true
		}
	}
	if !hasSelf {
		return hello.ErrShardingConfig
	}

	fmt.Println(nodes)
	p.nodes = nodes
	return nil
}

func hashCounterID(counterID hello.CounterID) core.Hash {
	return core.HashUint32(uint32(counterID))
}
