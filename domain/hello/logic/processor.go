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

// PROCESSOR

const maxBatchSize = 5000

type processor struct {
	repo       hello.Repository
	cmdChan    <-chan command
	counterMap map[hello.CounterID]uint32

	hashes     []core.ConsistentHash
	selfNodeID core.NodeID
}

func newProcessor(selfNodeID core.NodeID, repo hello.Repository, cmdChan <-chan command) *processor {
	return &processor{
		repo:       repo,
		cmdChan:    cmdChan,
		counterMap: make(map[hello.CounterID]uint32),
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
			p.handleWatch(wr.Hashes)

		case <-ctx.Done():
			return nil
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
				p.handleWatch(wr.Hashes)

			case <-ctx.Done():
				return nil

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

type processResponse struct {
	updates     map[hello.CounterID]uint32
	replyEvents []replyEvent
}

func processCommandsPure(
	hashes []core.ConsistentHash, selfNodeID core.NodeID,
	counterMap map[hello.CounterID]uint32, commands []command,
) processResponse {
	updates := make(map[hello.CounterID]uint32)
	replyEvents := make([]replyEvent, 0, len(commands))

	for _, cmd := range commands {
		switch cmd.Type() {
		case commandTypeInc:
			cmdInc := cmd.(commandInc)
			fmt.Println(cmdInc)

			hash := hashCounterID(cmdInc.counterID)
			fmt.Println(hash)

			nullNodeID := core.GetNodeID(hashes, hash)
			if !nullNodeID.Valid || nullNodeID.NodeID != selfNodeID {
				replyEvents = append(replyEvents, replyEvent{
					replyChan: cmdInc.replyChan,
					event:     eventInc{err: hello.ErrCommandAborted},
				})
				break
			}

			counterMap[cmdInc.counterID] = counterMap[cmdInc.counterID] + 1
			updates[cmdInc.counterID] = counterMap[cmdInc.counterID]

			replyEvents = append(replyEvents, replyEvent{
				replyChan: cmdInc.replyChan,
				event:     eventInc{err: nil},
			})

		default:
			panic("Invalid command type")
		}
	}
	return processResponse{
		updates:     updates,
		replyEvents: replyEvents,
	}
}

func (p *processor) processCommands(cmds []command) error {
	res := processCommandsPure(p.hashes, p.selfNodeID, p.counterMap, cmds)

	// save to database, close all channels if error
	ctx := context.Background()
	err := p.repo.Transact(ctx, func(ctx context.Context, tx hello.TxRepository) error {
		for id, value := range res.updates {
			err := tx.UpsertCounter(ctx, id, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
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

func (p *processor) handleWatch(inputHashes []core.ConsistentHash) {
	hashes := make([]core.ConsistentHash, len(inputHashes))
	copy(hashes, inputHashes)
	core.Sort(hashes)

	if core.Equals(hashes, p.hashes) {
		return
	}

	fmt.Println("Hashes:", hashes)
	p.hashes = hashes
}

func hashCounterID(counterID hello.CounterID) core.Hash {
	return core.HashUint32(uint32(counterID))
}
