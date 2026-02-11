package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/rpc"

	"pranavpateriya.com/multipaxos/common"
)

type ObserverRPC struct {
	node *Node
}

func startObserverRPC(n *Node) {
	srv := &ObserverRPC{node: n}
	if err := rpc.RegisterName("ObserverRPC", srv); err != nil {
		slog.Error("observer rpc registration failed", slog.Any("error", err))
	}
}

func (s *ObserverRPC) PrintLog(_ *common.PrintLogArgs, reply *common.PrintLogReply) error {
	if reply == nil {
		return errors.New("nil reply")
	}
	node := s.node

	node.Log.LogMutex.Lock()
	defer node.Log.LogMutex.Unlock()

	entries := make([]common.LogEntryMetadata, 0, len(node.Log.LogEntries))
	for seq := 0; seq < len(node.Log.LogEntries); seq++ {
		entry := node.Log.LogEntries[seq]
		meta := common.LogEntryMetadata{
			Sequence: seq,
			Status:   common.StatusNoStatus,
		}
		if entry != nil {
			meta.Status = entry.Status
			meta.BallotN = entry.BallotNum.N
			meta.BallotNode = entry.BallotNum.NodeID
			meta.IsNoOp = entry.IsNoOp
			meta.TransactionReq = entry.TxnVal
		}
		entries = append(entries, meta)
	}

	reply.Success = true
	reply.Entries = entries
	reply.Message = fmt.Sprintf("log entries: %d", len(entries))
	return nil
}

func (s *ObserverRPC) PrintDB(_ *common.PrintDBArgs, reply *common.PrintDBReply) error {
	if reply == nil {
		return errors.New("nil reply")
	}
	node := s.node

	snapshot := node.Bank.Snapshot(context.Background())
	result := make(map[string]int64, len(snapshot))
	for acct, bal := range snapshot {
		result[string(acct)] = int64(bal)
	}

	reply.Success = true
	reply.Balances = result
	reply.Message = fmt.Sprintf("accounts: %d", len(result))
	return nil
}

func (s *ObserverRPC) PrintStatus(args *common.PrintStatusArgs, reply *common.PrintStatusReply) error {
	if args == nil {
		return errors.New("nil args")
	}
	if reply == nil {
		return errors.New("nil reply")
	}
	node := s.node

	node.Log.LogMutex.Lock()
	defer node.Log.LogMutex.Unlock()

	status := common.StatusNoStatus
	if args.Sequence >= 0 && args.Sequence < len(node.Log.LogEntries) {
		entry := node.Log.LogEntries[args.Sequence]
		if entry != nil {
			status = entry.Status
		}
	}

	reply.Success = true
	reply.NodeID = node.NodeID
	reply.Sequence = args.Sequence
	reply.Status = status
	reply.ExecutedIndex = node.Log.ExecutedIndex
	reply.Message = fmt.Sprintf("node %d status for seq %d: %s", node.NodeID, args.Sequence, status)
	return nil
}

func (s *ObserverRPC) PrintView(_ *common.PrintViewArgs, reply *common.PrintViewReply) error {
	if reply == nil {
		return errors.New("nil reply")
	}
	node := s.node

	node.mu.Lock()
	history := make([]NewViewRecord, len(node.NewViewHistory))
	copy(history, node.NewViewHistory)
	node.mu.Unlock()

	views := make([]common.NewViewMetadata, 0, len(history))
	for _, rec := range history {
		meta := common.NewViewMetadata{
			FromLeader: rec.FromLeader,
			BallotN:    rec.Ballot.N,
			BallotNode: rec.Ballot.NodeID,
			ReceivedAt: rec.ReceivedAt,
		}
		if len(rec.Entries) > 0 {
			entrySummaries := make([]common.LogEntryMetadata, 0, len(rec.Entries))
			for _, ent := range rec.Entries {
				entrySummaries = append(entrySummaries, common.LogEntryMetadata{
					Sequence:       ent.Sequence,
					Status:         ent.Status,
					BallotN:        ent.Ballot.N,
					BallotNode:     ent.Ballot.NodeID,
					IsNoOp:         ent.IsNoOp,
					TransactionReq: ent.Transaction,
				})
			}
			meta.Entries = entrySummaries
		}
		views = append(views, meta)
	}

	reply.Success = true
	reply.Views = views
	reply.Message = fmt.Sprintf("new-view messages: %d", len(views))
	return nil
}
