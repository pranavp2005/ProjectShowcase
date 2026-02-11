// filename election_rpc.go
package main

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"time"

	"pranavpateriya.com/multipaxos/common"
)

type ElectionRPC struct{ node *Node }

func startElectionRPC(n *Node) {
	srv := &ElectionRPC{node: n}
	if err := rpc.RegisterName("ElectionRPC", srv); err != nil {
		slog.Error("election rpc registration failed", slog.Any("error", err))
		os.Exit(1)
	}
	// Re-use the same listener as ConsensusRPC? Simpler: separate listen on same addr.
	// net/rpc allows multiple services on same server; we already registered ConsensusRPC
	// in startConsensusRPC via default rpc server. So we don't listen again here.
	// (startConsensusRPC already did: go accept loop + rpc.ServeConn)
	// Nothing else to do.
}

func (electionRPC *ElectionRPC) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	n := electionRPC.node
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Status == common.StatusStopped {
		*reply = HeartbeatReply{Ok: false, Promised: n.Promised}
		return nil
	}

	// If heartbeat carries a strictly higher ballot, advance promise.
	if n.Promised.Less(&args.Ballot) {
		n.Promised = args.Ballot
	}
	n.Role = common.RoleBackup

	n.bumpOnLeaderActivity()
	*reply = HeartbeatReply{Ok: true, Promised: n.Promised}
	return nil
}

// ===== PREPARE (Phase 1a/1b) =====
// Strategy 1: if we haven't timed out yet, block until our liveness timer expires,
// then accept the HIGHEST-ballot Prepare (we're single-threaded per-conn, so we
// simply wait until expiry and then check again).
func (electionRPC *ElectionRPC) Prepare(args *PrepareArgs, reply *Promise) error {

	currNode := electionRPC.node
	if args == nil {
		slog.Info("Prepare: recieved empty args")
		return fmt.Errorf("nil PrepareArgs")
	}

	LogProtocol(currNode.NodeID, "Prepare received: ballot=%+v", args.Ballot)

	// Fast reject: strictly lower than promised
	currNode.mu.Lock()
	if currNode.SystemStartup {
		currNode.SystemStartup = false
	}

	if args.Ballot.Less(&currNode.Promised) {
		*reply = Promise{Ok: false, Promised: currNode.Promised}
		LogProtocol(currNode.NodeID, "Promise sent (NACK): promised=%+v lower ballot=%+v", currNode.Promised, args.Ballot)
		currNode.mu.Unlock()
		return nil
	}
	slog.Info(fmt.Sprintf("nodeId: %d, recieved ballot number %+v", currNode.NodeID, args.Ballot))

	// Strategy 1 book-keeping
	currNode.LastSeenPrepare = time.Now()
	currNode.mu.Unlock()

	//NOTE: In project spec it is written we have to wait but I don't think we should
	// remaining := currNode.timeToExpiry()

	// If timer not expired, block until it does (bounded wait)
	// if remaining > 0 {
	// 	time.Sleep(remaining)
	// }

	currNode.mu.Lock()
	defer currNode.mu.Unlock()

	// Node down?
	if currNode.Status == common.StatusStopped {
		*reply = Promise{Ok: false, Promised: currNode.Promised}
		LogProtocol(currNode.NodeID, "Promise not sent: node stopped promised=%+v", currNode.Promised)
		return nil
	}

	// Another higher ballot may have arrived while we waited
	if args.Ballot.Less(&currNode.Promised) {
		*reply = Promise{Ok: false, Promised: currNode.Promised}
		LogProtocol(currNode.NodeID, "Promise sent (NACK after wait): promised=%+v lower ballot=%+v", currNode.Promised, args.Ballot)
		return nil
	}

	// Promise up to this ballot
	currNode.Promised = args.Ballot
	currNode.Role = common.RoleBackup
	currNode.bumpOnLeaderActivity()

	// Build AcceptedRecords from our log
	var acc []AcceptLog
	//TODO: the AcceptLog is built as follows:
	//the backup sends all logs
	for slot := 0; slot < len(currNode.Log.LogEntries); slot++ {
		e := currNode.Log.LogEntries[slot]
		if e == nil {
			continue
		}
		if e.Status != common.StatusNoStatus {
			acc = append(acc, AcceptLog{
				Slot:   slot,
				Ballot: e.BallotNum,
				Value:  e.TxnVal,
			})
		}
	}

	*reply = Promise{
		Ok:        true,
		Promised:  currNode.Promised,
		AccpetLog: acc,
	}
	LogProtocol(currNode.NodeID, "Promise sent: promised=%+v accepted=%d", currNode.Promised, len(acc))
	return nil
}

// ===== NEW-VIEW (Phase 1c) =====
// Leader sends normalized log (with gaps as no-ops) at its ballot.
func (electionRPC *ElectionRPC) NewView(args *NewViewArgs, reply *NewViewReply) error {
	currNode := electionRPC.node
	LogProtocol(currNode.NodeID, "NewView received: fromLeader=%d ballot=%+v entries=%d", args.FromLeader, args.Ballot, len(args.Entries))

	if currNode.Status == common.StatusStopped {
		*reply = NewViewReply{
			Ok:       false,
			Promised: currNode.Promised,
			Message:  "Node Inactive",
		}
		return nil
	}

	currNode.mu.Lock()
	defer currNode.mu.Unlock()

	// Only accept if we haven't promised > args.Ballot
	if args.Ballot.Less(&currNode.Promised) {
		reply.Ok = false
		reply.From = currNode.NodeID
		reply.Promised = currNode.Promised
		LogProtocol(currNode.NodeID, "NewView rejected: promised=%+v greater than ballot", currNode.Promised)
		return nil
	}

	currNode.Promised = args.Ballot
	// currNode.Role = common.RoleBackup

	var accepted, already []int

	logEntries := currNode.Log.LogEntries

	//resize logs:
	if len(args.Entries) >= len(currNode.Log.LogEntries) {
		grow := len(args.Entries) - len(currNode.Log.LogEntries) + 1
		for i := 0; i < grow; i++ {
			currNode.Log.LogEntries = append(currNode.Log.LogEntries, GetEmptyLogEntry())
		}
		logEntries = currNode.Log.LogEntries
	}

	for idx, incoming := range args.Entries {
		if incoming == nil {
			continue
		}

		existing := logEntries[idx]
		if existing == nil {
			existing = GetEmptyLogEntry()
			logEntries[idx] = existing
		}

		if existing.Status != common.StatusNoStatus && existing.BallotNum.GreaterEq(&incoming.BallotNum) && existing.TxnVal == incoming.TxnVal {
			already = append(already, idx)
			continue
		}

		existing.IsNoOp = incoming.IsNoOp
		existing.BallotNum = incoming.BallotNum
		if existing.Status != common.StatusExecuted {
			existing.Status = common.StatusAccepted
		} else {
			if existing.TxnVal.TransactionID.TransactionId != incoming.TxnVal.TransactionID.TransactionId {
				slog.Error("NewView error, the entry is executed but in new view I got different entry", slog.Any("executed", existing.TxnVal), slog.Any("incoming", incoming.TxnVal))
			}
		}
		existing.TxnVal = incoming.TxnVal
		logEntries[idx] = existing
		accepted = append(accepted, idx)
	}
	currNode.Log.LogEntries = logEntries

	record := NewViewRecord{
		FromLeader: args.FromLeader,
		Ballot:     args.Ballot,
		ReceivedAt: time.Now(),
	}

	updateSeenReq := make(map[string]int)
	for idx, incoming := range args.Entries {
		if incoming == nil {
			continue
		}
		if incoming.TxnVal.TransactionID.TransactionId != "" {
			updateSeenReq[incoming.TxnVal.TransactionID.TransactionId] = idx
		}
		record.Entries = append(record.Entries, NewViewEntry{
			Sequence:    idx,
			IsNoOp:      incoming.IsNoOp,
			Status:      incoming.Status,
			Ballot:      incoming.BallotNum,
			Transaction: incoming.TxnVal,
		})
	}
	currNode.NewViewHistory = append(currNode.NewViewHistory, record)
	currNode.Log.ClientRequestIndex = updateSeenReq

	reply.Ok = true
	reply.From = currNode.NodeID
	reply.Promised = currNode.Promised
	reply.AcceptedSeqs = accepted
	reply.AlreadyHadSeq = already

	currNode.bumpOnLeaderActivity()
	LogProtocol(currNode.NodeID, "NewView applied: accepted=%d alreadyHad=%d", len(accepted), len(already))

	return nil
}

func (electionRPC *ElectionRPC) LeaderFailure(args *LeaderFailureArgs, reply *LeaderFailureReply) error {
	n := electionRPC.node
	reason := "unspecified"
	if args != nil && args.Reason != "" {
		reason = args.Reason
	}

	n.mu.Lock()
	if n.Role != common.RoleLeader {
		n.mu.Unlock()
		if reply != nil {
			reply.Stopped = false
			reply.Message = "node not leader; ignoring failure"
		}
		return nil
	}
	alreadyStopped := n.Status == common.StatusStopped
	if !alreadyStopped {
		n.Status = common.StatusStopped
		n.Role = common.RoleBackup
	}
	n.mu.Unlock()

	if !alreadyStopped {
		n.StopStrategy1Timer()
		slog.Warn("leader failure triggered; node stopped", slog.Int("node", n.NodeID), slog.String("reason", reason))
	} else {
		slog.Info("leader failure rpc received but node already stopped", slog.Int("node", n.NodeID))
	}

	if reply != nil {
		if alreadyStopped {
			reply.Stopped = false
			reply.Message = "node already stopped"
		} else {
			reply.Stopped = true
			reply.Message = "node transitioned to stopped"
		}
	}
	return nil
}

func (electionRPC *ElectionRPC) LeaderResume(args *LeaderResumeArgs, reply *LeaderResumeReply) error {
	n := electionRPC.node
	reason := "unspecified"
	if args != nil && args.Reason != "" {
		reason = args.Reason
	}

	n.mu.Lock()
	wasStopped := n.Status == common.StatusStopped
	n.Status = common.StatusActive
	//TODO: test with backup and what happens if I keep leader
	n.Role = common.RoleLeader
	n.mu.Unlock()

	if wasStopped {
		n.StartStrategy1Timer(n.t, n.tp, n.hbInterval)
		slog.Info("leader resume triggered; node active", slog.Int("node", n.NodeID), slog.String("reason", reason))
	} else {
		slog.Info("leader resume RPC received but node already active", slog.Int("node", n.NodeID))
	}

	if reply != nil {
		if wasStopped {
			reply.Resumed = true
			reply.Message = "node resumed as backup"
		} else {
			reply.Resumed = false
			reply.Message = "node already active"
		}
	}
	return nil
}

func (electionRPC *ElectionRPC) BackupFailure(args *LeaderFailureArgs, reply *LeaderFailureReply) error {
	n := electionRPC.node
	reason := "unspecified"
	if args != nil && args.Reason != "" {
		reason = args.Reason
	}

	n.mu.Lock()
	alreadyStopped := n.Status == common.StatusStopped
	if !alreadyStopped {
		n.Status = common.StatusStopped
		n.Role = common.RoleBackup
	}
	n.mu.Unlock()

	if !alreadyStopped {
		n.StopStrategy1Timer()
		slog.Warn("backup failure triggered; node stopped", slog.Int("node", n.NodeID), slog.String("reason", reason))
	} else {
		slog.Info("backup failure RPC received but node already stopped", slog.Int("node", n.NodeID))
	}

	if reply != nil {
		if alreadyStopped {
			reply.Stopped = false
			reply.Message = "node already stopped"
		} else {
			reply.Stopped = true
			reply.Message = "node transitioned to stopped"
		}
	}
	return nil
}

func (electionRPC *ElectionRPC) BackupResume(args *LeaderResumeArgs, reply *LeaderResumeReply) error {
	n := electionRPC.node
	reason := "unspecified"
	if args != nil && args.Reason != "" {
		reason = args.Reason
	}

	n.mu.Lock()
	wasStopped := n.Status == common.StatusStopped
	n.Status = common.StatusActive
	n.mu.Unlock()

	if wasStopped {
		n.StartStrategy1Timer(n.t, n.tp, n.hbInterval)
		slog.Info("backup resume triggered; node active", slog.Int("node", n.NodeID), slog.String("reason", reason))
	} else {
		slog.Info("backup resume RPC received but node already active", slog.Int("node", n.NodeID))
	}

	if reply != nil {
		if wasStopped {
			reply.Resumed = true
			reply.Message = "node resumed as backup"
			n.Role = common.RoleBackup
		} else {
			reply.Resumed = false
			reply.Message = "node already active"
		}
	}
	return nil
}

func (electionRPC *ElectionRPC) PrintRole(args *PrintRoleArgs, reply *PrintRoleRes) error {
	n := electionRPC.node

	n.mu.Lock()
	slog.Info(fmt.Sprintf("Current role is %s, current status is %s", n.Role, n.Status))
	n.mu.Unlock()

	return nil
}
