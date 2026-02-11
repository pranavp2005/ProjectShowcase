// file name consensus_rpc.go
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"pranavpateriya.com/multipaxos/common"
	"pranavpateriya.com/multipaxos/server/persistence/statemachine"
)

var consensusLogger = slog.Default().With(slog.String("file", "consensus_rpc.go"))

type ConsensusRPC struct{ node *Node } // Accept / Commit / etc.
/*
startConsensusRPC: registers the RPC and starts listening for incoming connections
*/
func startConsensusRPC(n *Node) {
	srv := &ConsensusRPC{node: n}
	if err := rpc.RegisterName("ConsensusRPC", srv); err != nil {
		consensusLogger.Error("rpc registration failed", slog.Any("error", err))
		os.Exit(1)
	}

	l, err := net.Listen("tcp", n.Address)
	if err != nil {
		consensusLogger.Error("listen failed", slog.String("address", n.Address), slog.Any("error", err))
		os.Exit(1)
	}
	// Handle incoming RPC connections in a dedicated goroutine so the caller
	// returns immediately and the listener can keep serving new peers without
	// blocking on any single connection.
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				consensusLogger.Error("accept failed", slog.Any("error", err))
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

/*
Accept:
Part of normal operations here is what it does:
1) Takes a lock on the node object
2) If node is stopped then returns with OK: false, to simulate deadnode
3) Node updates its ballot number to the one seen right now promising to never accept a lower ballot number
4) Look at the sequence number and resizes logs if necessary
5) When Resizing logs, insert empty logs in between these are gaps(status X) and not no ops
6)
*/
func (s *ConsensusRPC) Accept(args *AcceptArgs, reply *Accepted) error {
	currNode := s.node
	if args == nil {
		consensusLogger.Error("accept called with nil args", slog.Int("node", currNode.NodeID))
		return fmt.Errorf("nil AcceptArgs")
	}

	consensusLogger.Info("handling accept", slog.Int("node", currNode.NodeID), slog.Int("seq", args.SequenceNum), slog.Any("ballot", args.Ballot))
	LogProtocol(currNode.NodeID, "Accept received: <ACCEPT, b: %+v, s: %d, m %+v>", args.Ballot, args.SequenceNum, args.Value)

	currNode.mu.Lock()
	defer currNode.mu.Unlock()

	// Node stopped return immediately
	if currNode.Status == common.StatusStopped {
		*reply = Accepted{
			Ok:            false,
			Promised:      currNode.Promised,
			SequenceNum:   args.SequenceNum,
			ClientRequest: args.Value,
			Message:       "Node Inactive",
			NodeRepliedID: currNode.NodeID,
		}
		consensusLogger.Info("accept reply", slog.String("result", "inactive"), slog.Int("node", currNode.NodeID), slog.Int("seq", args.SequenceNum))
		LogProtocol(currNode.NodeID, "Accept reply (NACK node inactive): <ACCEPTED, b: %+v, s: %d, m:%+v, nb:%d>", currNode.Promised, args.SequenceNum, args.Value, currNode.NodeID)

		return nil
	}

	// Promise check: NACK if args.Ballot < Promised
	//this is a strict check
	if args.Ballot.Less(&currNode.Promised) {
		consensusLogger.Info("accept NACK lower ballot", slog.Int("node", currNode.NodeID),slog.Int("seq", args.SequenceNum), slog.Any("promised", currNode.Promised), slog.Any("incoming", args.Ballot))

		*reply = Accepted{
			Ok:            false,
			Promised:      currNode.Promised,
			SequenceNum:   args.SequenceNum,
			Message:       "NACK: lower ballot than promised",
			NodeRepliedID: currNode.NodeID,
			ClientRequest: args.Value,
		}
		LogProtocol(currNode.NodeID, "Accept reply (NACK lower ballot %+v): <ACCEPTED, b: %+v, s: %d, m:%+v, nb:%d>", args.Ballot, currNode.Promised, args.SequenceNum, args.Value, currNode.NodeID)
		return nil
	}

	// Update promise (we promise at least this ballot)
	currNode.Promised = args.Ballot
	consensusLogger.Info("accept updating promised", slog.Int("node", currNode.NodeID), slog.Any("promised", currNode.Promised))
	
	//NOTE: update the ClientRequestIndex
	currNode.Log.ClientRequestIndex[args.Value.TransactionID.TransactionId] = args.SequenceNum
	
	// Ensure log capacity and entry allocation
	idx := args.SequenceNum
	if idx >= len(currNode.Log.LogEntries) {
		grow := idx - len(currNode.Log.LogEntries) + 1
		for i := 0; i < grow; i++ {
			currNode.Log.LogEntries = append(currNode.Log.LogEntries, GetEmptyLogEntry())
		}
	}

	//since we used *LogEntry we did not have to assign back entry to currNode.Log.LogEntries[idx]
	entry := currNode.Log.LogEntries[idx]
	if entry == nil {
		entry = GetEmptyLogEntry()
		currNode.Log.LogEntries[idx] = entry
	}

	entry.TxnVal = args.Value
	entry.IsNoOp = false //no op is only true in NewViewMessage, otherwise it's just gap
	entry.Status = common.StatusAccepted
	entry.BallotNum = args.Ballot

	*reply = Accepted{
		Ok:            true,
		Promised:      currNode.Promised,
		SequenceNum:   args.SequenceNum,
		ClientRequest: args.Value,
		Message:       "ACCEPTED",
		NodeRepliedID: currNode.NodeID,
	}
	consensusLogger.Info("accept reply ok", slog.Int("node", currNode.NodeID), slog.Int("seq", args.SequenceNum), slog.Any("ballot", currNode.Promised))
	LogProtocol(currNode.NodeID, "Accepted sent (OK): <ACCEPTED, b: %+v, s: %d, m:%+v, nb:%d>", currNode.Promised, args.SequenceNum, args.Value, currNode.NodeID)

	// ===== Timer Strategy 1: leader activity seen -> reset timer =====
	//Why at the end? Because we want to make sure this is a valid leader, if this is a node which thinks it is the leader
	//but it is not then we do not want to bump up the timer
	currNode.bumpOnLeaderActivity()
	consensusLogger.Info("accept reset timer", slog.Int("node", currNode.NodeID))
	return nil
}

/*
Commit:
Part of normal operations here is what it does:
1) Takes a lock on the node object
2) If node is stopped then returns with OK: false, to simulate deadnode
3) Node updates its ballot number to the one seen right now promising to never accept a lower ballot number
4) Look at the sequence number and resizes logs if necessary
5) When Resizing logs, insert empty logs in between these are gaps(status X) and not no ops
6) Marks the LogEntry at sequence number committed
7) Does we try to execute if we can and if executed till index > the sequence number then we know the entry has been executed
*/
func (s *ConsensusRPC) Commit(args *CommitArgs, reply *CommitReply) error {
	currNode := s.node
	logString := fmt.Sprintf("Commit: nodeID %d, sender Ballot %+v, sequence number %+v", currNode.NodeID, args.BallotNumber, args.SequenceNumber)
	LogProtocol(currNode.NodeID, "Commit received: seq=%d ballot=%+v txn={%s->%s %d}", args.SequenceNumber, args.BallotNumber, args.ClientRequest.Transaction.From, args.ClientRequest.Transaction.To, args.ClientRequest.Transaction.Amount)

	if args == nil {
		msg := fmt.Sprintf("empty args for commit RPC in node %d", currNode.NodeID)
		consensusLogger.Error("commit RPC received empty args", slog.Int("node_id", currNode.NodeID))
		LogProtocol(currNode.NodeID, "Commit error: nil args")
		return errors.New(msg)
	}
	if args.SequenceNumber < 0 {
		consensusLogger.Error(logString + "sequence number -ve")
		return fmt.Errorf("invalid sequence number %d", args.SequenceNumber)
	}

	currNode.mu.Lock() 
	defer currNode.mu.Unlock()

	// Return immedialtely with proper status if nodestatus in inactive
	if currNode.Status == common.StatusStopped {
		consensusLogger.Info(logString + " this node is stopped")
		reply.Status = false
		reply.Message = "Node Inactive"
		LogProtocol(currNode.NodeID, "Commit reply sent (inactive): seq=%d", args.SequenceNumber)
		return nil
	}

	// Promise check: NACK if args.Ballot < Promised
	//this isn't really needed
	if args.BallotNumber.Less(&currNode.Promised) {
		consensusLogger.Info(logString + fmt.Sprintf("Reject current promised ballot %v is higher that args", currNode.Promised))
		*reply = CommitReply{
			Status:   false,
			Promised: currNode.Promised,
			Message:  "NACK: lower ballot than promised",
		}
		LogProtocol(currNode.NodeID, "Commit reply sent (NACK lower ballot): seq=%d promised=%+v", args.SequenceNumber, currNode.Promised)
		return nil
	}

	currNode.Promised = args.BallotNumber

	// Extend log if needed
	idx := args.SequenceNumber
	if idx >= len(currNode.Log.LogEntries) {
		grow := idx - len(currNode.Log.LogEntries) + 1
		for i := 0; i < grow; i++ {
			currNode.Log.LogEntries = append(currNode.Log.LogEntries, GetEmptyLogEntry())
		}
	}

	entry := currNode.Log.LogEntries[idx]
	if entry == nil {
		entry = GetEmptyLogEntry()
		currNode.Log.LogEntries[idx] = entry
	}

	entry.IsNoOp = false // IsNoOp is only set to true during newview
	entry.TxnVal = args.ClientRequest

	if entry.Status == common.StatusAccepted {
		entry.Status = common.StatusCommitted
	}

	entry.BallotNum = args.BallotNumber

	//Assignback again just for safety
	currNode.Log.LogEntries[idx] = entry

	//if current node is not the leader then execute now, other wise the leader executes after sending commit messages
	if currNode.Role != common.RoleLeader {
		execute(currNode)
	}

	reply.Status = true
	reply.Message = fmt.Sprintf("Committed node:%d seq:%d ballot:%+v, with txn %+v", currNode.NodeID, args.SequenceNumber, args.BallotNumber, args.ClientRequest.Transaction)
	consensusLogger.Info("commit applied", slog.Int("node_id", currNode.NodeID), slog.Int("seq", args.SequenceNumber), slog.Any("ballot", args.BallotNumber), slog.Any("transaction", args.ClientRequest.Transaction))
	LogProtocol(currNode.NodeID, "Commit applied: seq=%d ballot=%+v", args.SequenceNumber, args.BallotNumber)

	// ===== Strategy 1: leader activity seen -> reset timer =====
	currNode.bumpOnLeaderActivity()
	consensusLogger.Info(logString + "timer reset")

	return nil
}

// IsNoOp is meant to signify if the leader does not have any info about the transaction in that slot
func GetEmptyLogEntry() *LogEntry {
	return &LogEntry{IsNoOp: false, Status: common.StatusNoStatus, TxnVal: common.TransactionReq{}, BallotNum: GetInitBallot(), ClientReply: common.TransactionClientReply{Executed: false, Message: "not yet executed"}}
}

func GetInitBallot() Ballot {
	return Ballot{N: 0, NodeID: 0}
}

/*
proposeAndCommit: called in the leader and sends accept, commit and executes the logs in the leader
*/
func (currNode *Node) proposeAndCommit(seq int, tx common.TransactionReq, bal Ballot, rpcTimeout time.Duration) (Execution, error) {
	// Self-accept optimistically (no RPC)
	selfReply := &Accepted{}
	LogProtocol(currNode.NodeID, "Accept sent (local): <ACCEPT, b: %+v, seq: %d , m: %+v>", bal, seq, tx)
	consensusLogger.Info("accept sent local", slog.Int("node", currNode.NodeID), slog.Int("seq", seq), slog.Any("ballot", bal))
	if err := (&ConsensusRPC{node: currNode}).Accept(&AcceptArgs{
		Ballot:      bal,
		SequenceNum: seq,
		Value:       tx,
	}, selfReply); err != nil || !selfReply.Ok {
		LogProtocol(currNode.NodeID, "Accept local failed: <ACCEPTED, b: %+v, s: %d, m:%+v, nb:%d>", bal, seq, tx, currNode.NodeID)
		consensusLogger.Error("", slog.Int("nodeId", currNode.NodeID), slog.String("message", fmt.Sprintf("Accept local failed: <ACCEPTED, b: %+v, s: %d, m:%+v, nb:%d>", bal, seq, tx, currNode.NodeID)))
		return Execution{Result: false, Message: common.SelfAcceptFailed, Ballot:  bal}, fmt.Errorf("self Accept failed: %v, ok=%v", err, selfReply.Ok)
	}
	LogProtocol(currNode.NodeID, "Accepted local (OK): <ACCEPTED, b: %+v, s: %d, m:%+v, nb:%d>", bal, seq, tx, currNode.NodeID)

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		oks         = 1 // already counted self
		nacks       = 0
		highestSeen = currNode.Promised
	)

	for _, peer := range currNode.Peers {
		if peer == currNode.Address { // skip self; already accepted
			continue
		}
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			LogProtocol(currNode.NodeID, "Accept sent %s: <ACCEPT, b: %+v, seq: %d , m: %+v", peer, bal, seq, tx)
			consensusLogger.Info("connecting to peer", peer)
			client, err := dialOnce(peer, rpcTimeout)
			if err != nil {
				mu.Lock()
				nacks++
				mu.Unlock()
				consensusLogger.Error("", slog.String("Accept send failed: to=", peer), slog.Any("seq=", seq), slog.Any("err", err))
				return
			}
			defer client.Close()

			args := &AcceptArgs{Ballot: bal, SequenceNum: seq, Value: tx}
			var rep Accepted
			callErr := client.Call("ConsensusRPC.Accept", args, &rep)
			if callErr != nil {
				mu.Lock()
				nacks++
				mu.Unlock()
				LogProtocol(currNode.NodeID, "Accept response error: from=%s seq=%d err=%v", peer, seq, callErr)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if rep.Ok {
				oks++
				LogProtocol(currNode.NodeID, "Accepted received: from=%s seq=%d ok=true", peer, seq)
			} else {
				nacks++
				LogProtocol(currNode.NodeID, "Accepted received: from=%s seq=%d ok=false promised to ballot=%+v", peer, seq, rep.Promised)
				if rep.Promised.GreaterEq(&highestSeen) {
					highestSeen = rep.Promised
				}
			}
		}()
	}

	wg.Wait()

	if oks < currNode.quorumSize() {
		consensusLogger.Error("no quorum on Accept")
		LogProtocol(currNode.NodeID, "Could not achieve quorum accepted=%d seq=%d ballot=%+v", oks, seq, bal)
		return Execution{Result: false, Message: common.NoQuorumError, Ballot: bal}, fmt.Errorf("no quorum on Accept: oks=%d nacks=%d (highest promised seen: %+v)", oks, nacks, highestSeen)
	}

	// Got quorum -> send Commit to all (including self is fine)
	var commitOK int32
	var cwg sync.WaitGroup

	//Do a local commit first
	var rep CommitReply
	_ = (&ConsensusRPC{node: currNode}).Commit(&CommitArgs{SequenceNumber: seq, BallotNumber: bal, ClientRequest: tx}, &rep)
	if rep.Status {
		atomic.AddInt32(&commitOK, 1)
	}
	LogProtocol(currNode.NodeID, "Commit local reply: seq=%d status=%v", seq, rep.Status)

	commitOne := func(addr string) {
		defer cwg.Done()
		var rep CommitReply
		LogProtocol(currNode.NodeID, "Commit sent: to=%s seq=%d ballot=%+v", addr, seq, bal)
		client, err := dialOnce(addr, rpcTimeout)
		if err != nil {
			LogProtocol(currNode.NodeID, "Commit send failed: to=%s seq=%d err=%v", addr, seq, err)
			consensusLogger.Error("commit send failed", slog.Int("node", currNode.NodeID), slog.String("peer", addr), slog.Int("seq", seq), slog.Any("error", err))
			return
		}
		defer client.Close()
		_ = client.Call("ConsensusRPC.Commit", &CommitArgs{SequenceNumber: seq, BallotNumber: bal, ClientRequest: tx}, &rep)
		if rep.Status {
			atomic.AddInt32(&commitOK, 1)
		}
		LogProtocol(currNode.NodeID, "Commit reply received: from=%s seq=%d status=%v", addr, seq, rep.Status)
	}

	for _, p := range currNode.Peers {
		if p != currNode.Address {
			cwg.Add(1)
			go commitOne(p)
		}
	}
	cwg.Wait()

	//Leader sent commits to other nodes, now the leader can execute
	lastExecuted := execute(currNode)

	if lastExecuted < seq {
		consensusLogger.Info("no entries executed after commit attempt", slog.Int("node_id", currNode.NodeID), slog.Int("seq", seq))

		return Execution{Result: false, Message: common.GapInLogs, Ballot: bal}, nil
	}

	return Execution{Result: true, Message: "transaction executed", Ballot: bal}, nil
}

// TODO: check if logic is correct here
func execute(node *Node) (int) {	

	node.Log.LogMutex.Lock()

	defer node.Log.LogMutex.Unlock()

	lastExecuted := node.Log.ExecutedIndex
	startExecutionFrom := lastExecuted + 1

	LogProtocol(node.NodeID,"lastexecuted %d, startExecution %d", lastExecuted, startExecutionFrom)
	LogProtocol(node.NodeID,"length of logs is %d", len(node.Log.LogEntries))

	if startExecutionFrom >= len(node.Log.LogEntries) {
		return node.Log.ExecutedIndex
	}

	for idx := startExecutionFrom; idx < len(node.Log.LogEntries); idx++ {
		entry := node.Log.LogEntries[idx]

		// Stop on gap/end/unallocated
		if entry == nil || entry.Status == common.StatusNoStatus {
			LogProtocol(node.NodeID, "stop execution at %d, gap encountered", idx)
			break
		}

		// Execute only committed (or already executed) entries
		if entry.Status != common.StatusCommitted && entry.Status != common.StatusExecuted {
			LogProtocol(node.NodeID, "entry at index %d has status %v exiting", idx, entry.Status)
			break
		}

		// Already executed -> advance pointer
		if entry.Status == common.StatusExecuted {
			LogProtocol(node.NodeID, "entry at index %d has status %v go to next ", idx, string(entry.Status))

			lastExecuted = idx
			continue
		}

		if entry.IsNoOp {
			LogProtocol(node.NodeID, "entry at index %d is no op, no change in state machine", idx)
			entry.ClientReply = common.TransactionClientReply{Executed: true, Message: common.NoOpSuccess}
			entry.Status = common.StatusExecuted
		} else {
			// Non-noop must have a valid txn; otherwise treat as a gap and stop.
			txn := entry.TxnVal
			if txn.Transaction == (common.Transaction{}) {
				LogProtocol(node.NodeID, "entry at index %d has no transaction", idx)
				break
			}

			if _, _, err := node.Bank.Transfer(
				statemachine.AccountID(txn.Transaction.From),
				statemachine.AccountID(txn.Transaction.To),
				statemachine.Dollars(txn.Transaction.Amount),
			); err != nil {
				LogProtocol(node.NodeID, "txn %s -> %s, %d unsuccessful", txn.Transaction.From, txn.Transaction.To, txn.Transaction.Amount)
				if err == statemachine.ErrInsufficient {
					LogProtocol(node.NodeID, "reason: insufficient balance")
					entry.ClientReply = common.TransactionClientReply{Executed: true, Message: common.InsufficientBalance}
				} else if err == statemachine.ErrUnknownAccount {
					LogProtocol(node.NodeID, "reason: unknown account %s or %s",txn.Transaction.From, txn.Transaction.To )
					entry.ClientReply = common.TransactionClientReply{Executed: true, Message: common.UnknownAccount}
				} else if err == statemachine.ErrNegativeAmount {
					LogProtocol(node.NodeID, "reason: negative amount")
					entry.ClientReply = common.TransactionClientReply{Executed: true, Message: common.NegativeTxnAmt}
				} else {
					LogProtocol(node.NodeID, "reason: internal error")
					entry.ClientReply = common.TransactionClientReply{Executed: true, Message: common.InternalError}
				}

				entry.Status = common.StatusExecuted
				lastExecuted = idx
				node.Log.ExecutedIndex = lastExecuted
				continue
			} else {
				LogProtocol(node.NodeID, "txn %s -> %s, %d successful", txn.Transaction.From, txn.Transaction.To, txn.Transaction.Amount)
			}
			entry.ClientReply = common.TransactionClientReply{Executed: true, Message: "ok"}
			entry.Status = common.StatusExecuted
		}

		lastExecuted = idx
		LogProtocol(node.NodeID, "lastExecuted %d status set to executed", idx)

	}

	//NOTE: This case should not arise, but in order to prevent mayhem just use this
	if lastExecuted > node.Log.ExecutedIndex {
		LogProtocol(node.NodeID, "Node's Executed index %d updated to %d", node.Log.ExecutedIndex, lastExecuted)
		node.Log.ExecutedIndex = lastExecuted
	}
	LogProtocol(node.NodeID, "executed till %d", node.Log.ExecutedIndex)
	return node.Log.ExecutedIndex
}
