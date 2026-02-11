// file name transaction_rpc.go
package main

import (
	"errors"
	"log/slog"
	"net/rpc"
	"os"
	"time"

	"pranavpateriya.com/multipaxos/common"
)

var txLogger = slog.Default().With("component", "transaction_rpc")

// TransactionRPC exposes client-facing APIs for submitting transactions.
type TransactionRPC struct {
	node *Node
}

// TransactionRPCReq carries a single client transaction.
type TransactionRPCReq struct {
	TransactionReq common.TransactionReq
}

// TransactionRPCReply reports the outcome of a submission.
type TransactionRPCReply struct {
	RequestResult bool
	Message       common.ExecutionResultMessage
	Ballot        Ballot
	LeaderID      int
}

// startTransactionRPC registers RPC
func startTransactionRPC(n *Node) {
	srv := &TransactionRPC{node: n}
	if err := rpc.RegisterName("TransactionRPC", srv); err != nil {
		txLogger.Error("transaction rpc registration failed", slog.Any("error", err))
		os.Exit(1)
	}
	txLogger.Info("transaction rpc registered")
}

func (svc *TransactionRPC) Submit(args *TransactionRPCReq, reply *TransactionRPCReply) error {
	if args == nil {
		txLogger.Warn("received nil transaction request")
		return errors.New("nil TransactionRequest")
	}
	if reply == nil {
		txLogger.Warn("received nil transaction reply placeholder")
		return errors.New("nil TransactionReply")
	}

	txLogger.Info("received transaction request", slog.Any("request", args.TransactionReq))

	currNode := svc.node
	txnReq := args.TransactionReq
	if txnReq.Transaction.To == "" || txnReq.TransactionID.ClientID == "" {
		txLogger.Error("received invalid transaction payload", slog.Any("request", args.TransactionReq))
		return errors.New("empty txn req")
	}

	currNode.mu.Lock()
	triggerElection := false
	if currNode.SystemStartup {
		currNode.SystemStartup = false
		if currNode.Status == common.StatusActive && currNode.Role != common.RoleLeader {
			triggerElection = true
		}
	}
	triggerIfNeeded := func() {
		if triggerElection {
			triggerElection = false
			// Start initial timer and kick off election.
			currNode.StartStrategy1Timer(currNode.t, currNode.tp, currNode.hbInterval)
			startElectionAsync(currNode)
		}
	}
	// order the client request
	seq := currNode.SequenceNumber
	key := txnReq.TransactionID.TransactionId
	newRequest := false

	if idx, ok := currNode.Log.ClientRequestIndex[key]; ok {
		if idx >= 0 && idx < len(currNode.Log.LogEntries) {
			entry := currNode.Log.LogEntries[idx]
			if entry != nil && entry.Status == common.StatusExecuted {
				reply.RequestResult = entry.ClientReply.Executed
				reply.Message = entry.ClientReply.Message
				reply.Ballot = currNode.SelfBallot
				reply.LeaderID = currNode.NodeID
				txLogger.Info("duplicate transaction served from log", slog.Int("node", currNode.NodeID), slog.String("txnKey", key), slog.Int("seq", idx))
				currNode.mu.Unlock()
				triggerIfNeeded()
				return nil
			} else if entry != nil && (entry.Status == common.StatusCommitted || entry.Status == common.StatusAccepted) {
				//NOTE: this is a duplicate transaction but it wasn't executed, so try to get it accepted by all and executed now
				//just set the sequence number and the rest should be handled
				seq = idx
			}
		}
	} else {
		newRequest = true
	}

	status := currNode.Status
	role := currNode.Role
	nodeID := currNode.NodeID

	//If node is not active simulate failed request
	if status != common.StatusActive {
		currNode.mu.Unlock()
		//NOTE: I don't need to triggerElections here
		// triggerIfNeeded()
		txLogger.Warn("rejecting transaction: node inactive", slog.Int("node", nodeID))
		reply.RequestResult = false
		reply.Message = "node inactive"
		return nil
	}

	//if the node is not a leader, we will not reply
	if role != common.RoleLeader {
		currNode.mu.Unlock()
		triggerIfNeeded()
		hintedLeader := int(currNode.Promised.NodeID)
		txLogger.Warn("rejecting transaction: not leader", slog.Int("node", nodeID))
		reply.RequestResult = false
		reply.Message = "not leader"
		if hintedLeader != 0 {
			reply.LeaderID = hintedLeader
		}
		return nil
	}

	bal := currNode.SelfBallot
	if newRequest {
		if key != "" {
			currNode.Log.ClientRequestIndex[key] = seq
		}
		currNode.SequenceNumber++
	} else if key != "" {
		currNode.Log.ClientRequestIndex[key] = seq
	}

	//NOTE: This is the timeout which the node uses for RPC Connections
	timeout := currNode.RPCTimeout
	if timeout <= 0 {
		timeout = 1000 * time.Millisecond
	}

	//NOTE: I think this is important
	currNode.mu.Unlock()
	triggerIfNeeded()

	txLogger.Info("attempting transaction proposal", slog.Int("node", nodeID), slog.Int("seq", seq))

	// since we are the leader start propose and commit phase
	executionRes, err := currNode.proposeAndCommit(seq, txnReq, bal, timeout)

	if err != nil {
		txLogger.Warn("transaction proposal failed", slog.Int("node", nodeID), slog.Int("seq", seq), slog.Any("error", err))
		reply.RequestResult = false
		reply.Ballot = bal
		reply.Message = common.ProposalError
		reply.LeaderID = nodeID
		return nil
	}

	if !executionRes.Result {
		txLogger.Warn("transaction proposal lacked quorum", slog.Int("node", nodeID), slog.Int("seq", seq))
		reply.RequestResult = false
		reply.Ballot = executionRes.Ballot
		reply.Message = executionRes.Message
		reply.LeaderID = nodeID
		return nil
	}

	txLogger.Info("transaction committed", slog.Int("node", nodeID), slog.Int("seq", seq))
	reply.RequestResult = true
	reply.Ballot = executionRes.Ballot
	reply.Message = executionRes.Message
	reply.LeaderID = nodeID
	return nil

}

func startElectionAsync(n *Node) {
	go func() {
		txLogger.Info("startup election triggered", slog.Int("node", n.NodeID))
		if ok, b, err := n.tryBecomeLeader(); err != nil {
			txLogger.Warn("startup election failed", slog.Int("node", n.NodeID), slog.Any("error", err))
		} else if ok {
			txLogger.Info("startup election succeeded", slog.Int("node", n.NodeID), slog.Any("ballot", b))
			go n.runHeartbeats()
		}
	}()
}
