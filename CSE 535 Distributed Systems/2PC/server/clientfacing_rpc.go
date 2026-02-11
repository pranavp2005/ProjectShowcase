package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/rpc"
	"time"

	"pranavpateriya.com/distributed-sys/common"
	"pranavpateriya.com/distributed-sys/persistence"
)

//******************** TRANSACTIONS ***********************
//*********************************************************

// ======================== READ ONLY TRANSATION ========================
// ReadOnly returns the balance of the account number
// it checks for leader role and if the account is locked based on the consistency level
func (node *Node) ReadOnly(req *common.ClientReqConsLevel, resp *common.ClientReply) error {
	slog.Info("read-only request", "acct", req.ClientReq.TranscationVal.From, "consistency", req.ConsistencyLevel)

	//pre set request type as read beacuse that has to be set for all cases
	resp.RequestType = common.RequestTypeRead
	lockedAccount := 0
	defer func() {
		if lockedAccount != 0 {
			removeID(lockedAccount)
		}
	}()

	//if the node is stopped immediately send a NACK
	if node.Status == common.StatusStopped {
		slog.Info("node is stopped")
		resp.Status = false
		resp.Message = "node-stopped"
		return nil
	}

	//handle the request based on the selected consistency level
	switch req.ConsistencyLevel {
	case common.Eventual, common.Majority:
		//weakest, can send an intermediate balance as well do not check for locks just return the current balance
		slog.Info("eventual consistency level for read")

	// case common.Majority:
	// 	slog.Info("majority consistency level for read")

	// 	//can read from any node, just check if we have any in-flight requests which have exclusive lock on the object
	// 	if isLocked(req.ClientReq.TranscationVal.From) {
	// 		resp.Status = false
	// 		resp.Message = "account locked"
	// 		return nil
	// 	}

	case common.Linearizable:
		slog.Info("linearizable consistency level for read")

		// for linearizable consistency only leader replies
		if node.PaxosModule.Role != common.RoleLeader {
			resp.Status = false
			resp.Message = "not-leader"
			return nil
		}

		for !tryAddIfAbsent(req.ClientReq.TranscationVal.From) {
			time.Sleep(100 * time.Microsecond)
		}
		lockedAccount = req.ClientReq.TranscationVal.From
	}

	balance, err := node.PaxosModule.Bank.GetBalance(req.ClientReq.TranscationVal.From)
	if err != nil {
		slog.Error("error in fetching client balance", "acctID", req.ClientReq.TranscationVal.From, "error", err)
		resp.Status = false
		resp.Message = "db fetch error"
		return nil
	}

	resp.Status = true
	resp.Message = "success"
	resp.Balance = balance
	slog.Info("read-only success", "acct", req.ClientReq.TranscationVal.From, "balance", balance)

	return nil
}

// ======================== INTRA SHARD TRANSACTION ========================
// IntraShardTransfer RPC is called on the coordinator cluster's leader
// TODO: Implement Intrashard like normal mpaxos and add lock checks
func (node *Node) IntraShardTransfer(req *common.ClientReqConsLevel, resp *common.ClientReply) error {
	slog.Info("intrashard start", "from", req.ClientReq.TranscationVal.From, "to", req.ClientReq.TranscationVal.To, "amt", req.ClientReq.TranscationVal.Amount, "txn", req.ClientReq.TransactionID.TransactionID)

	resp.RequestType = common.RequestTypeInstraShardTransfer

	// 1. If the node is stopped then return immediately
	if node.Status == common.StatusStopped {
		slog.Info("intrashard rejected: node stopped")
		resp.Status = false
		resp.Message = "node-stopped"
		return nil
	}

	// TODO: decide on queuing behaviour

	// 2. Only leaders handle intrashard transactions
	if node.PaxosModule.Role != common.RoleLeader {
		slog.Info("intrashard rejected: not leader")
		resp.Status = false
		resp.Message = "not-leader"
		return nil
	}

	// 3. Check if this is a duplicate transaction from node logs
	if _, exists := node.PaxosModule.txnIndex.Get(req.ClientReq.TransactionID.TransactionID); exists {
		slog.Info("intrashard duplicate", "txn", req.ClientReq.TransactionID.TransactionID)
		resp.Status = true
		resp.Message = "duplicate txn"
		return nil
	}

	from := req.ClientReq.TranscationVal.From
	to := req.ClientReq.TranscationVal.To

	// 4. Check if any of the accounts involved are locked, if not then lock them
	if !tryAddPairIfBothAbsent(from, to) {
		slog.Info("intrashard lock failed", "from", from, "to", to)
		resp.Status = false
		resp.Message = "accounts locked skip"
		return nil
	}
	//both values sucessfully locked on the leader

	if _, err := node.runConsensusInstance(req.ClientReq, Intrashard); err != nil {
		slog.Warn("intrashard consensus failed", "error", err)
		unlockBoth(from, to)
		resp.Status = false
		if errors.Is(err, errNotLeader) {
			resp.Message = "not-leader"
		} else {
			resp.Message = err.Error()
		}
		return nil
	}

	resp.Status = true
	resp.Message = "success"
	slog.Info("intrashard success", "from", from, "to", to, "amt", req.ClientReq.TranscationVal.Amount)

	return nil
}

// ======================== CROSS SHARD TRANSACTION ========================
func (node *Node) CrossShardTransfer(req *common.ClientReqConsLevel, resp *common.ClientReply) error {
	slog.Info("cross-shard start", "from", req.ClientReq.TranscationVal.From, "to", req.ClientReq.TranscationVal.To, "amt", req.ClientReq.TranscationVal.Amount, "txn", req.ClientReq.TransactionID.TransactionID)
	resp.RequestType = common.RequestTypeCrossshardTransfer

	// 1) Basic checks
	if node.Status == common.StatusStopped {
		slog.Info("cross-shard rejected: node stopped")
		resp.Status = false
		resp.Message = "node-stopped"
		return nil
	}

	if node.PaxosModule.Role != common.RoleLeader {
		slog.Info("cross-shard rejected: not leader")
		resp.Status = false
		resp.Message = "not-leader"
		return nil
	}

	t := req.ClientReq.TranscationVal
	from := t.From
	to := t.To
	amt := t.Amount

	//TODO: clean this up, I assume that if I get a cross-shard request then this is a cross-shar transaction
	// 3) If this is actually intra-shard, delegate to the intrashard path.
	if node.clusterForAccount(from) == node.ClusterID && node.clusterForAccount(to) == node.ClusterID {
		slog.Error("Crossshard Transfer called when request is intrashard")
		resp.Status = false
		resp.Message = "wrong function call"
		return nil
	}

	// 4) Coordinator preconditions: lock the source and validate balance.
	if !tryAddIfAbsent(from) {
		slog.Info("cross-shard lock failed", "from", from)
		resp.Status = false
		resp.Message = "source locked skip"
		return nil
	}

	bal, err := node.PaxosModule.Bank.GetBalance(from)
	if err != nil || bal < amt {
		slog.Info("cross-shard rejected: insufficient funds", "from", from, "balance", bal, "amt", amt, "err", err)
		unlockBoth(from, to)
		resp.Status = false
		//TODO: Add skip/abort here
		resp.Message = "insufficient funds skip"
		return nil
	}

	participantCluster := node.clusterForAccount(to)
	leader, ok := node.ClusterLeaders[participantCluster]

	// 6) Coordinator PREPARE Paxos
	if _, err := node.runPaxosForTxn(-1, TwoPCPrepared, req.ClientReq); err != nil {
		slog.Warn("cross-shard prepare consensus failed", "error", err)
		if _, abortErr := node.runPaxosForTxn(-1, TwoPCAborted, req.ClientReq); abortErr != nil {
			slog.Warn("cross-shard abort consensus best-effort failed", "error", abortErr)
		}

		unlockBoth(from, to)
		resp.Status = false
		if errors.Is(err, errNotLeader) {
			resp.Message = "not-leader"
		} else {
			resp.Message = err.Error()
		}
		return nil
	}

	// 7) Send 2PC PREPARE to participant leader
	prepareOK := false
	if ok {
		prepareReq := &PrepareTpcReq{T: req.ClientReq}
		prepareCh := make(chan bool, 1)

		go func(addr string) {
			client, err := node.rpcClient(addr)
			if err != nil {
				prepareCh <- false
				return
			}
			var prepResp PrepareTpcResp
			if err := client.Call("Node.Prepare_2PC", prepareReq, &prepResp); err != nil {
				slog.Warn("coordinator send prepare failed", "peer", addr, "error", err)
				prepareCh <- false
				return
			}
			prepareCh <- prepResp.Ack
		}(leader.Address)

		timeout := node.TwoPCModule.CoordinatorTimer
		if timeout == 0 {
			timeout = 900 * time.Millisecond
		}

		select {
		case ok := <-prepareCh:
			prepareOK = ok
		case <-time.After(timeout):
			prepareOK = false
		}
	}

	// 8) Decision: commit or abort.
	if prepareOK {
		if _, err := node.runPaxosForTxn(-1, TwoPCCommitted, req.ClientReq); err != nil {
			slog.Warn("coordinator commit paxos failed", "error", err)
			resp.Status = false
			if errors.Is(err, errNotLeader) {
				resp.Message = "not-leader"
			} else {
				resp.Message = err.Error()
			}
			return nil
		}

		go node.sendFinalize(to, req.ClientReq, true)

		resp.Status = true
		resp.Message = "committed"
		slog.Info("cross-shard committed", "from", from, "to", to, "amt", amt)
		return nil
	}

	_, _ = node.runPaxosForTxn(-1, TwoPCAborted, req.ClientReq)
	go node.sendFinalize(to, req.ClientReq, false)

	resp.Status = false
	resp.Message = "abort"
	slog.Info("cross-shard aborted", "from", from, "to", to, "amt", amt)
	return nil
}

func (node *Node) GetAllBalances(req *common.GetAllAccountsReq, resp *common.GetAllAccountsResp) error {
	snapshot := node.PaxosModule.Bank.Snapshot()
	resp.Balances = make(map[int]int, len(snapshot))
	for acctID, balance := range snapshot {
		resp.Balances[acctID] = balance
	}
	resp.Status = true
	resp.Message = "ok"
	return nil
}

func (node *Node) ReshardNode(req *common.ResetBalanceReq, resp *common.ResetbalanceResp) error {
	// High-level goal: handle a "reshard" command for this node. This RPC will be called by a controller to move
	// accounts between shards and rebalance data.

	if req == nil || req.Balances == nil {
		resp.Status = false
		resp.Message = "invalid request"
		return nil
	}

	bank := node.PaxosModule.Bank
	// Fast path for BoltStore: clear and rewrite in one transaction to avoid thousands of per-account writes.
	if bs, ok := bank.(*persistence.BoltStore); ok {
		if err := bs.ReplaceAllBalances(req.Balances); err != nil {
			slog.Error("failed bulk replace during reshard", "error", err)
			resp.Status = false
			resp.Message = "db error"
			return nil
		}
		resp.Status = true
		resp.Message = "reshard completed"
		return nil
	}

	current := bank.Snapshot()

	for acct := range current {
		if _, keep := req.Balances[acct]; keep {
			continue
		}
		if err := bank.DeleteAccount(acct); err != nil {
			slog.Error("failed to delete account during reshard", "account", acct, "error", err)
		}
	}

	for acct, bal := range req.Balances {
		if err := bank.SetBalance(acct, bal); err != nil {
			slog.Error("failed to set balance during reshard", "account", acct, "error", err)
		}
	}

	resp.Status = true
	resp.Message = "reshard completed"
	return nil
}

func (node *Node) sendTwoPcPrepare(req *common.ClientReq) (result bool, message string) {
	if node.TwoPCModule == nil {
		slog.Warn("2pc prepare send failed: module not configured")
		return false, "2pc module not configured"
	}
	participantCluster := node.clusterForAccount(req.TranscationVal.To)
	leader, ok := node.ClusterLeaders[participantCluster]
	if !ok {
		slog.Warn("2pc prepare send failed: participant leader unknown", "to", req.TranscationVal.To)
		return false, "participant leader unknown"
	}

	client, err := rpc.Dial("tcp", leader.Address)
	if err != nil {
		slog.Warn("2pc prepare dial failed", "peer", leader.Address, "error", err)
		return false, fmt.Sprintf("dial failed: %v", err)
	}
	defer client.Close()

	c := make(chan struct {
		ok  bool
		msg string
	}, 1)

	go func() {
		var resp PrepareTpcResp
		if err := client.Call("Node.Prepare_2PC", &PrepareTpcReq{T: *req}, &resp); err != nil {
			c <- struct {
				ok  bool
				msg string
			}{false, err.Error()}
			return
		}
		c <- struct {
			ok  bool
			msg string
		}{resp.Ack, resp.Message}
	}()

	timeout := node.TwoPCModule.CoordinatorTimer
	if timeout == 0 {
		timeout = 900 * time.Millisecond
	}

	select {
	case r := <-c:
		slog.Info("2pc prepare response", "ok", r.ok, "msg", r.msg)
		return r.ok, r.msg
	case <-time.After(timeout):
		slog.Warn("2pc prepare timeout", "peer", leader.Address)
		return false, "participant timeout"
	}
}

func (node *Node) sendTwoPcAbort(req *common.ClientReq) (result bool, message string) {
	if node.TwoPCModule == nil {
		slog.Warn("2pc abort send failed: module not configured")
		return false, "2pc module not configured"
	}
	participantCluster := node.clusterForAccount(req.TranscationVal.To)
	leader, ok := node.ClusterLeaders[participantCluster]
	if !ok {
		slog.Warn("2pc abort send failed: participant leader unknown", "to", req.TranscationVal.To)
		return false, "participant leader unknown"
	}
	client, err := rpc.Dial("tcp", leader.Address)
	if err != nil {
		slog.Warn("2pc abort dial failed", "peer", leader.Address, "error", err)
		return false, fmt.Sprintf("dial failed: %v", err)
	}
	defer client.Close()
	var resp AbortTPCResp
	err = client.Call("Node.Abort_2PC", &AbortTPCReq{T: *req}, &resp)
	if err != nil {
		slog.Warn("2pc abort rpc failed", "peer", leader.Address, "error", err)
		return false, err.Error()
	}
	slog.Info("2pc abort response", "ok", resp.Ack, "msg", resp.Message)
	return resp.Ack, resp.Message
}

func (node *Node) sendTwoPcCommit(req *common.ClientReq) (result bool, message string) {
	if node.TwoPCModule == nil {
		slog.Warn("2pc commit send failed: module not configured")
		return false, "2pc module not configured"
	}
	participantCluster := node.clusterForAccount(req.TranscationVal.To)
	leader, ok := node.ClusterLeaders[participantCluster]
	if !ok {
		slog.Warn("2pc commit send failed: participant leader unknown", "to", req.TranscationVal.To)
		return false, "participant leader unknown"
	}
	client, err := rpc.Dial("tcp", leader.Address)
	if err != nil {
		slog.Warn("2pc commit dial failed", "peer", leader.Address, "error", err)
		return false, fmt.Sprintf("dial failed: %v", err)
	}
	defer client.Close()
	var resp CommitTPCResp
	err = client.Call("Node.Commit_2PC", &CommitTPCReq{T: *req}, &resp)
	if err != nil {
		slog.Warn("2pc commit rpc failed", "peer", leader.Address, "error", err)
		return false, err.Error()
	}
	slog.Info("2pc commit response", "ok", resp.Ack, "msg", resp.Message)
	return resp.Ack, resp.Message
}

// sendFinalize best-effort sends the final 2PC decision (commit/abort) to the participant leader.
func (node *Node) sendFinalize(to int, txn common.ClientReq, commit bool) {
	participantCluster := node.clusterForAccount(to)
	leader, ok := node.ClusterLeaders[participantCluster]
	if !ok {
		slog.Warn("finalize skipped: participant leader unknown", "to", to)
		return
	}

	client, err := node.rpcClient(leader.Address)
	if err != nil {
		slog.Warn("finalize dial failed", "peer", leader.Address, "error", err)
		return
	}

	req := &FinalizeTPCReq{T: txn, Commit: commit}
	slog.Info("sending finalize", "to", to, "commit", commit)
	done := make(chan struct{}, 1)
	go func() {
		var resp FinalizeTPCResp
		_ = client.Call("Node.Finalize_2PC", req, &resp)
		done <- struct{}{}
	}()

	timeout := node.TwoPCModule.CoordinatorTimer
	if timeout == 0 {
		timeout = 900 * time.Millisecond
	}

	select {
	case <-done:
		slog.Info("finalize completed", "to", to, "commit", commit)
	case <-time.After(timeout):
		slog.Warn("finalize timeout", "to", to, "commit", commit)
	}
}

// ******************** STATUS FUNCTION ****************
// *****************************************************
func (node *Node) Fail(req *common.NodeControlReq, resp *common.NodeControlResp) error {
	node.Status = common.StatusStopped
	node.stopLivenessLoops()
	resp.Status = true
	resp.Message = "node-stopped"
	return nil
}

func (node *Node) Recover(req *common.NodeControlReq, resp *common.NodeControlResp) error {
	node.Status = common.StatusActive
	node.PaxosModule.Role = common.RoleBackup

	node.startLivenessLoops()
	resp.Status = true
	resp.Message = "node recovered"
	return nil
}

func (node *Node) RecoverWithRole(req *common.RecoverWithRoleReq, resp *common.NodeControlResp) error {
	desiredRole := node.ConfigRole
	if req != nil {
		desiredRole = req.Role
	}

	node.Status = common.StatusActive

	node.PaxosModule.mu.Lock()
	node.PaxosModule.Role = desiredRole
	if desiredRole == common.RoleLeader {
		node.PaxosModule.ActiveBallotNumber.NodeID = node.NodeID
		if node.PaxosModule.ActiveBallotNumber.SequenceNumber == 0 {
			node.PaxosModule.ActiveBallotNumber.SequenceNumber = 1
		}
	}
	node.PaxosModule.mu.Unlock()

	node.startLivenessLoops()
	resp.Status = true
	resp.Message = "node recovered with role"
	return nil
}

//******************** INFO FUNCTION ****************
//*****************************************************

func (node *Node) PrintView(req *common.PrintViewReq, resp *common.PrintViewResp) error {
	resp.Status = true
	resp.Message = "ok"
	resp.ViewMessages = make([]common.ViewMessage, 0)
	return nil
}

func (node *Node) PrintStatus(req *common.NodeStatusReq, resp *common.NodeStatusResp) error {
	role := node.PaxosModule.Role
	resp.Status = true
	resp.Message = "ok"
	resp.NodeID = node.NodeID
	resp.ClusterID = node.ClusterID
	resp.Role = role
	resp.NodeState = node.Status
	return nil
}

func (node *Node) PrintBalance(req *common.PrintBalanceReq, resp *common.PrintBalanceResp) error {
	if node.Status == common.StatusStopped {
		resp.Status = false
		resp.Message = "node-stopped"
		return nil
	}

	if req == nil {
		resp.Status = false
		resp.Message = "invalid request"
		return nil
	}

	balance, err := node.PaxosModule.Bank.GetBalance(req.AccountID)
	if err != nil {
		slog.Error("error fetching balance", "acctID", req.AccountID, "error", err)
		resp.Status = false
		resp.Message = "db fetch error"
		return nil
	}

	resp.Status = true
	resp.Message = "success"
	resp.Balance = balance
	return nil
}

func (node *Node) PrintLog(req *common.PrintLogReq, resp *common.PrintLogResp) error {
	if node.PaxosModule == nil {
		resp.Status = false
		resp.Message = "paxos module not initialized"
		return nil
	}

	logRef := &node.PaxosModule.ReplicationLog
	logRef.LogMutex.Lock()
	defer logRef.LogMutex.Unlock()

	resp.Log = make([]common.PaxosLogEntry, 0, len(logRef.LogEntries))
	for idx, entry := range logRef.LogEntries {
		if entry == nil {
			resp.Log = append(resp.Log, common.PaxosLogEntry{
				Index:  idx,
				Status: common.StatusNoStatus,
				Ballot: common.PaxosBallot{},
			})
			continue
		}

		resp.Log = append(resp.Log, common.PaxosLogEntry{
			Index:       idx,
			IsNoOp:      entry.IsNoOp,
			TxnVal:      entry.TxnVal,
			Status:      entry.Status,
			TwoPCStatus: string(entry.TwoPCSProtocolStage),
			Ballot: common.PaxosBallot{
				SequenceNumber: entry.BallotNum.SequenceNumber,
				NodeID:         entry.BallotNum.NodeID,
			},
		})
	}

	resp.Status = true
	resp.Message = "ok"

	return nil
}

func (node *Node) PrintLogSlot(req *common.PrintLogSlotReq, resp *common.PrintLogSlotResp) error {
	if node.PaxosModule == nil {
		resp.Status = false
		resp.Message = "paxos module not initialized"
		return nil
	}

	logRef := &node.PaxosModule.ReplicationLog
	logRef.LogMutex.Lock()
	defer logRef.LogMutex.Unlock()

	if req.Slot < 0 || req.Slot >= len(logRef.LogEntries) {
		resp.Status = false
		resp.Message = "slot out of range"
		return nil
	}

	entry := logRef.LogEntries[req.Slot]
	out := common.PaxosLogEntry{
		Index:  req.Slot,
		Status: common.StatusNoStatus,
		Ballot: common.PaxosBallot{},
	}
	if entry != nil {
		out.IsNoOp = entry.IsNoOp
		out.TxnVal = entry.TxnVal
		out.Status = entry.Status
		out.TwoPCStatus = string(entry.TwoPCSProtocolStage)
		out.Ballot = common.PaxosBallot{
			SequenceNumber: entry.BallotNum.SequenceNumber,
			NodeID:         entry.BallotNum.NodeID,
		}
	}

	resp.Status = true
	resp.Message = "ok"
	resp.Entry = out
	return nil
}

func (node *Node) clusterForAccount(acct int) int {
	clusterID, ok := node.AccountIDCluster[acct]
	if !ok {
		return 0
	}
	return clusterID
}
