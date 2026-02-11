package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"pranavpateriya.com/distributed-sys/common"
)

// ======== PREPARE RPC ==========
type PrepareReq struct {
	Ballot Ballot
}

type PrepareResp struct {
	Ok           bool
	Promised     Ballot
	Accepted     []AcceptedRecord
	LastExecuted int
}

type AcceptedRecord struct {
	Slot   int
	Ballot Ballot
	Entry  LogEntry
}

var errNotLeader = errors.New("not-leader")

// MPaxosPrepare lets a candidate ask for leadership promises (Phase 1).
func (n *Node) MPaxosPrepare(req *PrepareReq, resp *PrepareResp) error {
	if n.Status != common.StatusActive {
		slog.Info("prepare rejected: node stopped")
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		return nil
	}

	n.PaxosModule.mu.Lock()
	defer n.PaxosModule.mu.Unlock()

	if lessBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		slog.Info("prepare NACK", "reqBallot", req.Ballot, "active", n.PaxosModule.ActiveBallotNumber)
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		return nil
	}

	if moreBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		slog.Info("prepare update ballot", "old", n.PaxosModule.ActiveBallotNumber, "new", req.Ballot)
		n.PaxosModule.ActiveBallotNumber = req.Ballot
		n.PaxosModule.Role = common.RoleBackup
	}

	resp.Ok = true
	resp.Promised = n.PaxosModule.ActiveBallotNumber

	// collect accepted records
	n.PaxosModule.ReplicationLog.LogMutex.Lock()
	accepted := make([]AcceptedRecord, 0)
	for idx, entry := range n.PaxosModule.ReplicationLog.LogEntries {
		if entry == nil || entry.Status == common.StatusNoStatus {
			continue
		}
		accepted = append(accepted, AcceptedRecord{
			Slot:   idx,
			Ballot: entry.BallotNum,
			Entry:  *entry,
		})
	}
	lastExec := n.PaxosModule.ReplicationLog.LastExecuted
	n.PaxosModule.ReplicationLog.LogMutex.Unlock()

	resp.Accepted = accepted
	resp.LastExecuted = lastExec

	n.recordLeaderActivity()
	return nil
}

//********** ACCEPT RPC **********

//======== TYPES ===========

// <ACCEPT, b, s, P/A, m>
type Accept struct {
	Ballot      Ballot
	SequenceNum int
	SlotNumber  int

	ProtocolPart *ProtocolPart
	Txn          common.ClientReq
}

// ⟨ACCEPTED,b,s,m,nb⟩
type Accepted struct {
	Ok          bool
	Promised    Ballot // higher promised if NACK
	SequenceNum int

	Message       string
	ClientRequest common.ClientReq //TODOL re-evaluate if this is needed
	NodeRepliedID int
}

// ======== FUNCTION ==========
func (n *Node) MPaxosAccept(req *Accept, resp *Accepted) error {
	slog.Info("accept received", "seq", req.SequenceNum, "ballot", req.Ballot, "part", req.ProtocolPart)

	if n.Status != common.StatusActive {
		slog.Info("accept rejected: node stopped")
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		resp.SequenceNum = req.SequenceNum
		resp.Message = "node stopped"
		resp.ClientRequest = req.Txn
		resp.NodeRepliedID = n.NodeID
		return nil
	}

	n.PaxosModule.mu.Lock()
	defer n.PaxosModule.mu.Unlock()

	//check if the leader's ballot number is less than my ballot number, if yes then return NACK with my highest ballot number
	if lessBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		slog.Warn("reques ballot number is smaller than node's ballot number", "reqBallot", req.Ballot, "nodeBallot", n.PaxosModule.ActiveBallotNumber)
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		resp.SequenceNum = req.SequenceNum
		resp.Message = "lower ballot in req"
		return nil
	} else if moreBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		// Update promise (we promise at least this ballot)
		slog.Info("updated active ballot number", "from", n.PaxosModule.ActiveBallotNumber, "to", req.Ballot)
		n.PaxosModule.ActiveBallotNumber = req.Ballot
	}

	// Followers ensure records are free before accepting; lock them if free.
	// For commit/abort phases we must not re-lock, because the prepare phase
	// already holds the lock. Re-locking would NACK and stall the commit.
	lockNeeded := true
	if req.ProtocolPart != nil {
		switch *req.ProtocolPart {
		case TwoPCCommitted, TwoPCAborted:
			lockNeeded = false
		}
	}
	if lockNeeded && n.PaxosModule.Role != common.RoleLeader {
		tv := req.Txn.TranscationVal
		if !tryAddPairIfBothAbsent(tv.From, tv.To) {
			slog.Info("accept rejected: account locked", "from", tv.From, "to", tv.To)
			resp.Ok = false
			resp.Promised = n.PaxosModule.ActiveBallotNumber
			resp.SequenceNum = req.SequenceNum
			resp.Message = "account locked"
			resp.ClientRequest = req.Txn
			resp.NodeRepliedID = n.NodeID
			return nil
		}
	}

	//add the newly seen request to node transaction index
	n.PaxosModule.txnIndex.Add(req.Txn.TransactionID.TransactionID, req.SequenceNum)

	//extend logs if needed
	idx := req.SequenceNum
	if idx >= len(n.PaxosModule.ReplicationLog.LogEntries) {
		grow := idx - len(n.PaxosModule.ReplicationLog.LogEntries) + 1
		for i := 0; i < grow; i++ {
			n.PaxosModule.ReplicationLog.LogEntries = append(n.PaxosModule.ReplicationLog.LogEntries, GetEmptyLogEntry())
		}
	}

	entry := n.PaxosModule.ReplicationLog.LogEntries[idx]
	if entry == nil {
		entry = GetEmptyLogEntry()
		n.PaxosModule.ReplicationLog.LogEntries[idx] = entry
	}

	// mark which protocol phase/type this log entry represents
	twoPCStatus := Intrashard
	if req.ProtocolPart != nil {
		twoPCStatus = *req.ProtocolPart
	}

	entry.TxnVal = req.Txn.TranscationVal
	entry.TxID = req.Txn.TransactionID.TransactionID
	entry.IsNoOp = false //no op is only true in NewViewMessage, otherwise it's just gap
	entry.Status = common.StatusAccepted
	entry.BallotNum = req.Ballot
	entry.TwoPCSProtocolStage = twoPCStatus

	slog.Info("accept ok", "seq", req.SequenceNum, "part", twoPCStatus)
	resp.Ok = true
	resp.Promised = n.PaxosModule.ActiveBallotNumber
	resp.SequenceNum = req.SequenceNum
	resp.Message = "success"
	resp.ClientRequest = req.Txn
	resp.NodeRepliedID = n.NodeID

	slog.Info("sent accepted message")

	n.recordLeaderActivity()

	//TODO: bump on leader activity

	return nil
}

// runConsensusInstance encapsulates Accept+Commit for a single log slot.
func (n *Node) runConsensusInstance(txn common.ClientReq, part ProtocolPart) (int, error) {
	return n.runConsensusWithSeq(txn, part, -1)
}

// runConsensusWithSeq runs Accept+Commit, optionally targeting a specific sequence number.
// If seqOverride < 0, it picks the next slot; otherwise it uses the provided slot.
func (n *Node) runConsensusWithSeq(txn common.ClientReq, part ProtocolPart, seqOverride int) (int, error) {

	//responsibility to check if node n is active and is the leader(wheneve needed) lies with the called of runConsensusWithSeq

	peers := n.ClusterAddresses[n.ClusterID]

	//TODO: remove this check as it is redundant
	if len(peers) == 0 {
		return -1, fmt.Errorf("no peers configured")
	}

	quorum := QuorumSize()

	n.PaxosModule.mu.Lock()
	seqNum := seqOverride
	if seqNum < 0 {
		seqNum = len(n.PaxosModule.ReplicationLog.LogEntries)
		// Reserve the slot locally so concurrent proposals pick the next slot.
		n.PaxosModule.ReplicationLog.LogEntries = append(n.PaxosModule.ReplicationLog.LogEntries, GetEmptyLogEntry())
	}
	ballot := n.PaxosModule.ActiveBallotNumber
	n.PaxosModule.mu.Unlock()

	acceptReq := &Accept{
		Ballot:       ballot,
		SequenceNum:  seqNum,
		ProtocolPart: &part,
		Txn:          txn,
	}

	var nackMu sync.Mutex
	var sawHigher bool
	var highestPromised Ballot
	noteHigherBallot := func(promised Ballot) {
		if !moreBallot(promised, ballot) {
			return
		}
		nackMu.Lock()
		if !sawHigher || moreBallot(promised, highestPromised) {
			highestPromised = promised
		}
		sawHigher = true
		nackMu.Unlock()
	}

	accepted := 0
	var localAccepted Accepted
	if err := n.MPaxosAccept(acceptReq, &localAccepted); err == nil {
		if localAccepted.Ok {
			accepted++
		} else {
			noteHigherBallot(localAccepted.Promised)
		}
	}

	var wg sync.WaitGroup
	var acceptedCount int32 = int32(accepted)

	for _, peer := range peers {
		if peer == n.NodeAddress {
			continue
		}
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			client, err := n.rpcClient(p)
			if err != nil {
				slog.Warn("accept dial failed", "peer", p, "error", err)
				return
			}
			var resp Accepted
			if err := client.Call("Node.MPaxosAccept", acceptReq, &resp); err != nil {
				slog.Warn("accept rpc failed", "peer", p, "error", err)
				return
			}
			if resp.Ok {
				atomic.AddInt32(&acceptedCount, 1)
			} else {
				noteHigherBallot(resp.Promised)
			}
		}(peer)
	}
	wg.Wait()

	if sawHigher {
		n.adoptHigherBallot(highestPromised)
		return -1, fmt.Errorf("%w: higher ballot seen during accept %d/%d", errNotLeader, highestPromised.SequenceNumber, highestPromised.NodeID)
	}

	if int(acceptedCount) < quorum {
		return -1, fmt.Errorf("accept quorum not met")
	}

	commitReq := &Commited{
		Ballot:       ballot,
		SequenceNum:  seqNum,
		ProtocolPart: &part,
		Txn:          txn,
	}

	var localCommit CommitReply
	if err := n.MPaxosCommit(commitReq, &localCommit); err != nil {
		return -1, fmt.Errorf("local commit failed: %w", err)
	}
	if !localCommit.Ok {
		noteHigherBallot(localCommit.Promised)
	}

	wg = sync.WaitGroup{}
	for _, peer := range peers {
		if peer == n.NodeAddress {
			continue
		}
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			client, err := n.rpcClient(p)
			if err != nil {
				slog.Warn("commit dial failed", "peer", p, "error", err)
				return
			}
			var resp CommitReply
			if err := client.Call("Node.MPaxosCommit", commitReq, &resp); err != nil {
				slog.Warn("commit rpc failed", "peer", p, "error", err)
				return
			}
			if !resp.Ok {
				noteHigherBallot(resp.Promised)
			}
		}(peer)
	}
	wg.Wait()

	if sawHigher {
		n.adoptHigherBallot(highestPromised)
		return -1, fmt.Errorf("%w: higher ballot seen during commit %d/%d", errNotLeader, highestPromised.SequenceNumber, highestPromised.NodeID)
	}

	go executeLocked(n)

	return seqNum, nil
}

// runPaxosForTxn is a convenience wrapper that runs consensus for the given txn/phase.
// If seq < 0, a new slot is chosen.
func (n *Node) runPaxosForTxn(seq int, part ProtocolPart, txn common.ClientReq) (int, error) {
	return n.runConsensusWithSeq(txn, part, seq)
}

//==============================================================================

//**********COMMIT RPC **********

//======== TYPES ===========

// <ACCEPT, b, s, P/A, m>
type Commited struct {
	Ballot       Ballot
	SequenceNum  int
	ProtocolPart *ProtocolPart
	Txn          common.ClientReq
}

// ⟨ACCEPTED,b,s,m,nb⟩
type CommitReply struct {
	Ok            bool
	Promised      Ballot // higher promised if NACK
	SequenceNum   int
	Message       string
	NodeRepliedID int
}

// ======== FUNCTION ==========

func (n *Node) MPaxosCommit(req *Commited, resp *CommitReply) error {
	slog.Info("commit called", "seq num", req.SequenceNum, "ballot num", req.Ballot)

	if n.Status != common.StatusActive {
		slog.Info("commit rejected: node stopped")
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		resp.SequenceNum = req.SequenceNum
		resp.Message = "node stopped"
		resp.NodeRepliedID = n.NodeID
		return nil
	}

	n.PaxosModule.mu.Lock()
	defer n.PaxosModule.mu.Unlock()

	// Promise check: NACK if args.Ballot < Promised
	//this isn't really needed
	//check if the leader's ballot number is less than my ballot number, if yes then return NACK with my highest ballot number
	if lessBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		slog.Warn("reques ballot number is smaller than node's ballot number", "reqBallot", req.Ballot, "nodeBallot", n.PaxosModule.ActiveBallotNumber)
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		resp.SequenceNum = req.SequenceNum
		resp.Message = "lower ballot in req"
		return nil
	} else if moreBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		// Update promise (we promise at least this ballot)
		slog.Info("updated active ballot number", "from", n.PaxosModule.ActiveBallotNumber, "to", req.Ballot)
		n.PaxosModule.ActiveBallotNumber = req.Ballot
	}

	//extend logs if needed
	idx := req.SequenceNum
	if idx >= len(n.PaxosModule.ReplicationLog.LogEntries) {
		grow := idx - len(n.PaxosModule.ReplicationLog.LogEntries) + 1
		for i := 0; i < grow; i++ {
			n.PaxosModule.ReplicationLog.LogEntries = append(n.PaxosModule.ReplicationLog.LogEntries, GetEmptyLogEntry())
		}
	}

	entry := n.PaxosModule.ReplicationLog.LogEntries[idx]
	if entry == nil {
		entry = GetEmptyLogEntry()
		n.PaxosModule.ReplicationLog.LogEntries[idx] = entry
	}

	entry.TxnVal = req.Txn.TranscationVal
	entry.TxID = req.Txn.TransactionID.TransactionID
	entry.IsNoOp = false //no op is only true in NewViewMessage, otherwise it's just gap
	entry.Status = common.StatusCommitted
	entry.BallotNum = req.Ballot

	//TODO: Assignback again just for safety

	//if current node is not the leader then execute now, other wise the leader executes after sending commit messages
	if n.PaxosModule.Role != common.RoleLeader {
		slog.Info("follower executing after commit", "seq", req.SequenceNum)
		go executeLocked(n) // execution is on a seaparate thread
	}

	resp.Ok = true
	resp.Promised = n.PaxosModule.ActiveBallotNumber
	resp.SequenceNum = req.SequenceNum
	resp.Message = "commit success"
	resp.NodeRepliedID = n.NodeID

	slog.Info("sent commit reply", "seq", req.SequenceNum)

	n.recordLeaderActivity()

	//TODO: bump on leader activity

	return nil
}

// ==============================================================================
// ========= NEW VIEW RPC =========
func (n *Node) MPaxosNewView() error {
	return nil
}

// ======== HEARTBEAT / LIVENESS =========
type HeartbeatReq struct {
	Ballot   Ballot
	LeaderID int
}

type HeartbeatResp struct {
	Ok       bool
	Promised Ballot
}

type NewViewArgs struct {
	Ballot  Ballot
	Entries []*LogEntry
}

type NewViewReply struct {
	Ok       bool
	Promised Ballot
}

// ReceiveHeartbeat lets followers learn the current leader's ballot and refresh liveness.
func (n *Node) ReceiveHeartbeat(req *HeartbeatReq, resp *HeartbeatResp) error {
	if n.Status != common.StatusActive {
		// slog.Info("heartbeat ignored: node stopped")
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		return nil
	}

	n.PaxosModule.mu.Lock()
	if moreBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		n.PaxosModule.ActiveBallotNumber = req.Ballot
		n.PaxosModule.Role = common.RoleBackup
	}
	resp.Promised = n.PaxosModule.ActiveBallotNumber
	n.PaxosModule.mu.Unlock()

	n.recordLeaderActivity()
	// slog.Info("heartbeat received", "leader", req.LeaderID, "ballot", req.Ballot)
	resp.Ok = true
	return nil
}

func (n *Node) startLivenessLoops() {
	n.stopLivenessLoops()
	ctx, cancel := context.WithCancel(context.Background())
	n.heartbeatCancelMu.Lock()
	n.heartbeatCancel = cancel
	n.heartbeatCancelMu.Unlock()

	n.recordLeaderActivity()
	slog.Info("liveness loops started")
	go n.heartbeatSender(ctx)
	go n.heartbeatWatcher(ctx)
}

func (n *Node) stopLivenessLoops() {
	n.heartbeatCancelMu.Lock()
	if n.heartbeatCancel != nil {
		n.heartbeatCancel()
		n.heartbeatCancel = nil
	}
	n.heartbeatCancelMu.Unlock()
}

// heartbeatSender periodically notifies peers when this node is the leader.
func (n *Node) heartbeatSender(ctx context.Context) {
	interval := n.PaxosModule.HeartbeatTimer / 3
	if interval <= 0 {
		interval = 300 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.Status != common.StatusActive {
				continue
			}

			n.PaxosModule.mu.Lock()
			if n.PaxosModule.Role != common.RoleLeader {
				n.PaxosModule.mu.Unlock()
				continue
			}
			ballot := n.PaxosModule.ActiveBallotNumber
			n.PaxosModule.mu.Unlock()

			n.broadcastHeartbeat(ballot)
			n.recordLeaderActivity()
			// slog.Info("heartbeat sent", "ballot", ballot)
		}
	}
}

// heartbeatWatcher triggers elections if the leader is silent for too long.
func (n *Node) heartbeatWatcher(ctx context.Context) {
	interval := n.PaxosModule.HeartbeatTimer
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.Status != common.StatusActive {
				continue
			}

			if n.isLeader() {
				continue
			}

			last := atomic.LoadInt64(&n.lastHeartbeatUnix)
			if time.Since(time.Unix(0, last)) < interval {
				continue
			}

			// slog.Info("heartbeat timeout; starting election")
			n.startLeaderElection()
		}
	}
}

func (n *Node) isLeader() bool {
	n.PaxosModule.mu.Lock()
	defer n.PaxosModule.mu.Unlock()
	return n.PaxosModule.Role == common.RoleLeader
}

// startLeaderElection runs a simple Prepare-only election and self-promotes on quorum.
func (n *Node) startLeaderElection() {
	if n.Status != common.StatusActive {
		return
	}

	n.PaxosModule.mu.Lock()
	newBallot := n.PaxosModule.ActiveBallotNumber
	newBallot.SequenceNumber++
	newBallot.NodeID = n.NodeID
	n.PaxosModule.mu.Unlock()

	req := &PrepareReq{Ballot: newBallot}
	acks := 1 // count self
	highest := newBallot
	promises := []PrepareResp{{
		Ok:           true,
		Promised:     newBallot,
		Accepted:     n.collectAcceptedSnapshot(),
		LastExecuted: n.PaxosModule.ReplicationLog.LastExecuted,
	}}

	peers := n.ClusterAddresses[n.ClusterID]
	for _, peer := range peers {
		if peer == n.NodeAddress {
			continue
		}
		client, err := n.rpcClient(peer)
		if err != nil {
			slog.Warn("prepare dial failed", "peer", peer, "error", err)
			continue
		}

		var resp PrepareResp
		if err := client.Call("Node.MPaxosPrepare", req, &resp); err != nil {
			slog.Warn("prepare rpc failed", "peer", peer, "error", err)
			continue
		}
		if resp.Ok {
			acks++
			promises = append(promises, resp)
		} else if moreBallot(resp.Promised, highest) {
			highest = resp.Promised
		}
	}

	if acks >= QuorumSize() {
		mergedLog := n.buildNewViewFromPromises(promises, newBallot)
		n.installNewViewLocally(mergedLog, newBallot)

		n.PaxosModule.mu.Lock()
		n.PaxosModule.ActiveBallotNumber = newBallot
		n.PaxosModule.Role = common.RoleLeader
		n.PaxosModule.mu.Unlock()

		n.ClusterLeaders[n.ClusterID] = clusterLeader{NodeID: n.NodeID, Address: n.NodeAddress}
		n.pushNewViewToFollowers(mergedLog, newBallot)
		n.recordLeaderActivity()
		n.broadcastHeartbeat(newBallot)
		n.broadcastLeaderAnnouncement(newBallot)
		slog.Info("became leader", "cluster", n.ClusterID, "ballot", newBallot)
		return
	}

	n.PaxosModule.mu.Lock()
	if moreBallot(highest, n.PaxosModule.ActiveBallotNumber) {
		n.PaxosModule.ActiveBallotNumber = highest
	}
	n.PaxosModule.Role = common.RoleBackup
	n.PaxosModule.mu.Unlock()
}

func (n *Node) broadcastHeartbeat(ballot Ballot) {
	req := &HeartbeatReq{
		Ballot:   ballot,
		LeaderID: n.NodeID,
	}
	for _, peer := range n.ClusterAddresses[n.ClusterID] {
		if peer == n.NodeAddress {
			continue
		}
		client, err := n.rpcClient(peer)
		if err != nil {
			// slog.Warn("heartbeat dial failed", "peer", peer, "error", err)
			continue
		}
		var resp HeartbeatResp
		if err := client.Call("Node.ReceiveHeartbeat", req, &resp); err != nil {
			// slog.Warn("heartbeat rpc failed", "peer", peer, "error", err)
		}
	}
}

func (n *Node) broadcastCommit(peers []string, req *Commited) {
	for _, peer := range peers {
		if peer == n.NodeAddress {
			continue
		}
		client, err := n.rpcClient(peer)
		if err != nil {
			slog.Warn("commit dial failed", "peer", peer, "error", err)
			continue
		}
		var resp CommitReply
		if err := client.Call("Node.MPaxosCommit", req, &resp); err != nil {
			slog.Warn("commit rpc failed", "peer", peer, "error", err)
		}
	}
}

func (n *Node) adoptHigherBallot(promised Ballot) {
	n.PaxosModule.mu.Lock()
	if moreBallot(promised, n.PaxosModule.ActiveBallotNumber) {
		n.PaxosModule.ActiveBallotNumber = promised
	}
	n.PaxosModule.Role = common.RoleBackup
	n.PaxosModule.mu.Unlock()
}

func (n *Node) recordLeaderActivity() {
	atomic.StoreInt64(&n.lastHeartbeatUnix, time.Now().UnixNano())
}

func statusPriority(s common.SequenceStatus) int {
	switch s {
	case common.StatusExecuted:
		return 3
	case common.StatusCommitted:
		return 2
	case common.StatusAccepted:
		return 1
	default:
		return 0
	}
}

// collectAcceptedSnapshot gathers accepted entries with status present.
func (n *Node) collectAcceptedSnapshot() []AcceptedRecord {
	n.PaxosModule.ReplicationLog.LogMutex.Lock()
	defer n.PaxosModule.ReplicationLog.LogMutex.Unlock()

	accepted := make([]AcceptedRecord, 0)
	for idx, entry := range n.PaxosModule.ReplicationLog.LogEntries {
		if entry == nil || entry.Status == common.StatusNoStatus {
			continue
		}
		accepted = append(accepted, AcceptedRecord{
			Slot:   idx,
			Ballot: entry.BallotNum,
			Entry:  *entry,
		})
	}
	return accepted
}

// buildNewViewFromPromises merges accepted logs from promises selecting highest ballot per slot.
func (n *Node) buildNewViewFromPromises(promises []PrepareResp, ballot Ballot) []*LogEntry {
	type best struct {
		ballot Ballot
		entry  LogEntry
	}
	bestPerSlot := make(map[int]best)
	maxSlot := -1

	for _, pr := range promises {
		for _, rec := range pr.Accepted {
			if rec.Slot > maxSlot {
				maxSlot = rec.Slot
			}
			cur, ok := bestPerSlot[rec.Slot]
			if !ok || moreBallot(rec.Ballot, cur.ballot) || (rec.Ballot == cur.ballot && statusPriority(rec.Entry.Status) > statusPriority(cur.entry.Status)) {
				bestPerSlot[rec.Slot] = best{ballot: rec.Ballot, entry: rec.Entry}
			}
		}
	}

	if maxSlot < 0 {
		return []*LogEntry{}
	}

	out := make([]*LogEntry, maxSlot+1)
	for i := 0; i <= maxSlot; i++ {
		if cur, ok := bestPerSlot[i]; ok {
			// normalize entry with new ballot for this view
			entryCopy := cur.entry
			entryCopy.BallotNum = ballot
			if entryCopy.Status == common.StatusAccepted || entryCopy.Status == common.StatusNoStatus {
				entryCopy.Status = common.StatusCommitted
			}
			out[i] = &entryCopy
		} else {
			noop := GetEmptyLogEntry()
			noop.IsNoOp = true
			noop.BallotNum = ballot
			noop.Status = common.StatusAccepted
			out[i] = noop
		}
	}
	return out
}

// installNewViewLocally installs merged log entries and executes committed ones.
func (n *Node) installNewViewLocally(entries []*LogEntry, ballot Ballot) {
	logRef := &n.PaxosModule.ReplicationLog
	logRef.LogMutex.Lock()
	if len(entries) > len(logRef.LogEntries) {
		needed := len(entries) - len(logRef.LogEntries)
		for i := 0; i < needed; i++ {
			logRef.LogEntries = append(logRef.LogEntries, GetEmptyLogEntry())
		}
	}

	for idx, incoming := range entries {
		if incoming == nil {
			continue
		}
		existing := logRef.LogEntries[idx]
		if existing == nil {
			existing = GetEmptyLogEntry()
		}

		// if executed, prefer existing
		if existing.Status == common.StatusExecuted {
			logRef.LogEntries[idx] = existing
			continue
		}

		// choose stronger status
		status := incoming.Status
		if existing.Status == common.StatusCommitted && status != common.StatusExecuted {
			status = common.StatusCommitted
		}

		copyEntry := *incoming
		copyEntry.Status = status
		copyEntry.BallotNum = ballot
		logRef.LogEntries[idx] = &copyEntry
	}
	logRef.LogMutex.Unlock()

	// Execute committed entries to catch up.
	executeLocked(n)
}

// pushNewViewToFollowers sends NewView to peers to align logs.
func (n *Node) pushNewViewToFollowers(entries []*LogEntry, ballot Ballot) {
	peers := n.ClusterAddresses[n.ClusterID]
	for _, peer := range peers {
		if peer == n.NodeAddress {
			continue
		}
		addr := peer
		go func() {
			client, err := n.rpcClient(addr)
			if err != nil {
				slog.Warn("newview dial failed", "peer", addr, "error", err)
				return
			}
			var resp NewViewReply
			args := &NewViewArgs{Ballot: ballot, Entries: entries}
			if err := client.Call("Node.NewView", args, &resp); err != nil {
				slog.Warn("newview rpc failed", "peer", addr, "error", err)
				return
			}
			if !resp.Ok {
				slog.Warn("newview rejected", "peer", addr, "promised", resp.Promised)
			}
		}()
	}
}

// broadcastLeaderAnnouncement notifies all nodes in other clusters about new leader, this is best effort
func (n *Node) broadcastLeaderAnnouncement(ballot Ballot) {
	for cid, addrs := range n.ClusterAddresses {
		if cid == n.ClusterID {
			continue
		}
		for _, addr := range addrs {
			if addr == n.NodeAddress {
				continue
			}
			go func(peer string) {
				client, err := n.rpcClient(peer)
				if err != nil {
					slog.Warn("leader announce dial failed", "peer", peer, "error", err)
					return
				}
				req := &LeaderAnnounceReq{
					ClusterID: n.ClusterID,
					LeaderID:  n.NodeID,
					Address:   n.NodeAddress,
					Ballot:    ballot,
				}
				var resp LeaderAnnounceResp
				if err := client.Call("Node.LeaderAnnounce", req, &resp); err != nil {
					slog.Warn("leader announce rpc failed", "peer", peer, "error", err)
				}
			}(addr)
		}
	}
}

// ============= HELPER FUNC =============
// lessBallot tells if a is less than b Ballot Number
func lessBallot(a, b Ballot) bool {
	if a.SequenceNumber == b.SequenceNumber {
		return a.NodeID < b.NodeID
	} else {
		return a.SequenceNumber < b.SequenceNumber
	}
}

// moreBallot tells if a is greater than b Ballot Number
func moreBallot(a, b Ballot) bool {
	if a.SequenceNumber == b.SequenceNumber {
		return a.NodeID > b.NodeID
	} else {
		return a.SequenceNumber > b.SequenceNumber
	}
}

// IsNoOp is meant to signify if the leader does not have any info about the transaction in that slot
func GetEmptyLogEntry() *LogEntry {
	return &LogEntry{IsNoOp: false, Status: common.StatusNoStatus, TxnVal: common.TransactionVal{}, BallotNum: GetInitBallot(), ClientReply: common.ClientReply{}}
}

func GetInitBallot() Ballot {
	return Ballot{SequenceNumber: 0, NodeID: 0}
}

func accountInShard(acct int) bool {
	if GlobalConfig.AccountIDs == nil {
		return false
	}
	_, ok := GlobalConfig.AccountIDs[acct]
	return ok
}

func (n *Node) rpcClient(peer string) (*rpc.Client, error) {
	n.rpcMu.Lock()
	client := n.RPCClients[peer]
	n.rpcMu.Unlock()

	if client != nil {
		return client, nil
	}

	c, err := rpc.Dial("tcp", peer)
	if err != nil {
		return nil, err
	}

	n.rpcMu.Lock()
	n.RPCClients[peer] = c
	n.rpcMu.Unlock()
	return c, nil
}

// NewView installs the provided entries if ballot is not stale.
func (n *Node) NewView(req *NewViewArgs, resp *NewViewReply) error {
	if n.Status != common.StatusActive {
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		return nil
	}

	n.PaxosModule.mu.Lock()
	if lessBallot(req.Ballot, n.PaxosModule.ActiveBallotNumber) {
		resp.Ok = false
		resp.Promised = n.PaxosModule.ActiveBallotNumber
		n.PaxosModule.mu.Unlock()
		return nil
	}
	n.PaxosModule.ActiveBallotNumber = req.Ballot
	n.PaxosModule.Role = common.RoleBackup
	n.PaxosModule.mu.Unlock()

	logRef := &n.PaxosModule.ReplicationLog
	logRef.LogMutex.Lock()
	if len(req.Entries) > len(logRef.LogEntries) {
		needed := len(req.Entries) - len(logRef.LogEntries)
		for i := 0; i < needed; i++ {
			logRef.LogEntries = append(logRef.LogEntries, GetEmptyLogEntry())
		}
	}

	for idx, incoming := range req.Entries {
		if incoming == nil {
			continue
		}
		existing := logRef.LogEntries[idx]
		if existing == nil {
			existing = GetEmptyLogEntry()
		}
		if existing.Status == common.StatusExecuted {
			logRef.LogEntries[idx] = existing
			continue
		}

		status := incoming.Status
		if existing.Status == common.StatusCommitted && status != common.StatusExecuted {
			status = common.StatusCommitted
		}

		entryCopy := *incoming
		entryCopy.Status = status
		entryCopy.BallotNum = req.Ballot
		logRef.LogEntries[idx] = &entryCopy
	}
	logRef.LogMutex.Unlock()

	executeLocked(n)

	resp.Ok = true
	resp.Promised = req.Ballot
	return nil
}

// =========== EXECUTE ===========
func executeLocked(n *Node) {
	logRef := &n.PaxosModule.ReplicationLog

	// Ensure only one executor loop runs at a time on a node.
	logRef.ExecMutex.Lock()
	defer logRef.ExecMutex.Unlock()

	for {
		logRef.LogMutex.Lock()
		next := logRef.LastExecuted + 1
		if next >= len(logRef.LogEntries) {
			logRef.LogMutex.Unlock()
			return
		}
		entry := logRef.LogEntries[next]
		if entry == nil || entry.Status == common.StatusNoStatus {
			logRef.LogMutex.Unlock()
			return
		}
		if entry.Status == common.StatusAccepted {
			logRef.LogMutex.Unlock()
			return
		}
		if entry.Status == common.StatusExecuted {
			logRef.LastExecuted = next
			logRef.LogMutex.Unlock()
			continue
		}
		if entry.Status != common.StatusCommitted {
			logRef.LogMutex.Unlock()
			return
		}

		copyEntry := *entry
		logRef.LogMutex.Unlock()

		tv := copyEntry.TxnVal
		from := tv.From
		to := tv.To
		amt := tv.Amount
		txnID := copyEntry.TxID

		switch copyEntry.TwoPCSProtocolStage {
		case Intrashard:
			slog.Info("execute intrashard", "slot", next, "from", from, "to", to, "amt", amt)
			fromBal, err := n.PaxosModule.Bank.GetBalance(from)
			if err != nil {
				slog.Warn("intrashard balance read failed", "acct", from, "error", err)
			} else if fromBal < amt {
				slog.Warn("intrashard insufficient funds", "acct", from, "balance", fromBal, "amt", amt)
			} else {
				if err := n.PaxosModule.Bank.SetBalance(from, fromBal-amt); err != nil {
					slog.Warn("intrashard debit failed", "acct", from, "error", err)
				}
				if toBal, err := n.PaxosModule.Bank.GetBalance(to); err == nil {
					if err := n.PaxosModule.Bank.SetBalance(to, toBal+amt); err != nil {
						slog.Warn("intrashard credit failed", "acct", to, "error", err)
					}
				} else {
					slog.Warn("intrashard balance read failed", "acct", to, "error", err)
				}
			}
			unlockBoth(from, to)

		case TwoPCPrepared:
			slog.Info("execute 2pc prepare", "slot", next, "from", from, "to", to, "amt", amt)
			fromPresent := accountInShard(from)
			toPresent := accountInShard(to)

			var fromOld, toOld int
			if fromPresent {
				if bal, err := n.PaxosModule.Bank.GetBalance(from); err == nil {
					fromOld = bal
				} else {
					slog.Warn("wal prepare read failed", "acct", from, "error", err)
					fromPresent = false
				}
			}
			if toPresent {
				if bal, err := n.PaxosModule.Bank.GetBalance(to); err == nil {
					toOld = bal
				} else {
					slog.Warn("wal prepare read failed", "acct", to, "error", err)
					toPresent = false
				}
			}

			if n.TwoPCModule != nil && n.TwoPCModule.WalService != nil {
				if err := n.TwoPCModule.WalService.LogPrepare(txnID, fromPresent, fromOld, toPresent, toOld); err != nil {
					slog.Warn("wal log prepare failed", "txn", txnID, "error", err)
				} else {
					slog.Info("wal logged", "txn", txnID, "fromPresent", fromPresent, "toPresent", toPresent)
				}
			}

			if fromPresent {
				if err := n.PaxosModule.Bank.SetBalance(from, fromOld-amt); err != nil {
					slog.Warn("prepare balance update failed", "acct", from, "error", err)
				}
			}
			if toPresent {
				if err := n.PaxosModule.Bank.SetBalance(to, toOld+amt); err != nil {
					slog.Warn("prepare balance update failed", "acct", to, "error", err)
				}
			}

		case TwoPCCommitted:
			slog.Info("execute 2pc commit", "slot", next, "from", from, "to", to)
			unlockBoth(from, to)
			if n.TwoPCModule != nil && n.TwoPCModule.WalService != nil {
				if err := n.TwoPCModule.WalService.Clear(txnID); err != nil {
					slog.Warn("wal clear failed", "txn", txnID, "error", err)
				} else {
					slog.Info("wal cleared", "txn", txnID)
				}
			}

		case TwoPCAborted:
			slog.Info("execute 2pc abort", "slot", next, "from", from, "to", to)
			if n.TwoPCModule != nil && n.TwoPCModule.WalService != nil {
				if rec, err := n.TwoPCModule.WalService.Read(txnID); err == nil {
					if rec.FromPresent {
						if err := n.PaxosModule.Bank.SetBalance(from, rec.FromOldBalance); err != nil {
							slog.Warn("wal restore failed", "acct", from, "error", err)
						} else {
							slog.Info("wal restore", "acct", from, "old", rec.FromOldBalance)
						}
					}
					if rec.ToPresent {
						if err := n.PaxosModule.Bank.SetBalance(to, rec.ToOldBalance); err != nil {
							slog.Warn("wal restore failed", "acct", to, "error", err)
						} else {
							slog.Info("wal restore", "acct", to, "old", rec.ToOldBalance)
						}
					}
				} else {
					slog.Warn("wal read failed", "txn", txnID, "error", err)
				}
				if err := n.TwoPCModule.WalService.Clear(txnID); err != nil {
					slog.Warn("wal clear failed", "txn", txnID, "error", err)
				} else {
					slog.Info("wal cleared", "txn", txnID)
				}
			}
			unlockBoth(from, to)
		default:
			slog.Info("execute skipped for protocol part", "part", copyEntry.TwoPCSProtocolStage, "seq", next)
		}

		logRef.LogMutex.Lock()
		if logRef.LogEntries[next] != nil {
			logRef.LogEntries[next].Status = common.StatusExecuted
		}
		logRef.LastExecuted = next
		slog.Info("execute complete", "slot", next, "part", copyEntry.TwoPCSProtocolStage)
		logRef.LogMutex.Unlock()
	}
}

// ********************* LEADER CHANGE NOTIFICATION TO OTHER CLUSTERS *********************
// ===================== TYPE =====================
// LeaderAnnounceReq carries the new leader details to other clusters.
type LeaderAnnounceReq struct {
	ClusterID int
	LeaderID  int
	Address   string
	Ballot    Ballot
}

type LeaderAnnounceResp struct {
	Ok bool
}

// ===================== RPC =====================
// LeaderAnnounce is invoked by leaders of other clusters to update leader mappings.
func (n *Node) LeaderAnnounce(req *LeaderAnnounceReq, resp *LeaderAnnounceResp) error {
	// NOTE: Redundant check. Ignore announcements about our own cluster; local election state wins.
	if req.ClusterID != n.ClusterID {
		n.TwoPCModule.mu.Lock()
		defer n.TwoPCModule.mu.Unlock()
		n.ClusterLeaders[req.ClusterID] = clusterLeader{NodeID: req.LeaderID, Address: req.Address}
	}
	slog.Info("updated leader mapping", "updated lead of cluster", req.ClusterID, "new leader", req.LeaderID)
	resp.Ok = true
	return nil
}
