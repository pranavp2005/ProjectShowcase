// file: leader.go
package main

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"pranavpateriya.com/multipaxos/common"
)

// Attempt to become leader: Phase 1 (Prepare -> Promises) then send NEW-VIEW.
// Returns (true, ballot) if leadership established.
func (n *Node) tryBecomeLeader() (bool, Ballot, error) {
	// Build next ballot: strictly higher than promised.
	n.mu.Lock()
	base := n.Promised
	b := Ballot{N: base.N + 1, NodeID: n.NodeID}
	// nextSlot := n.Log.ExecutedIndex + 1
	peers := append([]string(nil), n.Peers...)
	me := n.Address
	n.mu.Unlock()
	LogProtocol(n.NodeID, "Election attempt started: ballot=%+v", b)

	oks := 1 // count self (we "promise" to ourselves)
	var promises []Promise

	// Self "promise"
	selfAcc := n.collectAcceptedFromStart()
	promises = append(promises, Promise{
		Ok:        true,
		Promised:  b,
		AccpetLog: selfAcc,
	})

	// Send Prepare to peers
	type nack struct{ promised Ballot }
	var highestNack *nack

	var wgErr error
	var okCountCh = make(chan bool, len(peers))
	expected := 0
	for _, p := range peers {
		//we've already counted our vote
		if p == me {
			continue
		}
		expected++
		go func(addr string) {
			LogProtocol(n.NodeID, "Prepare sent: to=%s ballot=%+v", addr, b)
			rep, err := n.callPrepare(addr, &PrepareArgs{Ballot: b})
			if err != nil {
				LogProtocol(n.NodeID, "Prepare error: to=%s err=%v", addr, err)
				okCountCh <- false
				return
			}
			if rep.Ok {
				n.bumpOnLeaderActivity() // to prevent ovelapping elections from self
				n.mu.Lock()
				promises = append(promises, *rep)
				n.mu.Unlock()
				LogProtocol(n.NodeID, "Promise received: from=%s ballot=%+v accepted=%d", addr, rep.Promised, len(rep.AccpetLog))
				okCountCh <- true
			} else {
				n.mu.Lock()
				if highestNack == nil || highestNack.promised.Less(&rep.Promised) {
					highestNack = &nack{promised: rep.Promised}
				}
				n.mu.Unlock()
				LogProtocol(n.NodeID, "Promise NACK received: from=%s promised=%+v", addr, rep.Promised)
				okCountCh <- false
			}
		}(p)
	}

	timeout := time.After(2 * n.t)
WaitPrepare:
	for i := 0; i < expected; i++ {
		select {
		case ok := <-okCountCh:
			if ok {
				oks++
			}
		case <-timeout:
			wgErr = fmt.Errorf("prepare quorum timeout")
			LogProtocol(n.NodeID, "Prepare timeout waiting for promises")
			break WaitPrepare
		}
	}

	if wgErr != nil {
		return false, b, wgErr
	}
	if oks < n.quorumSize() {
		// If someone promised higher, step aside.
		if highestNack != nil && highestNack.promised.GreaterEq(&b) {
			n.mu.Lock()
			if n.Promised.Less(&highestNack.promised) {
				n.Promised = highestNack.promised
			}
			n.mu.Unlock()
			LogProtocol(n.NodeID, "Election aborted: higher ballot seen %+v", highestNack.promised)
		}
		return false, b, fmt.Errorf("no quorum on Prepare: oks=%d", oks)
	}

	// Merge AcceptLogs recieved in promise -> make NEW-VIEW entries at our ballot.
	entries := n.buildNewViewEntriesFromPromises(promises, b)
	LogProtocol(n.NodeID, "NewView entries prepared: count=%d", len(entries))

	newViewOks := 1 // self
	// ========================
	// Self-accept optimistically (no RPC)
	selfReply := &NewViewReply{}
	LogProtocol(n.NodeID, "NewView sent: to=%s entries=%d ballot=%+v", me, len(entries), b)
	consensusLogger.Info("NewView", slog.String("message:", fmt.Sprintf("NewView sent: to=%s entries=%d ballot=%+v", me, len(entries), b)))
	if err := (&ElectionRPC{node: n}).NewView(&NewViewArgs{
		FromLeader: n.NodeID,
		Ballot:     b,
		Entries:    entries,
	}, selfReply); err != nil || !selfReply.Ok {
		LogProtocol(n.NodeID, "NewView local failed response not ok: from=%s err=%v ok=%v promised=%+v", me, err, selfReply.Ok, selfReply.Promised)
		return false, b, fmt.Errorf("NEW-VIEW accept for local failed: oks=%d", 0)
	}

	// ========================
	// Push NEW-VIEW to followers
	okCh := make(chan bool, len(peers))
	expectedNewView := 0
	for _, p := range peers {
		if p == me {
			continue
		}
		expectedNewView++
		go func(addr string) {
			client, err := dialOnce(addr, 800*time.Millisecond)
			if err != nil {
				LogProtocol(n.NodeID, "NewView send failed: to=%s err=%v", addr, err)
				okCh <- false
				return
			}
			defer client.Close()
			var rep NewViewReply
			LogProtocol(n.NodeID, "NewView sent: to=%s entries=%d ballot=%+v", addr, len(entries), b)
			err = client.Call("ElectionRPC.NewView",
				&NewViewArgs{FromLeader: n.NodeID, Ballot: b, Entries: entries}, &rep)
			if err != nil || !rep.Ok {
				LogProtocol(n.NodeID, "NewView response not ok: from=%s err=%v ok=%v promised=%+v", addr, err, rep.Ok, rep.Promised)
				okCh <- false
				return
			}
			LogProtocol(n.NodeID, "NewView acknowledgement received: from=%s accepted=%d already=%d", addr, len(rep.AcceptedSeqs), len(rep.AlreadyHadSeq))
			okCh <- true
		}(p)
	}
	timeout2 := time.After(2 * n.t)
WaitNewView:
	for i := 0; i < expectedNewView; i++ {
		select {
		case ok := <-okCh:
			if ok {
				newViewOks++
			}
		case <-timeout2:
			break WaitNewView
		}
	}

	if newViewOks < n.quorumSize() {
		LogProtocol(n.NodeID, "Election failed: insufficient NewView acknowledgements oks=%d", newViewOks)
		return false, b, fmt.Errorf("no quorum on NEW-VIEW: oks=%d", newViewOks)
	}

	// We are leader now.
	n.mu.Lock()
	n.Role = common.RoleLeader
	n.SelfBallot = b
	// Advance SequenceNumber to end of normalized view
	maxIdx := -1
	for i := range entries {
		if entries[i] != nil && i > maxIdx {
			maxIdx = i
		}
	}
	if maxIdx+1 > n.SequenceNumber {
		n.SequenceNumber = maxIdx + 1
	}
	n.mu.Unlock()

	// Adopt recovered entries locally and broadcast commits so the cluster converges.
	if len(entries) > 0 {
		n.mu.Lock()

		// Ensure the log is large enough to hold recovered entries.
		needed := len(entries) - len(n.Log.LogEntries)
		for i := 0; i < needed; i++ {
			n.Log.LogEntries = append(n.Log.LogEntries, GetEmptyLogEntry())
		}

		for idx, entry := range entries {
			if entry == nil {
				slog.Error("empty log entry in newview message this should not happen", slog.Any("list", entries))
				continue
			}

			local := n.Log.LogEntries[idx]
			if local == nil {
				local = GetEmptyLogEntry()
				n.Log.LogEntries[idx] = local
			}

			if local.Status == common.StatusExecuted {
				if local.TxnVal.TransactionID == entry.TxnVal.TransactionID {
					local.BallotNum = entry.BallotNum
					n.Log.LogEntries[idx] = local
				}
			} else {
				local.IsNoOp = entry.IsNoOp
				local.TxnVal = entry.TxnVal
				local.Status = common.StatusCommitted
				local.BallotNum = entry.BallotNum
				n.Log.LogEntries[idx] = local
			}
		}
		n.mu.Unlock()

		// Execute the recovered entries locally.


		for idx, entry := range entries {
			if entry == nil {
				continue
			}
			n.broadcastCommit(n.Address, idx, entry.TxnVal, b, 800*time.Millisecond)
		}

		//try broadcast and then execute
		LogProtocol(n.NodeID, "Leader execute after new view")
		execute(n)

	}

	LogProtocol(n.NodeID, "Election success: became leader ballot=%+v seqNumber=%d", b, n.SequenceNumber)

	return true, b, nil
}

func (n *Node) callPrepare(addr string, args *PrepareArgs) (*Promise, error) {
	client, err := dialOnce(addr, 800*time.Millisecond)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	var rep Promise
	if err := client.Call("ElectionRPC.Prepare", args, &rep); err != nil {
		return nil, err
	}
	return &rep, nil
}

func (n *Node) broadcastCommit(self string, seq int, txReq common.TransactionReq, bal Ballot, timeout time.Duration) {
	peers := append([]string(nil), n.Peers...)
	var wg sync.WaitGroup
	for _, addr := range peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if addr == n.Address {
				var rep CommitReply
				_ = (&ConsensusRPC{node: n}).Commit(&CommitArgs{SequenceNumber: seq, BallotNumber: bal, ClientRequest: txReq}, &rep)
				LogProtocol(n.NodeID, "Commit local reply: seq=%d status=%v", seq, rep.Status)
				return
			}
			LogProtocol(n.NodeID, "Commit sent: to=%s seq=%d ballot=%+v", addr, seq, bal)

			client, err := dialOnce(addr, timeout)
			if err != nil {
				LogProtocol(n.NodeID, "Commit send failed: to=%s seq=%d err=%v", addr, seq, err)
				consensusLogger.Error("commit send failed", slog.Int("node", n.NodeID), slog.String("peer", addr), slog.Int("seq", seq), slog.Any("error", err))
				return
			}
			defer client.Close()
			var rep CommitReply
			_ = client.Call("ConsensusRPC.Commit", &CommitArgs{SequenceNumber: seq, BallotNumber: bal, ClientRequest: txReq}, &rep)
			LogProtocol(n.NodeID, "Commit reply received: from=%s seq=%d status=%v", addr, seq, rep.Status)
		}(addr)
	}
	wg.Wait()
}

// collect our AcceptedRecords
func (n *Node) collectAcceptedFromStart() []AcceptLog {
	n.mu.Lock()
	defer n.mu.Unlock()
	var acc []AcceptLog

	for slot := 0; slot < len(n.Log.LogEntries); slot++ {
		e := n.Log.LogEntries[slot]
		if e == nil {
			continue
		}
		switch e.Status {
		case common.StatusAccepted, common.StatusCommitted, common.StatusExecuted:
			acc = append(acc, AcceptLog{
				Slot:   slot,
				Ballot: e.BallotNum,
				Value:  e.TxnVal,
			})
		default:
			// skip gaps
		}
	}
	return acc
}

// Merge promises into a normalized NEW-VIEW at ballot b, filling gaps with no-ops.
// for the leader mark the status as accepted, so that we don't need a separate self accept
func (n *Node) buildNewViewEntriesFromPromises(promises []Promise, b Ballot) []*LogEntry {
	type best struct {
		ballot Ballot
		val    common.TransactionReq
	}
	bestPerSlot := map[int]best{}
	maxSlot := -1

	for _, pr := range promises {
		for _, rec := range pr.AccpetLog {
			slot := int(rec.Slot)
			if slot > maxSlot {
				maxSlot = slot
			}
			cur, ok := bestPerSlot[slot]
			if !ok || cur.ballot.Less(&rec.Ballot) {
				bestPerSlot[slot] = best{ballot: rec.Ballot, val: rec.Value}
			}
		}
	}

	if maxSlot < 0 {
		return []*LogEntry{}
	}

	out := make([]*LogEntry, maxSlot+1)
	for s := 0; s <= maxSlot; s++ {
		if cur, ok := bestPerSlot[s]; ok {
			out[s] = &LogEntry{Status: common.StatusAccepted, BallotNum: b, TxnVal: cur.val}
		} else {
			emptyLog := GetEmptyLogEntry()

			emptyLog.IsNoOp = true
			emptyLog.TxnVal = common.TransactionReq{}
			emptyLog.BallotNum = b
			emptyLog.Status = common.StatusAccepted
			out[s] = emptyLog
		}
	}
	return out
}

// Heartbeat loop (leader only). Keeps backups' timers fresh.
func (n *Node) runHeartbeats() {
	ticker := time.NewTicker(n.hbInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		if n.Status != common.StatusActive || n.Role != common.RoleLeader {
			n.mu.Unlock()
			return
		}
		b := n.SelfBallot
		peers := append([]string(nil), n.Peers...)
		me := n.Address
		n.mu.Unlock()

		// If we recently sent ACCEPT/COMMIT, those also act as heartbeats; still fine to send lightweight pings.
		for _, p := range peers {
			if p == me {
				continue
			}
			go func(addr string) {
				client, err := dialOnce(addr, 600*time.Millisecond)
				if err != nil {
					return
				}
				defer client.Close()
				var rep HeartbeatReply
				_ = client.Call("ElectionRPC.Heartbeat",
					&HeartbeatArgs{Ballot: b, LeaderID: n.NodeID}, &rep)
			}(p)
		}
		<-ticker.C
	}
}
