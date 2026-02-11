package server

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"sort"
	"time"

	"pranavpateriya.com/bft/common"
)

const (
	maxViewChangeBackoffMultiplier = 4
)

// InitLivenessState prepares timer/view-change state for a freshly created node.
func (node *Node) InitLivenessState() {
	if node == nil {
		return
	}
	if node.pendingRequests == nil {
		node.pendingRequests = make(map[string]struct{})
	}
	if node.viewChangeCache == nil {
		node.viewChangeCache = make(map[int]map[int]*ViewChangeSigned)
	}
	if node.recoveryViewProofs == nil {
		node.recoveryViewProofs = make(map[int]map[int]PrepareProof)
	}
	if node.viewChangeSent == nil {
		node.viewChangeSent = make(map[int]bool)
	}
	if node.higherViewVotes == nil {
		node.higherViewVotes = make(map[int]map[int]struct{})
	}
	if node.newViewODigest == nil {
		node.newViewODigest = make(map[int]map[int]Digest)
	}
	if node.ViewChangeTimeout <= 0 {
		node.ViewChangeTimeout = 5 * time.Second
	}
	node.nextViewChangeTimeout = node.ViewChangeTimeout
	if node.pendingViewNumber == 0 {
		node.pendingViewNumber = node.CurrentView
		if node.pendingViewNumber == 0 {
			node.pendingViewNumber = 1
		}
	}
}

func (node *Node) prepareViewChangeLocked(reason string) (*ViewChangeSigned, []string) {
	return node.prepareViewChangeForLocked(node.CurrentView+1, reason)
}

func (node *Node) initiateViewChangeUnlocked(targetView int) {
	if node == nil {
		return
	}

	if targetView <= 0 {
		targetView = 1
	}

	node.lock()
	if targetView <= node.CurrentView {
		node.unlock()
		return
	}

	node.ensureViewChangeStateLocked()
	if node.viewChangeSent[targetView] {
		node.unlock()
		return
	}

	oldPending := node.pendingViewNumber
	if oldPending == 0 {
		oldPending = node.CurrentView
	}

	node.ViewChangeInProgress = true
	node.pendingViewNumber = targetView
	node.viewChangeSent[targetView] = true

	slog.Info("pending view bump", "nodeID", node.NodeID, "oldView", oldPending, "newView", node.pendingViewNumber, "reason", "initiate-view-change")

	pset := node.buildPrepareSetLocked()
	viewChange := ViewChange{
		NewViewNumber:        targetView,
		LastStableCheckpoint: node.LastStableCheckpoint,
		Pset:                 pset,
		FromReplica:          node.NodeID,
	}

	sig, err := SignStruct(viewChange, node.PrivateKey)
	if err != nil {
		slog.Error("failed to sign view change", "nodeID", node.NodeID, "view", targetView, "error", err)
		node.unlock()
		return
	}

	signed := &ViewChangeSigned{
		ViewChange: viewChange,
		Signature:  sig,
	}

	node.storeViewChangeLocked(signed)
	node.stopReplicaTimerLocked("initiate-view-change")
	peers := append([]string(nil), node.Peers...)
	peers = append(peers, node.Address)
	node.unlock()

	node.broadcastViewChange(signed, peers)
}

func (node *Node) prepareViewChangeForLocked(targetView int, reason string) (*ViewChangeSigned, []string) {
	if targetView <= node.CurrentView {
		targetView = node.CurrentView + 1
	}

	node.ensureViewChangeStateLocked()
	if node.viewChangeSent[targetView] {
		return nil, nil
	}

	vc, err := node.buildViewChangeMessageLocked(targetView)
	if err != nil {
		slog.Error("build view change failed", "nodeID", node.NodeID, "view", targetView, "error", err)
		return nil, nil
	}

	node.viewChangeSent[targetView] = true
	if targetView > node.pendingViewNumber {
		oldPending := node.pendingViewNumber
		if oldPending == 0 {
			oldPending = node.CurrentView
		}
		node.pendingViewNumber = targetView
		slog.Info("pending view bump", "nodeID", node.NodeID, "oldView", oldPending, "newView", node.pendingViewNumber, "reason", reason)
	}
	node.ViewChangeInProgress = true
	node.stopReplicaTimerLocked("view-change-prepared")
	node.bumpViewChangeBackoffLocked()
	node.storeViewChangeLocked(vc)
	node.registerHigherViewVoteLocked(targetView, node.NodeID)

	peers := append([]string(nil), node.Peers...)
	return vc, peers
}

func (node *Node) ensureViewChangeStateLocked() {
	if node.viewChangeCache == nil {
		node.viewChangeCache = make(map[int]map[int]*ViewChangeSigned)
	}
	if node.recoveryViewProofs == nil {
		node.recoveryViewProofs = make(map[int]map[int]PrepareProof)
	}
	if node.viewChangeSent == nil {
		node.viewChangeSent = make(map[int]bool)
	}
	if node.higherViewVotes == nil {
		node.higherViewVotes = make(map[int]map[int]struct{})
	}
	if node.pendingRequests == nil {
		node.pendingRequests = make(map[string]struct{})
	}
}

func (node *Node) bumpViewChangeBackoffLocked() {
	base := node.ViewChangeTimeout
	if base <= 0 {
		base = 5 * time.Second
	}
	if node.nextViewChangeTimeout <= 0 {
		node.nextViewChangeTimeout = base
		return
	}
	next := node.nextViewChangeTimeout * 2
	maxTimeout := base * maxViewChangeBackoffMultiplier
	if next > maxTimeout {
		next = maxTimeout
	}
	node.nextViewChangeTimeout = next
}

func (node *Node) resetViewChangeBackoffLocked() {
	base := node.ViewChangeTimeout
	if base <= 0 {
		base = 5 * time.Second
	}
	node.nextViewChangeTimeout = base
}

func (node *Node) buildViewChangeMessageLocked(targetView int) (*ViewChangeSigned, error) {
	low := node.LastStableCheckpoint
	high := node.LastStableCheckpoint + node.CheckpointInterval
	proofs := make(PrepareSet)

	for seq := low + 1; seq < len(node.Log.LogEntries) && seq <= high; seq++ {
		entry := node.Log.LogEntries[seq]
		if entry == nil || isEmptyLogEntry(entry) {
			continue
		}
		if entry.Status != StatusPrepared && entry.Status != StatusCommitted && entry.Status != StatusExecuted {
			continue
		}
		if len(entry.PrepareCert) == 0 || entry.PrePrepareCert.PPSignature == nil {
			continue
		}
		proofs[seq] = PrepareProof{
			PrePrepareMessage: PrePrepareMessage{
				PrePrepareSig: clonePrePrepareSigned(entry.PrePrepareCert),
				Transaction:   entry.Txn,
			},
			Prepares: clonePrepareCerts(entry.PrepareCert),
		}
	}

	vc := ViewChange{
		NewViewNumber:        targetView,
		LastStableCheckpoint: node.LastStableCheckpoint,
		Pset:                 proofs,
		FromReplica:          node.NodeID,
	}

	sig, err := SignStruct(vc, node.PrivateKey)
	if err != nil {
		return nil, err
	}

	return &ViewChangeSigned{
		ViewChange: vc,
		Signature:  sig,
	}, nil
}

func (node *Node) storeViewChangeLocked(vc *ViewChangeSigned) {
	view := vc.ViewChange.NewViewNumber
	from := vc.ViewChange.FromReplica

	node.ensureViewChangeStateLocked()

	cache := node.viewChangeCache[view]
	if cache == nil {
		cache = make(map[int]*ViewChangeSigned)
		node.viewChangeCache[view] = cache
	}
	cache[from] = cloneViewChange(vc)

	proofMap := node.recoveryViewProofs[view]
	if proofMap == nil {
		proofMap = make(map[int]PrepareProof)
		node.recoveryViewProofs[view] = proofMap
	}
	for seq, proof := range vc.ViewChange.Pset {
		existing, ok := proofMap[seq]
		if !ok || betterPrepareProof(proof, existing) {
			proofMap[seq] = clonePrepareProof(proof)
		}
	}
}

func cloneViewChange(vc *ViewChangeSigned) *ViewChangeSigned {
	result := &ViewChangeSigned{
		ViewChange: ViewChange{
			NewViewNumber:        vc.ViewChange.NewViewNumber,
			LastStableCheckpoint: vc.ViewChange.LastStableCheckpoint,
			Pset:                 make(PrepareSet),
			FromReplica:          vc.ViewChange.FromReplica,
		},
		Signature: append([]byte(nil), vc.Signature...),
	}
	for seq, proof := range vc.ViewChange.Pset {
		result.ViewChange.Pset[seq] = clonePrepareProof(proof)
	}
	return result
}

func betterPrepareProof(candidate PrepareProof, existing PrepareProof) bool {
	newView := candidate.PrePrepareMessage.PrePrepareSig.PrePrepare.ViewNumber
	oldView := existing.PrePrepareMessage.PrePrepareSig.PrePrepare.ViewNumber
	if newView != oldView {
		return newView > oldView
	}
	return bytes.Compare(
		candidate.PrePrepareMessage.PrePrepareSig.PrePrepare.Digest[:],
		existing.PrePrepareMessage.PrePrepareSig.PrePrepare.Digest[:],
	) > 0
}

func (node *Node) broadcastViewChange(vc *ViewChangeSigned, peers []string) {
	if vc == nil {
		return
	}
	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	for _, addr := range peers {
		if addr == "" || addr == node.Address {
			continue
		}

		go func(target string) {
			node.warnIfMuHeld("dial-view-change")
			conn, err := net.DialTimeout("tcp", target, timeout)
			if err != nil {
				slog.Error("dial peer for view change failed", "nodeID", node.NodeID, "peer", target, "error", err)
				return
			}
			defer conn.Close()

			client := rpc.NewClient(conn)
			defer client.Close()

			var reply ViewChangeReply
			if err := client.Call("Node.ViewChange", vc, &reply); err != nil {
				slog.Error("view change RPC failed", "nodeID", node.NodeID, "peer", target, "error", err)
			}
		}(addr)
	}
}

func (node *Node) broadcastNewView(nv *NewViewSigned, peers []string) {
	if nv == nil {
		return
	}

	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	for _, addr := range peers {
		if addr == "" {
			continue
		}
		go func(target string) {
			node.warnIfMuHeld("dial-new-view")
			conn, err := net.DialTimeout("tcp", target, timeout)
			if err != nil {
				slog.Error("dial peer for new view failed", "nodeID", node.NodeID, "peer", target, "error", err)
				return
			}
			defer conn.Close()

			client := rpc.NewClient(conn)
			defer client.Close()

			var reply NewViewReply
			if err := client.Call("Node.NewView", nv, &reply); err != nil {
				slog.Error("new view RPC failed", "nodeID", node.NodeID, "peer", target, "error", err)
			}
		}(addr)
	}
}

func (node *Node) registerHigherViewVoteLocked(view int, replica int) int {
	node.ensureViewChangeStateLocked()

	votes := node.higherViewVotes[view]
	if votes == nil {
		votes = make(map[int]struct{})
		node.higherViewVotes[view] = votes
	}
	votes[replica] = struct{}{}
	return len(votes)
}

func (node *Node) applyNewViewLocked(newView NewView) {
	oldView := node.CurrentView
	node.CurrentView = newView.NewViewNumber
	node.pendingViewNumber = newView.NewViewNumber
	node.ViewChangeInProgress = false
	node.Role = RoleReplica
	if node.NodeID == leaderForView(newView.NewViewNumber) {
		node.Role = RoleLeader
		node.syncLeaderSequenceLocked()
	}
	node.resetViewChangeBackoffLocked()
	node.stopReplicaTimerLocked("entered-new-view")
	slog.Info("current view updated", "nodeID", node.NodeID, "oldView", oldView, "newView", node.CurrentView, "reason", "new-view-adopted")
	node.recordNewViewHistoryLocked(newView)

	for view := range node.viewChangeCache {
		if view <= newView.NewViewNumber {
			delete(node.viewChangeCache, view)
			delete(node.recoveryViewProofs, view)
			delete(node.higherViewVotes, view)
		}
	}
}

func (node *Node) recordNewViewHistoryLocked(newView NewView) {
	record := NewViewRecord{
		View:              newView.NewViewNumber,
		Primary:           newView.Primary,
		ViewChangeSenders: append([]int(nil), newView.ViewChangeSenders...),
		IncludedProofs:    make(map[int]string, len(newView.ProofDigests)),
		Time:              time.Now(),
	}
	for seq, digest := range newView.ProofDigests {
		record.IncludedProofs[seq] = digest
	}
	node.ViewChangeHistory = append(node.ViewChangeHistory, record)
}

func (node *Node) syncLeaderSequenceLocked() {
	if node == nil || node.Role != RoleLeader {
		return
	}

	highest := node.HigestSequenceNumber
	logEntries := node.Log.LogEntries
	for seq := len(logEntries) - 1; seq >= 0; seq-- {
		entry := logEntries[seq]
		if entry == nil || isEmptyLogEntry(entry) {
			continue
		}
		if seq > highest {
			highest = seq
		}
		break
	}

	if highest > node.HigestSequenceNumber {
		slog.Info("leader sequence sync", "nodeID", node.NodeID, "oldSeq", node.HigestSequenceNumber, "newSeq", highest)
		node.HigestSequenceNumber = highest
	}
}

func (node *Node) replayProofsAsLeader(view int, proofs map[int]PrepareProof) {
	if len(proofs) == 0 {
		return
	}

	seqs := make([]int, 0, len(proofs))
	for seq := range proofs {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)

	for _, seq := range seqs {
		proof := proofs[seq]
		digest := proof.PrePrepareMessage.PrePrepareSig.PrePrepare.Digest
		txn := proof.PrePrepareMessage.Transaction

		if err := node.replaySingleProof(view, seq, txn, digest); err != nil {
			slog.Error("replay prepared request failed", "nodeID", node.NodeID, "view", view, "seq", seq, "error", err)
		}
	}
}

func (node *Node) replaySingleProof(view int, seq int, txn common.ClientReq, digest Digest) error {
	node.lock()
	if seq >= len(node.Log.LogEntries) {
		for len(node.Log.LogEntries) <= seq {
			node.Log.LogEntries = append(node.Log.LogEntries, nil)
		}
	}
	entry := node.Log.LogEntries[seq]
	if entry == nil {
		entry = &LogEntry{}
	}
	entry.ViewNumber = view
	entry.Txn = txn
	entry.Digest = digest
	entry.Status = StatusPrePrepared
	node.Log.LogEntries[seq] = entry
	node.Log.ClientRequestIndexLock.Lock()
	node.Log.ClientRequestIndex[txn.TransactionID.UniqueID] = seq
	node.Log.ClientRequestIndexLock.Unlock()
	node.unlock()

	prePrepare := PrePrepare{
		ViewNumber:  view,
		SequenceNum: seq,
		Digest:      digest,
	}
	payload, err := json.Marshal(prePrepare)
	if err != nil {
		return err
	}
	ppSig := generateSignatureHelper(node.PrivateKey, payload, node.InvalidSignatureAttackFlag)
	msg := PrePrepareMessage{
		PrePrepareSig: PrePrepareSigned{
			PrePrepare:  prePrepare,
			PPSignature: ppSig,
		},
		Transaction: txn,
	}

	return node.executeOrderingForPrePrepare(msg, digest, false)
}

func (node *Node) executeOrderingForPrePrepare(msg PrePrepareMessage, digest Digest, useNewViewRPC bool) error {
	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	targetPeers := getPeerlistExcludingSelf(node.Address, node.Peers, false, nil)
	digestBytes := append([]byte(nil), digest[:]...)

	f := (getTotalNodes() - 1) / 3
	requiredPrepare := 2 * f

	prepareMatches := make(map[int]int)
	prepareCertificates := make(map[int][]PrepareSigned)
	preprepareMethod := "Node.PrePrepare"
	dialLabel := "dial-preprepare"
	if useNewViewRPC {
		preprepareMethod = "Node.PrePrepareNewView"
		dialLabel = "dial-preprepare-newview"
	}

	for _, addr := range targetPeers {
		node.warnIfMuHeld(dialLabel)
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for replay pre-prepare failed", "nodeID", node.NodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)

		var prepareResp PrepareSigned
		callErr := client.Call(preprepareMethod, &msg, &prepareResp)
		client.Close()
		if callErr != nil {
			slog.Error("pre-prepare RPC (replay) failed", "nodeID", node.NodeID, "peer", addr, "error", callErr)
			continue
		}

		resp := prepareResp.PrePrepareResponse
		if !resp.Ack ||
			resp.ViewNumber != msg.PrePrepareSig.PrePrepare.ViewNumber ||
			resp.SequenceNumber != msg.PrePrepareSig.PrePrepare.SequenceNum ||
			!bytes.Equal(resp.Digest[:], digestBytes) {
			continue
		}

		pub := node.getPublicKey(resp.NodeID)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}
		payload, err := json.Marshal(resp)
		if err != nil {
			continue
		}
		if !ed25519.Verify(pub, payload, prepareResp.PRSignature) {
			continue
		}

		prepareCertificates[resp.SequenceNumber] = append(prepareCertificates[resp.SequenceNumber], clonePrepareSigned(prepareResp))
		prepareMatches[resp.SequenceNumber]++
	}

	seq := msg.PrePrepareSig.PrePrepare.SequenceNum
	if prepareMatches[seq] < requiredPrepare {
		return fmt.Errorf("insufficient prepares for seq %d", seq)
	}

	node.lock()
	if seq < len(node.Log.LogEntries) && node.Log.LogEntries[seq] != nil {
		entry := node.Log.LogEntries[seq]
		entry.Status = StatusPrepared
		entry.PrepareCert = clonePrepareCerts(prepareCertificates[seq])
		entry.PrePrepareCert = clonePrePrepareSigned(msg.PrePrepareSig)
		node.Log.LogEntries[seq] = entry
	}
	node.unlock()

	preparedMsg := PreparedMessage{
		ViewNumber:     msg.PrePrepareSig.PrePrepare.ViewNumber,
		SequenceNumber: seq,
		Digest:         digestBytes,
		NodeID:         node.NodeID,
	}
	payload, err := json.Marshal(preparedMsg)
	if err != nil {
		return err
	}
	preparedSig := generateSignatureHelper(node.PrivateKey, payload, node.InvalidSignatureAttackFlag)
	preparedSigned := PreparedMessageSigned{
		PrepareMessage: preparedMsg,
		PrepareMsgSign: preparedSig,
		PrepareCert:    clonePrepareCerts(prepareCertificates[seq]),
	}

	requiredCommit := 2 * f
	commitMatches := make(map[int]int)
	commitCertBundle := make([]CommitSigned, 0, requiredCommit+1)
	commitCertNodes := make(map[int]struct{})

	for _, addr := range targetPeers {
		node.warnIfMuHeld("dial-prepared")
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for replay prepared failed", "nodeID", node.NodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)

		var commitResp CommitSigned
		callErr := client.Call("Node.Prepared", &preparedSigned, &commitResp)
		client.Close()
		if callErr != nil {
			slog.Error("prepared RPC (replay) failed", "nodeID", node.NodeID, "peer", addr, "error", callErr)
			continue
		}

		resp := commitResp.Commit
		if !resp.Ack ||
			resp.ViewNumber != preparedMsg.ViewNumber ||
			resp.SequenceNumber != seq ||
			!bytes.Equal(resp.Digest, digestBytes) {
			continue
		}
		pub := node.getPublicKey(resp.NodeID)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}
		payload, err := json.Marshal(resp)
		if err != nil {
			continue
		}
		if !ed25519.Verify(pub, payload, commitResp.CommitSignature) {
			continue
		}
		if _, exists := commitCertNodes[resp.NodeID]; !exists {
			commitCertNodes[resp.NodeID] = struct{}{}
			commitCertBundle = append(commitCertBundle, cloneCommitSigned(commitResp))
		}
		commitMatches[seq]++
	}

	if commitMatches[seq] < requiredCommit {
		return fmt.Errorf("insufficient commits for seq %d", seq)
	}

	selfCommit := Commit{
		Ack:            true,
		ViewNumber:     preparedMsg.ViewNumber,
		SequenceNumber: seq,
		Digest:         append([]byte(nil), digestBytes...),
		NodeID:         node.NodeID,
	}
	payload, err = json.Marshal(selfCommit)
	if err != nil {
		return err
	}
	selfSig := generateSignatureHelper(node.PrivateKey, payload, node.InvalidSignatureAttackFlag)
	commitCertBundle = append(commitCertBundle, CommitSigned{
		Commit:          selfCommit,
		CommitSignature: selfSig,
	})

	node.lock()
	if seq < len(node.Log.LogEntries) && node.Log.LogEntries[seq] != nil {
		entry := node.Log.LogEntries[seq]
		entry.Status = StatusCommitted
		node.Log.LogEntries[seq] = entry
	}
	node.unlock()

	committedMsg := Committed{
		ViewNumber:     preparedMsg.ViewNumber,
		SequenceNumber: seq,
		Digest:         digestBytes,
		NodeID:         node.NodeID,
	}
	committedPayload, err := json.Marshal(committedMsg)
	if err != nil {
		return err
	}
	commitMsgSig := generateSignatureHelper(node.PrivateKey, committedPayload, node.InvalidSignatureAttackFlag)
	committed := CommitedSigned{
		Committed:       committedMsg,
		CommitSignature: commitMsgSig,
		CommitCert:      commitCertBundle,
	}

	for _, addr := range targetPeers {
		node.warnIfMuHeld("dial-commit")
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for replay commit failed", "nodeID", node.NodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)
		var reply CommitedReply
		callErr := client.Call("Node.Commited", &committed, &reply)
		client.Close()
		if callErr != nil {
			slog.Error("commit RPC (replay) failed", "nodeID", node.NodeID, "peer", addr, "error", callErr)
		}
	}

	var selfReply CommitedReply
	node.lock()
	err = node.commitedLocked(&committed, &selfReply)
	node.unlock()
	return err
}
