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

	"pranavpateriya.com/bft/common"
)

type newViewDecision struct {
	txn       common.ClientReq
	digest    Digest
	preparedV int
	isNoOp    bool
}

type newViewPlan struct {
	view         int
	startSeq     int
	endSeq       int
	decisions    map[int]newViewDecision
	senders      []int
	proofDigests map[int]string
}

func (node *Node) NewView(req *NewViewSigned, resp *NewViewReply) error {
	if req == nil || resp == nil {
		return nil
	}

	node.lock()

	resp.ViewNumber = node.CurrentView
	view := req.NewView.NewViewNumber
	primary := req.NewView.Primary

	if view < node.CurrentView+1 {
		node.unlock()
		resp.Ack = false
		resp.Reason = "stale-view"
		return nil
	}

	if leaderForView(view) != primary {
		node.unlock()
		resp.Ack = false
		resp.Reason = "incorrect-primary"
		return nil
	}

	pub := node.getPublicKey(primary)
	if len(pub) != ed25519.PublicKeySize {
		node.unlock()
		resp.Ack = false
		resp.Reason = "unknown-primary"
		return nil
	}

	valid, err := verifyStruct(req.NewView, req.Signature, pub)
	if err != nil || !valid {
		node.unlock()
		resp.Ack = false
		resp.Reason = "invalid-signature"
		return nil
	}

	node.ingestNewViewProofsLocked(req.NewView.ViewChanges)

	plan, ok := node.computeNewViewPlanLocked(view)
	if !ok {
		node.unlock()
		resp.Ack = false
		resp.Reason = "insufficient-proofs"
		return nil
	}

	if !compareProofDigests(plan.proofDigests, req.NewView.ProofDigests) {
		node.unlock()
		resp.Ack = false
		resp.Reason = "digest-mismatch"
		go node.initiateViewChangeUnlocked(view + 1)
		return nil
	}

	if node.newViewODigest == nil {
		node.newViewODigest = make(map[int]map[int]Digest)
	}
	node.newViewODigest[view] = make(map[int]Digest, len(plan.decisions))
	for seq, decision := range plan.decisions {
		node.newViewODigest[view][seq] = decision.digest
	}

	node.applyNewViewLocked(req.NewView)
	resp.Ack = true
	resp.Reason = "accepted"
	node.unlock()

	return nil
}

func (node *Node) tryConstructAndBroadcastNewView(view int) {
	if node == nil || leaderForView(view) != node.NodeID {
		return
	}

	node.lock()
	plan, ok := node.computeNewViewPlanLocked(view)
	if !ok {
		node.unlock()
		return
	}

	newView := NewView{
		NewViewNumber:        view,
		Primary:              node.NodeID,
		LastStableCheckpoint: node.LastStableCheckpoint,
		ProofDigests:         plan.proofDigests,
		ViewChangeSenders:    append([]int(nil), plan.senders...),
		ViewChanges:          node.collectViewChangesLocked(view, plan.senders),
	}

	sig, err := SignStruct(newView, node.PrivateKey)
	if err != nil {
		node.unlock()
		slog.Error("failed to sign NewView", "nodeID", node.NodeID, "view", view, "error", err)
		return
	}

	nvSigned := &NewViewSigned{
		NewView:   newView,
		Signature: sig,
	}

	if node.newViewODigest == nil {
		node.newViewODigest = make(map[int]map[int]Digest)
	}
	node.newViewODigest[view] = make(map[int]Digest, len(plan.decisions))
	for seq, decision := range plan.decisions {
		node.newViewODigest[view][seq] = decision.digest
	}
	node.unlock()

	acks := node.broadcastNewViewAndCollectAcks(nvSigned)
	required := 2*((getTotalNodes()-1)/3) + 1
	if acks < required {
		slog.Warn("insufficient new-view acknowledgements", "nodeID", node.NodeID, "view", view, "acks", acks, "required", required)
		return
	}

	go node.broadcastPrePrepareNewView(plan)
}

func (node *Node) computeNewViewPlanLocked(view int) (newViewPlan, bool) {
	var plan newViewPlan

	node.ensureViewChangeStateLocked()
	cache := node.viewChangeCache[view]
	required := 2*((getTotalNodes()-1)/3) + 1
	if len(cache) < required {
		return plan, false
	}

	senders := make([]int, 0, len(cache))
	viewChanges := make([]*ViewChangeSigned, 0, len(cache))
	for id, vc := range cache {
		senders = append(senders, id)
		viewChanges = append(viewChanges, cloneViewChange(vc))
	}
	sort.Ints(senders)

	firstPending := 0
	foundPending := false
	for seq := 0; seq < len(node.Log.LogEntries); seq++ {
		entry := node.Log.LogEntries[seq]
		if entry == nil || isEmptyLogEntry(entry) || entry.Status != StatusExecuted {
			firstPending = seq
			foundPending = true
			break
		}
	}
	if !foundPending {
		firstPending = len(node.Log.LogEntries)
	}

	startSeq := firstPending
	minSeq := -1
	maxSeq := -1
	for _, vc := range viewChanges {
		for seq := range vc.ViewChange.Pset {
			if seq < startSeq {
				continue
			}
			if minSeq == -1 || seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}
	}
	if minSeq == -1 || minSeq > startSeq {
		minSeq = startSeq
	}
	if maxSeq < minSeq {
		maxSeq = minSeq
	}

	decisions := make(map[int]newViewDecision, maxSeq-minSeq+1)
	proofDigests := make(map[int]string, maxSeq-minSeq+1)

	for seq := minSeq; seq <= maxSeq; seq++ {
		decision, ok := chooseBestPrepared(viewChanges, seq)
		if !ok {
			txn := buildNoOpRequest(view, seq)
			decision = newViewDecision{
				txn:       txn,
				digest:    DigestOfTransaction(txn),
				preparedV: view,
				isNoOp:    true,
			}
		}
		decisions[seq] = decision
		proofDigests[seq] = digestToHex(decision.digest)
	}

	plan = newViewPlan{
		view:         view,
		startSeq:     minSeq,
		endSeq:       maxSeq,
		decisions:    decisions,
		senders:      senders,
		proofDigests: proofDigests,
	}
	return plan, true
}

func chooseBestPrepared(viewChanges []*ViewChangeSigned, seq int) (newViewDecision, bool) {
	bestView := -1
	var bestDigest Digest
	var bestTxn common.ClientReq
	found := false

	for _, vc := range viewChanges {
		proof, ok := vc.ViewChange.Pset[seq]
		if !ok {
			continue
		}
		proofView := proof.PrePrepareMessage.PrePrepareSig.PrePrepare.ViewNumber
		digest := proof.PrePrepareMessage.PrePrepareSig.PrePrepare.Digest

		switch {
		case !found:
			found = true
		case proofView < bestView:
			continue
		case proofView == bestView && bytes.Compare(digest[:], bestDigest[:]) <= 0:
			continue
		}

		bestView = proofView
		bestDigest = digest
		bestTxn = cloneClientReq(proof.PrePrepareMessage.Transaction)
	}

	if !found {
		return newViewDecision{}, false
	}

	return newViewDecision{
		txn:       bestTxn,
		digest:    bestDigest,
		preparedV: bestView,
		isNoOp:    bestTxn.TransactionVal.TransactionType == common.RequestNoOp,
	}, true
}

func (node *Node) broadcastNewViewAndCollectAcks(nv *NewViewSigned) int {
	if nv == nil {
		return 0
	}

	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	targets := getPeerlistIncludingSelf(node.Address, node.Peers, node.InDarkAttackFlag, node.InDarkTargets)
	if len(targets) == 0 {
		targets = []string{node.Address}
	}

	required := 2*((getTotalNodes()-1)/3) + 1
	acks := 0
	for _, addr := range targets {
		if addr == node.Address {
			var reply NewViewReply
			if err := node.NewView(nv, &reply); err == nil && reply.Ack {
				acks++
				slog.Info("new-view ack progress", "nodeID", node.NodeID, "view", nv.NewView.NewViewNumber, "acks", acks, "needed", required, "source", "self")
			}
			continue
		}

		node.warnIfMuHeld("dial-new-view")
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for new-view failed", "nodeID", node.NodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)

		var reply NewViewReply
		callErr := client.Call("Node.NewView", nv, &reply)
		client.Close()
		if callErr != nil {
			slog.Error("new-view RPC failed", "nodeID", node.NodeID, "peer", addr, "error", callErr)
			continue
		}
		if reply.Ack {
			acks++
			slog.Info("new-view ack progress", "nodeID", node.NodeID, "view", nv.NewView.NewViewNumber, "acks", acks, "needed", required, "source", addr)
		}
	}
	return acks
}

func (node *Node) broadcastPrePrepareNewView(plan newViewPlan) {
	for seq := plan.startSeq; seq <= plan.endSeq; seq++ {
		decision, ok := plan.decisions[seq]
		if !ok {
			continue
		}
		if err := node.driveNewViewDecision(plan.view, seq, decision); err != nil {
			slog.Error("new-view decision failed", "view", plan.view, "seq", seq, "error", err)
		}
	}
}

func compareProofDigests(expected, actual map[int]string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for seq, digest := range expected {
		if act, ok := actual[seq]; !ok || act != digest {
			return false
		}
	}
	return true
}

func buildNoOpRequest(view, seq int) common.ClientReq {
	return common.ClientReq{
		TransactionVal: common.TransactionVal{
			TransactionType: common.RequestNoOp,
		},
		TransactionID: common.TransactionID{
			UniqueID: fmt.Sprintf("noop-%d-%d", view, seq),
			ClientID: fmt.Sprintf("noop-%d", view),
		},
	}
}

func cloneClientReq(src common.ClientReq) common.ClientReq {
	dst := common.ClientReq{
		TransactionID: src.TransactionID,
		TransactionVal: common.TransactionVal{
			TransactionType: src.TransactionVal.TransactionType,
		},
	}

	if src.TransactionVal.TransactionTransfer != nil {
		copyTransfer := *src.TransactionVal.TransactionTransfer
		dst.TransactionVal.TransactionTransfer = &copyTransfer
	}
	if src.TransactionVal.TransactionRead != nil {
		copyRead := *src.TransactionVal.TransactionRead
		dst.TransactionVal.TransactionRead = &copyRead
	}
	return dst
}

func (node *Node) expectedODigest(view, seq int) (Digest, bool) {
	if node == nil || node.newViewODigest == nil {
		return Digest{}, false
	}
	perView, ok := node.newViewODigest[view]
	if !ok {
		return Digest{}, false
	}
	digest, ok := perView[seq]
	return digest, ok
}

func (node *Node) driveNewViewDecision(view, seq int, decision newViewDecision) error {
	node.lock()
	for len(node.Log.LogEntries) <= seq {
		node.Log.LogEntries = append(node.Log.LogEntries, getEmptyLogEntry())
	}
	entry := node.Log.LogEntries[seq]
	if entry == nil || isEmptyLogEntry(entry) {
		entry = getEmptyLogEntry()
	}
	alreadyExecuted := entry.Status == StatusExecuted && entry.Digest == decision.digest
	entry.ViewNumber = view
	entry.Txn = decision.txn
	entry.Digest = decision.digest
	entry.IsNoOp = decision.isNoOp
	if alreadyExecuted {
		node.Log.LogEntries[seq] = entry
		node.unlock()
		return nil
	}
	entry.Status = StatusPrePrepared
	node.Log.LogEntries[seq] = entry
	if txnID := decision.txn.TransactionID.UniqueID; txnID != "" {
		node.Log.ClientRequestIndexLock.Lock()
		node.Log.ClientRequestIndex[txnID] = seq
		node.Log.ClientRequestIndexLock.Unlock()
	}
	node.unlock()

	prePrepare := PrePrepare{
		ViewNumber:  view,
		SequenceNum: seq,
		Digest:      decision.digest,
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
		Transaction: decision.txn,
	}

	return node.executeOrderingForPrePrepare(msg, decision.digest, true)
}

func (node *Node) ingestNewViewProofsLocked(viewChanges []ViewChangeSigned) {
	if len(viewChanges) == 0 {
		return
	}
	for i := range viewChanges {
		vc := viewChanges[i]
		from := vc.ViewChange.FromReplica
		pub := node.getPublicKey(from)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}
		if ok, err := verifyStruct(vc.ViewChange, vc.Signature, pub); err != nil || !ok {
			continue
		}
		copyVC := vc
		node.storeViewChangeLocked(&copyVC)
	}
}

func (node *Node) collectViewChangesLocked(view int, senders []int) []ViewChangeSigned {
	if len(senders) == 0 {
		return nil
	}
	node.ensureViewChangeStateLocked()
	cache := node.viewChangeCache[view]
	if cache == nil {
		return nil
	}
	result := make([]ViewChangeSigned, 0, len(senders))
	for _, id := range senders {
		if vc, ok := cache[id]; ok && vc != nil {
			result = append(result, *cloneViewChange(vc))
		}
	}
	return result
}
