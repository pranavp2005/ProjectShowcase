package server

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"log/slog"

	"pranavpateriya.com/bft/common"
)

// NOTE: Preprepare
// invariant, every request in preprepare is a new request which the backup has not seen and should have different sequence numbers
// if the sequence numbers are same then we should reject the request
func (node *Node) PrePrepare(req *PrePrepareMessage, resp *PrepareSigned) error {

	node.lock()
	defer node.unlock()
	slog.Info("Preprepare called for", "seq", req.PrePrepareSig.PrePrepare.SequenceNum)
	nodeID := node.NodeID
	privKey := node.PrivateKey
	viewNum := node.CurrentView

	setNackLocked := func() {
		resp.PrePrepareResponse.Ack = false
		resp.PrePrepareResponse.NodeID = nodeID
		resp.PrePrepareResponse.ViewNumber = node.CurrentView
	}

	setAckLocked := func(seq int, digest Digest) {
		resp.PrePrepareResponse.Ack = true
		resp.PrePrepareResponse.NodeID = nodeID
		resp.PrePrepareResponse.ViewNumber = node.CurrentView
		resp.PrePrepareResponse.SequenceNumber = seq
		resp.PrePrepareResponse.Digest = digest
	}

	resp.PrePrepareResponse.NodeID = nodeID
	resp.PrePrepareResponse.ViewNumber = node.CurrentView

	if node.Status == NodeStopped {
		slog.Info("Node is stopped", "nodeID", nodeID)
		setNackLocked()
		return nil
	}

	if node.ViewChangeInProgress {
		slog.Info("pre-prepare view change in progress", "nodeID", nodeID, "new-view", viewNum)
		setNackLocked()
		return nil
	}

	leaderID := node.getLeader()
	publicKey := append(ed25519.PublicKey(nil), node.getPublicKey(leaderID)...)
	if len(publicKey) != ed25519.PublicKeySize {
		slog.Error("leader public key missing or invalid", "nodeID", nodeID, "leaderID", leaderID)
		setNackLocked()
		return nil
	}

	payload, err := json.Marshal(req.Transaction)
	if err != nil {
		slog.Error("failed to marshal transaction for digest verification", "nodeID", nodeID, "error", err)
		setNackLocked()
		return err
	}
	computedDigest := sha256.Sum256(payload)
	if computedDigest != req.PrePrepareSig.PrePrepare.Digest {
		slog.Warn("pre-prepare digest mismatch", "nodeID", nodeID)
		setNackLocked()
		return nil
	}

	prePreparePayload, err := json.Marshal(req.PrePrepareSig.PrePrepare)
	if err != nil {
		slog.Error("failed to marshal pre-prepare for signature verification", "nodeID", nodeID, "error", err)
		setNackLocked()
		return err
	}

	if !ed25519.Verify(publicKey, prePreparePayload, req.PrePrepareSig.PPSignature) {
		slog.Warn("invalid pre-prepare signature", "nodeID", nodeID, "leaderID", leaderID)
		setNackLocked()
		return nil
	}

	resp.PrePrepareResponse.ViewNumber = viewNum

	if req.PrePrepareSig.PrePrepare.ViewNumber != viewNum {
		slog.Warn("view number different", "nodeID", nodeID, "leaderID", leaderID, "currentView", viewNum, "recieved view", req.PrePrepareSig.PrePrepare.ViewNumber)
		setNackLocked()
		return nil
	}

	highWatermark := node.LastStableCheckpoint + node.CheckpointInterval
	if req.PrePrepareSig.PrePrepare.SequenceNum > highWatermark {
		slog.Warn("sequence number higher than watermark", "nodeID", nodeID, "leaderID", leaderID, "waterMark", highWatermark, "recieved seq", req.PrePrepareSig.PrePrepare.SequenceNum)
		setNackLocked()
		return nil
	}

	seq := req.PrePrepareSig.PrePrepare.SequenceNum
	for len(node.Log.LogEntries) <= seq {
		node.Log.LogEntries = append(node.Log.LogEntries, getEmptyLogEntry())
	}

	logEntry := node.Log.LogEntries[seq]
	reqView := req.PrePrepareSig.PrePrepare.ViewNumber

	registeredPending := false
	if isEmptyLogEntry(logEntry) {
		insertLogEntry := getEmptyLogEntry()
		insertLogEntry.ViewNumber = reqView
		insertLogEntry.Txn = req.Transaction
		insertLogEntry.Digest = computedDigest
		insertLogEntry.Status = StatusPrePrepared
		insertLogEntry.PrePrepareCert = req.PrePrepareSig
		node.Log.LogEntries[seq] = insertLogEntry
		registeredPending = true
	} else {
		//TODO: check if we accepted
	}

	if node.crashReplica(viewNum) {
		slog.Warn("crash attack: replica suppresses prepare response", "nodeID", nodeID, "seq", seq)
		setNackLocked()
		return nil
	}

	if !shouldRespondToLeader(node.InDarkAttackFlag, leaderID, node.InDarkTargets) {
		slog.Warn("leader is a in-dark target, simulating no reply", "nodeID", nodeID)
		setNackLocked()
		return nil
	}

	setAckLocked(seq, computedDigest)

	respPayload, err := json.Marshal(resp.PrePrepareResponse)
	if err != nil {
		return err
	}

	// ======================== MAYBE ATTACK: SIGNATURE FOR PRE-PREPARE REPLY ========================
	//if the node has InvalidSignatureAttackFlag true then this will corrupt the signature
	signature := generateSignatureHelper(privKey, respPayload, node.InvalidSignatureAttackFlag)
	resp.PRSignature = signature

	if registeredPending {
		node.handleAddWaitingReqAndTimerLocked(req.Transaction.TransactionID.UniqueID)
	}

	return nil
}

// func (node *Node) PrePrepareNewView(req *PrePrepareMessage, resp *PrepareSigned) error {
// 	slog.Info("Preprepare new view called", "seq", req.PrePrepareSig.PrePrepare.SequenceNum)
// 	node.mu.Lock()
// 	defer node.mu.Unlock()

// 	nodeID := node.NodeID
// 	privKey := node.PrivateKey
// 	viewNum := node.CurrentView

// 	setNackLocked := func() {
// 		resp.PrePrepareResponse.Ack = false
// 		resp.PrePrepareResponse.NodeID = nodeID
// 		resp.PrePrepareResponse.ViewNumber = node.CurrentView
// 	}

// 	setAckLocked := func(seq int, digest Digest) {
// 		resp.PrePrepareResponse.Ack = true
// 		resp.PrePrepareResponse.NodeID = nodeID
// 		resp.PrePrepareResponse.ViewNumber = node.CurrentView
// 		resp.PrePrepareResponse.SequenceNumber = seq
// 		resp.PrePrepareResponse.Digest = digest
// 	}

// 	resp.PrePrepareResponse.NodeID = nodeID
// 	resp.PrePrepareResponse.ViewNumber = node.CurrentView

// 	if node.Status == NodeStopped {
// 		slog.Info("Node is stopped", "nodeID", nodeID)
// 		setNackLocked()
// 		return nil
// 	}

// 	if node.ViewChangeInProgress {
// 		slog.Info("pre-prepare new view rejected, view change in progress", "nodeID", nodeID, "targetView", viewNum)
// 		setNackLocked()
// 		return nil
// 	}

// 	leaderID := node.getLeader()
// 	publicKey := append(ed25519.PublicKey(nil), node.getPublicKey(leaderID)...)
// 	if len(publicKey) != ed25519.PublicKeySize {
// 		slog.Error("leader public key missing or invalid", "nodeID", nodeID, "leaderID", leaderID)
// 		setNackLocked()
// 		return nil
// 	}

// 	payload, err := json.Marshal(req.Transaction)
// 	if err != nil {
// 		slog.Error("failed to marshal transaction for digest verification", "nodeID", nodeID, "error", err)
// 		setNackLocked()
// 		return err
// 	}
// 	computedDigest := sha256.Sum256(payload)
// 	if computedDigest != req.PrePrepareSig.PrePrepare.Digest {
// 		slog.Warn("pre-prepare digest mismatch", "nodeID", nodeID)
// 		setNackLocked()
// 		return nil
// 	}

// 	prePreparePayload, err := json.Marshal(req.PrePrepareSig.PrePrepare)
// 	if err != nil {
// 		slog.Error("failed to marshal pre-prepare for signature verification", "nodeID", nodeID, "error", err)
// 		setNackLocked()
// 		return err
// 	}

// 	if !ed25519.Verify(publicKey, prePreparePayload, req.PrePrepareSig.PPSignature) {
// 		slog.Warn("invalid pre-prepare signature", "nodeID", nodeID, "leaderID", leaderID)
// 		setNackLocked()
// 		return nil
// 	}

// 	resp.PrePrepareResponse.ViewNumber = viewNum

// 	if req.PrePrepareSig.PrePrepare.ViewNumber != viewNum {
// 		slog.Warn("view number different", "nodeID", nodeID, "leaderID", leaderID, "currentView", viewNum, "recieved view", req.PrePrepareSig.PrePrepare.ViewNumber)
// 		setNackLocked()
// 		return nil
// 	}

// 	highWatermark := node.LastStableCheckpoint + node.CheckpointInterval
// 	if req.PrePrepareSig.PrePrepare.SequenceNum > highWatermark {
// 		slog.Warn("sequence number higher than watermark", "nodeID", nodeID, "leaderID", leaderID, "waterMark", highWatermark, "recieved seq", req.PrePrepareSig.PrePrepare.SequenceNum)
// 		setNackLocked()
// 		return nil
// 	}

// 	seq := req.PrePrepareSig.PrePrepare.SequenceNum
// 	for len(node.Log.LogEntries) <= seq {
// 		node.Log.LogEntries = append(node.Log.LogEntries, getEmptyLogEntry())
// 	}

// 	logEntry := node.Log.LogEntries[seq]
// 	if logEntry == nil || isEmptyLogEntry(logEntry) {
// 		logEntry = getEmptyLogEntry()
// 	}
// 	reqView := req.PrePrepareSig.PrePrepare.ViewNumber
// 	isNoOpReq := req.Transaction.TransactionVal.TransactionType == common.RequestNoOp

// 	switch logEntry.Status {
// 	case StatusPrepared, StatusCommitted, StatusExecuted:
// 		if logEntry.Digest != ([32]byte{}) && logEntry.Digest != computedDigest {
// 			slog.Warn("pre-prepare new view conflicts with prepared log entry", "nodeID", nodeID, "seq", seq)
// 			setNackLocked()
// 			return nil
// 		}
// 	default:
// 		if logEntry.ViewNumber == reqView && logEntry.Digest != ([32]byte{}) && logEntry.Digest != computedDigest {
// 			slog.Warn("conflicting digest for view", "nodeID", nodeID, "seq", seq)
// 			setNackLocked()
// 			return nil
// 		}
// 		if logEntry.ViewNumber != reqView || logEntry.Digest == ([32]byte{}) {
// 			logEntry.Txn = req.Transaction
// 			logEntry.Digest = computedDigest
// 			logEntry.IsNoOp = isNoOpReq
// 		} else if logEntry.Txn.TransactionID.UniqueID == "" && req.Transaction.TransactionID.UniqueID != "" {
// 			logEntry.Txn = req.Transaction
// 			logEntry.IsNoOp = isNoOpReq
// 		}
// 	}

// 	if isNoOpReq {
// 		logEntry.IsNoOp = true
// 	}

// 	logEntry.ViewNumber = reqView
// 	if logEntry.Digest == ([32]byte{}) {
// 		logEntry.Digest = computedDigest
// 	}
// 	if logEntry.Status == StatusNone {
// 		logEntry.Status = StatusPrePrepared
// 	}
// 	logEntry.PrePrepareCert = req.PrePrepareSig
// 	node.Log.LogEntries[seq] = logEntry

// 	if node.crashReplica(viewNum) {
// 		slog.Warn("crash attack: replica suppresses prepare response", "nodeID", nodeID, "seq", seq)
// 		setNackLocked()
// 		return nil
// 	}

// 	if !shouldRespondToLeader(node.InDarkAttackFlag, leaderID, node.InDarkTargets) {
// 		slog.Warn("leader is a in-dark target, simulating no reply", "nodeID", nodeID)
// 		setNackLocked()
// 		return nil
// 	}

// 	setAckLocked(seq, computedDigest)

// 	respPayload, err := json.Marshal(resp.PrePrepareResponse)
// 	if err != nil {
// 		return err
// 	}

// 	// ======================== MAYBE ATTACK: SIGNATURE FOR PRE-PREPARE REPLY ========================
// 	//if the node has InvalidSignatureAttackFlag true then this will corrupt the signature
// 	signature := generateSignatureHelper(privKey, respPayload, node.InvalidSignatureAttackFlag)
// 	resp.PRSignature = signature

// 	//Add request to waiting to execute set and start the timer if not already started
// 	node.handleAddWaitingReqAndTimerLocked(req.Transaction.TransactionID.UniqueID)

// 	return nil
// }

// PrePrepareNewView handles Pre-Prepare(v', n, d) after we've adopted view v'.
// It enforces: leader sig, watermarks, digest(req)==d, consistency with O (if available),
// and correct state transitions across {None, PrePrepared(old/new), Prepared(old), Committed, Executed}.
func (node *Node) PrePrepareNewView(req *PrePrepareMessage, resp *PrepareSigned) error {
	slog.Info("Preprepare new view called", "seq", req.PrePrepareSig.PrePrepare.SequenceNum)

	node.lock()
	defer node.unlock()

	nodeID := node.NodeID
	privKey := node.PrivateKey
	viewNum := node.CurrentView

	setNackLocked := func() {
		resp.PrePrepareResponse.Ack = false
		resp.PrePrepareResponse.NodeID = nodeID
		resp.PrePrepareResponse.ViewNumber = viewNum
	}

	setAckLocked := func(seq int, digest Digest) {
		resp.PrePrepareResponse.Ack = true
		resp.PrePrepareResponse.NodeID = nodeID
		resp.PrePrepareResponse.ViewNumber = viewNum
		resp.PrePrepareResponse.SequenceNumber = seq
		resp.PrePrepareResponse.Digest = digest
	}

	resp.PrePrepareResponse.NodeID = nodeID
	resp.PrePrepareResponse.ViewNumber = viewNum

	// 0) Basic replica guards
	if node.Status == NodeStopped {
		slog.Info("node stopped", "nodeID", nodeID)
		setNackLocked()
		return nil
	}
	if node.ViewChangeInProgress {
		slog.Info("reject pre-prepare: view-change in progress", "nodeID", nodeID, "targetView", viewNum)
		setNackLocked()
		return nil
	}

	// 1) Verify Pre-Prepare is from current leader of v' and signature is valid
	leaderID := node.getLeader()
	pub := append(ed25519.PublicKey(nil), node.getPublicKey(leaderID)...)
	if len(pub) != ed25519.PublicKeySize {
		slog.Error("leader public key missing/invalid", "leaderID", leaderID)
		setNackLocked()
		return nil
	}
	// Must be for current view v'
	if req.PrePrepareSig.PrePrepare.ViewNumber != viewNum {
		slog.Warn("stale/wrong view in pre-prepare", "currentView", viewNum, "got", req.PrePrepareSig.PrePrepare.ViewNumber)
		setNackLocked()
		return nil
	}
	ppBytes, err := json.Marshal(req.PrePrepareSig.PrePrepare)
	if err != nil {
		slog.Error("marshal pre-prepare failed", "err", err)
		setNackLocked()
		return err
	}
	if !ed25519.Verify(pub, ppBytes, req.PrePrepareSig.PPSignature) {
		slog.Warn("invalid pre-prepare signature", "leaderID", leaderID)
		setNackLocked()
		return nil
	}

	// 2) Digest(req) must match d
	// (prefer your DigestOfTransaction to keep hashing consistent everywhere)
	computedDigest := DigestOfTransaction(req.Transaction)
	if computedDigest != req.PrePrepareSig.PrePrepare.Digest {
		slog.Warn("digest mismatch in pre-prepare")
		setNackLocked()
		return nil
	}

	seq := req.PrePrepareSig.PrePrepare.SequenceNum

	// 3) Watermarks: both low and high relative to last stable
	lowWatermark := node.LastStableCheckpoint
	highWatermark := node.LastStableCheckpoint + node.CheckpointInterval
	if seq < lowWatermark || seq > highWatermark {
		slog.Warn("seq out of watermarks", "low", lowWatermark, "high", highWatermark, "seq", seq)
		setNackLocked()
		return nil
	}

	if expDigest, ok := node.expectedODigest(viewNum, seq); ok && expDigest != computedDigest {
		slog.Warn("pre-prepare conflicts with O", "seq", seq)
		setNackLocked()
		return nil
	}

	// Ensure log slice capacity
	for len(node.Log.LogEntries) <= seq {
		node.Log.LogEntries = append(node.Log.LogEntries, getEmptyLogEntry())
	}
	logEntry := node.Log.LogEntries[seq]
	if logEntry == nil || isEmptyLogEntry(logEntry) {
		logEntry = getEmptyLogEntry()
	}

	reqView := req.PrePrepareSig.PrePrepare.ViewNumber
	isNoOpReq := req.Transaction.TransactionVal.TransactionType == common.RequestNoOp

	// 5) State-dependent handling
	switch logEntry.Status {

	// ---- Terminal / strong states: never roll back; allow duplicate-of-same-digest only ----
	case StatusExecuted:
		if logEntry.Digest != computedDigest {
			slog.Warn("conflicts with executed entry", "seq", seq)
			setNackLocked()
			return nil
		}
		// Adopt the new view number so later Prepared/Committed checks succeed.
		logEntry.ViewNumber = reqView
		node.Log.LogEntries[seq] = logEntry
		setAckLocked(seq, computedDigest)

	case StatusCommitted:
		if logEntry.Digest != computedDigest {
			slog.Warn("conflicts with committed entry", "seq", seq)
			setNackLocked()
			return nil
		}
		logEntry.ViewNumber = reqView
		node.Log.LogEntries[seq] = logEntry
		setAckLocked(seq, computedDigest)

	// ---- Prepared in an older view: may have different digest than O; adopt O's digest in v' ----
	case StatusPrepared:
		if logEntry.ViewNumber == reqView {
			// Same view as incoming: must not change digest within a view
			if logEntry.Digest != computedDigest {
				slog.Warn("conflicting pre-prepare for same (v,seq)", "view", reqView, "seq", seq)
				setNackLocked()
				return nil
			}
			// Same digest in v': duplicate; OK to ack
			setAckLocked(seq, computedDigest)
		} else { // prepared in old view, moving to v'
			if logEntry.Digest != computedDigest {
				// Different prepared value in old view; New-View may have chosen this new digest
				// Overwrite to adopt v' choice; downgrade to PrePrepared in v' (will re-prepare)
				logEntry.Txn = req.Transaction
				logEntry.Digest = computedDigest
				logEntry.IsNoOp = isNoOpReq
			}
			logEntry.ViewNumber = reqView
			logEntry.Status = StatusPrePrepared
			logEntry.PrePrepareCert = req.PrePrepareSig
			node.Log.LogEntries[seq] = logEntry
			setAckLocked(seq, computedDigest)
		}

	// ---- PrePrepared/None: accept if no in-view conflict; adopt v' ----
	case StatusPrePrepared, StatusNone:
		// If already in v' with a different digest => conflict
		if logEntry.ViewNumber == reqView && (logEntry.Digest != ([32]byte{}) && logEntry.Digest != computedDigest) {
			slog.Warn("conflicting digest in same view", "view", reqView, "seq", seq)
			setNackLocked()
			return nil
		}
		// Adopt/overwrite for v'
		logEntry.ViewNumber = reqView
		logEntry.Txn = req.Transaction
		logEntry.Digest = computedDigest
		logEntry.IsNoOp = isNoOpReq
		if logEntry.Status == StatusNone {
			logEntry.Status = StatusPrePrepared
		} else {
			// remains PrePrepared
			logEntry.Status = StatusPrePrepared
		}
		logEntry.PrePrepareCert = req.PrePrepareSig
		node.Log.LogEntries[seq] = logEntry

		setAckLocked(seq, computedDigest)

	default:
		// Unknown status: be conservative
		slog.Warn("invalid state for pre-prepare", "seq", seq, "status", logEntry.Status)
		setNackLocked()
		return nil
	}

	// Attack toggles (unchanged from pre-prepare)
	if node.crashReplica(viewNum) {
		slog.Warn("crash attack: suppress prepare reply", "nodeID", nodeID, "seq", seq)
		setNackLocked()
		return nil
	}
	if !shouldRespondToLeader(node.InDarkAttackFlag, leaderID, node.InDarkTargets) {
		slog.Warn("in-dark target: simulate no reply", "nodeID", nodeID)
		setNackLocked()
		return nil
	}

	// Sign the Prepare response (linear PBFT: Prepare goes back to the leader)
	respPayload, err := json.Marshal(resp.PrePrepareResponse)
	if err != nil {
		return err
	}
	resp.PRSignature = generateSignatureHelper(privKey, respPayload, node.InvalidSignatureAttackFlag)

	//NOTE: This is new view, we are in catch up there is no need for timer once we have seen a new view(and therefore pre-prepare now)
	// node.handleAddWaitingReqAndTimerLocked(req.Transaction.TransactionID.UniqueID)

	return nil
}
