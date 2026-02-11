package server

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"log/slog"
)

func (node *Node) Prepared(req *PreparedMessageSigned, resp *CommitSigned) error {
	node.lock()
	defer node.unlock()
	slog.Info("prepared called for seq num", "seq num", req.PrepareMessage.SequenceNumber)

	nodeID := node.NodeID
	viewNum := node.CurrentView

	setFailure := func(currentView int) {
		resp.Commit.Ack = false
		resp.Commit.NodeID = nodeID
		resp.Commit.ViewNumber = currentView
	}

	if node.Status == NodeStopped {
		slog.Info("Node is stopped", "nodeID", nodeID)
		setFailure(viewNum)
		return nil
	}

	if node.ViewChangeInProgress {
		slog.Info("Prepared view change in progress", "nodeID", nodeID, "new-view", viewNum)
		setFailure(viewNum)
		return nil
	}

	leaderID := node.getLeader()
	publicKey := node.getPublicKey(leaderID)

	//TODO: figure out if we need to verify the digest of the message in Prepare as well
	//verify the digest

	//verify the signature
	if len(publicKey) != ed25519.PublicKeySize {
		slog.Error("leader public key missing or invalid", "nodeID", nodeID, "leaderID", leaderID)
		setFailure(viewNum)
		return nil
	}

	preparedPayload, err := json.Marshal(req.PrepareMessage)
	if err != nil {
		slog.Error("failed to marshal pre-prepare for signature verification", "nodeID", nodeID, "error", err)
		setFailure(viewNum)
		return err
	}

	if !ed25519.Verify(publicKey, preparedPayload, req.PrepareMsgSign) {
		slog.Warn("invalid pre-prepare signature", "nodeID", nodeID, "leaderID", leaderID)
		setFailure(viewNum)
		return nil
	}

	//check if view is the same
	if req.PrepareMessage.ViewNumber != node.CurrentView {
		slog.Warn("view number different", "nodeID", nodeID, "leaderID", leaderID, "currentView", node.CurrentView, "recieved view", req.PrepareMessage.ViewNumber)
		setFailure(viewNum)
		return nil
	}

	//check if n is valid watermark range
	if req.PrepareMessage.SequenceNumber > (node.LastStableCheckpoint + node.CheckpointInterval) {
		slog.Warn("sequence number higher than watermark", "nodeID", nodeID, "leaderID", leaderID, "waterMark", (node.LastStableCheckpoint + node.CheckpointInterval), "recieved seq", req.PrepareMessage.SequenceNumber)
		setFailure(viewNum)
		return nil
	}

	//resize logs
	seq := req.PrepareMessage.SequenceNumber
	for len(node.Log.LogEntries) <= seq {
		node.Log.LogEntries = append(node.Log.LogEntries, getEmptyLogEntry())
	}

	// since the leader has sent a prepared message => the leader must have recieved 2f matching prepare messages
	// check if node has pre-prepare message and add prepare to logs
	logEntry := node.Log.LogEntries[seq]

	validStatus := logEntry.Status == StatusPrePrepared || logEntry.Status == StatusPrepared || logEntry.Status == StatusCommitted || logEntry.Status == StatusExecuted
	if !validStatus || logEntry.ViewNumber != req.PrepareMessage.ViewNumber {
		slog.Warn("request not prepared or view number did not match", "nodeID", nodeID, "leaderID", leaderID, "recieved seq", req.PrepareMessage.SequenceNumber, "status", logEntry.Status, "View", logEntry.ViewNumber)
		setFailure(viewNum)
		return nil
	}

	if node.crashReplica(req.PrepareMessage.ViewNumber) {
		slog.Warn("crash attack: replica refusing prepared ack", "nodeID", nodeID, "seq", req.PrepareMessage.SequenceNumber)
		setFailure(viewNum)
		return nil
	}

	//NOTE: keep it simple
	requiredPrepare := 2 * 2

	validCert, ok := node.verifyPrepareCertificate(req.PrepareMessage, req.PrepareCert, requiredPrepare)
	if !ok {
		slog.Warn("prepare certificate invalid", "nodeID", nodeID, "seq", req.PrepareMessage.SequenceNumber, "view", req.PrepareMessage.ViewNumber)
		setFailure(viewNum)
		return nil
	}

	if logEntry.Status == StatusPrePrepared {
		logEntry.Status = StatusPrepared
	}
	logEntry.PrepareCert = validCert
	node.Log.LogEntries[seq] = logEntry

	// ======================== ATTACK: IN DARK ========================
	if !shouldRespondToLeader(node.InDarkAttackFlag, leaderID, node.InDarkTargets) {
		slog.Warn("leader is a in-dark target, simulating no reply", "nodeID", nodeID)
		setFailure(viewNum)
		return nil
	}

	//make commit messages
	resp.Commit.Ack = true
	resp.Commit.ViewNumber = viewNum
	resp.Commit.SequenceNumber = req.PrepareMessage.SequenceNumber
	resp.Commit.Digest = req.PrepareMessage.Digest
	resp.Commit.NodeID = nodeID
	//TODO: check if this digest logic is correct

	//sign prepare message
	// node.maybeDelay(isByz, behaviour)

	respPayload, err := json.Marshal(resp.Commit)
	if err != nil {
		return err
	}

	// ======================== MAYBE ATTACK: SIGNATURE FOR PREPARED REPLY ========================
	signature := generateSignatureHelper(node.PrivateKey, respPayload, node.InvalidSignatureAttackFlag)
	resp.CommitSignature = signature

	return nil
}

func (node *Node) verifyPrepareCertificate(prepared PreparedMessage, bundle []PrepareSigned, required int) ([]PrepareSigned, bool) {
	if required <= 0 || len(bundle) < required {
		return nil, false
	}

	valid := make([]PrepareSigned, 0, required)
	seen := make(map[int]struct{}, required)
	targetDigest := prepared.Digest

	for _, signed := range bundle {
		prepare := signed.PrePrepareResponse

		if !prepare.Ack ||
			prepare.ViewNumber != prepared.ViewNumber ||
			prepare.SequenceNumber != prepared.SequenceNumber ||
			len(prepare.Digest[:]) != len(targetDigest) ||
			!bytes.Equal(targetDigest, prepare.Digest[:]) {
			continue
		}

		nodeID := prepare.NodeID
		if _, exists := seen[nodeID]; exists {
			continue
		}

		pub := node.getPublicKey(nodeID)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}

		payload, err := json.Marshal(prepare)
		if err != nil {
			continue
		}

		if !ed25519.Verify(pub, payload, signed.PRSignature) {
			continue
		}

		seen[nodeID] = struct{}{}
		valid = append(valid, clonePrepareSigned(signed))
		if len(valid) == required {
			return valid, true
		}
	}

	return nil, false
}
