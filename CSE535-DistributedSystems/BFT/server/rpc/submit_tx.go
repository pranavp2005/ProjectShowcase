package server

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"

	"pranavpateriya.com/bft/common"
)

// Inactive during view change
func (n *Node) SubmitRead(req *common.ClientReqSigned, resp *common.ReplySigned) error {
	if req == nil || resp == nil {
		return nil
	}

	nodePrivateKey := n.PrivateKey

	n.lock()
	defer n.unlock()
	nodeID := n.NodeID

	slog.Info("read only rpc called")
	resp.ReplyPayload.NodeId = nodeID
	resp.ReplyPayload.ViewNumber = n.CurrentView
	resp.ReplyPayload.TransactionID = req.ClientRequestPayload.TransactionID

	if n.Status == NodeStopped {
		slog.Info("dead node droppin read reply")
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "node-stopped"
		return nil
	}

	if n.ViewChangeInProgress {
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "view-change"
		return nil
	}

	requestType := req.ClientRequestPayload.TransactionVal.TransactionType
	if requestType != common.RequestBalance {
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "unexpected-transaction"
		return nil
	}

	acctID := req.ClientRequestPayload.TransactionVal.TransactionRead.AcctNum
	slog.Info("submit read request", "nodeID", nodeID, "account", acctID, "txnID", req.ClientRequestPayload.TransactionID.UniqueID)

	publicKey := n.ClientPublicKeyMap[acctID]
	if len(publicKey) != ed25519.PublicKeySize {
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "unknown-client"
		return nil
	}

	payload, err := json.Marshal(req.ClientRequestPayload)
	if err != nil {
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "marshal-error"
		return err
	}

	if !ed25519.Verify(publicKey, payload, req.ClientRequestSignature) {
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "invalid-signature"
		return nil
	}

	if n.crashActive() {
		slog.Warn("crash attack dropping read reply", "nodeID", nodeID, "account", acctID)
		resp.ReplyPayload.Ack = false
		resp.ReplyPayload.Result.Status = false
		resp.ReplyPayload.Result.Message = "crash-attack-no-reply"
		return nil
	}

	// if isByz && behaviour.Crash {
	// 	resp.ReplyPayload.Ack = false
	// 	resp.ReplyPayload.Result.Status = false
	// 	resp.ReplyPayload.Result.Message = "malicious-crash"
	// 	n.maybeDelay(isByz, behaviour)
	// 	replyPayload, err := json.Marshal(resp.ReplyPayload)
	// 	if err == nil {
	// 		sig := ed25519.Sign(nodePrivateKey, replyPayload)
	// 		if isByz {
	// 			sig = corruptSignature(behaviour, sig)
	// 		}
	// 		resp.ReplySignature = sig
	// 	}
	// 	n.mu.Unlock()
	// 	return nil
	// }

	balance, err := n.Bank.Balance(acctID)
	if err != nil {
		slog.Error("bank error: failed to fetch balance", "acctID", acctID, "error", err)
	}

	// ======================== ATTACK TIMING ========================
	if n.TimingAttackFlag {
		delayTa(int(n.TimeDelay))
	}

	resp.ReplyPayload.Ack = true
	resp.ReplyPayload.NodeId = nodeID
	resp.ReplyPayload.ViewNumber = n.CurrentView
	resp.ReplyPayload.TransactionID = req.ClientRequestPayload.TransactionID
	resp.ReplyPayload.Result.TransactionType = common.RequestBalance
	resp.ReplyPayload.Result.Balance = int(balance)
	resp.ReplyPayload.Result.Status = err == nil
	if err != nil {
		resp.ReplyPayload.Result.Message = err.Error()
	} else {
		resp.ReplyPayload.Result.Message = "balance successful"
	}

	replyPayload, _ := json.Marshal(resp.ReplyPayload)

	// ======================== MAYBE ATTACK: SIGNATURE FOR PRE-PREPARE MESSAGE ========================
	resp.ReplySignature = generateSignatureHelper(nodePrivateKey, replyPayload, n.InvalidSignatureAttackFlag)

	slog.Info("read only rpc called, sending response")

	return nil
}

// TODO: Check this again if it is in line with Linear PBFT
func (node *Node) SubmitNormal(req *common.ClientReqSigned, resp *common.SubmitTxnAck) error {
	if req == nil || resp == nil {
		return nil
	}

	node.lock()
	locked := true
	defer func() {
		if locked {
			node.unlock()
		}
	}()
	nodeID := node.NodeID
	view := node.CurrentView
	leaderID := node.getLeader()
	crashLeader := node.crashLeader(view)

	// We don't need to reply now call a separate cleint RPC for reply after execution
	// resp.ReplyPayload.NodeId = nodeID
	// resp.ReplyPayload.ViewNumber = view
	// resp.ReplyPayload.TransactionID = req.ClientRequestPayload.TransactionID

	txnType := req.ClientRequestPayload.TransactionVal.TransactionType

	if node.Status == NodeStopped {
		slog.Error("node is stooped replying NACK")
		resp.Ack = false
		resp.Message = "node-stopped"
		return nil
	}

	if node.ViewChangeInProgress {
		slog.Error("submit-tx view change in progress replying NACK to client")
		resp.Ack = false
		resp.Message = "view-change"
		return nil
	}

	txn := req.ClientRequestPayload
	clientID := txn.TransactionID.ClientID
	slog.Info("accepted client request", "nodeID", nodeID, "leaderID", leaderID, "client", clientID, "txnID", txn.TransactionID.UniqueID, "type", txnType, "view", view)

	//TODO: I don't need to actually check this, the client is written by me
	if txnType != common.RequestTransfer && txnType != common.RequestBalance {
		slog.Error("invalid transaction type")
		resp.Ack = false
		resp.Message = "unsupported-transaction"
		return nil
	}

	publicKey := node.ClientPublicKeyMap[clientID]
	//verify the key length
	if len(publicKey) != ed25519.PublicKeySize {
		slog.Error("client key verification failed")
		resp.Ack = false
		resp.Message = "unknown-client"
		return nil
	}

	payload, err := json.Marshal(txn)
	if err != nil {
		slog.Error("json marshalling failed")
		resp.Ack = false
		resp.Message = "marshal-error"
		return err
	}

	// verify the payload signature sent by client
	if !ed25519.Verify(publicKey, payload, req.ClientRequestSignature) {
		slog.Error("invalid cleint signature")
		resp.Ack = false
		resp.Message = "invalid-signature"
		return nil
	}

	//FLow: check for the request in logs, if it is executed then send a reply if it does not exist or not executed then the protocol moves ahead

	// NOTE: the transactionID is taken from the client

	digest := DigestOfTransaction(txn)
	txnKey := txn.TransactionID.UniqueID

	node.Log.ClientRequestIndexLock.Lock()
	if idx, ok := node.Log.ClientRequestIndex[txnKey]; ok && idx < len(node.Log.LogEntries) {
		if entry := node.Log.LogEntries[idx]; entry != nil {
			// Ensure same request content (defensive: same ID but different payload should be rejected)
			if entry.Digest != digest {
				slog.Error("entry already exists but diegest of client message doesn not match")
				resp.Ack = false
				resp.Message = "conflicting-request-id"
				node.Log.ClientRequestIndexLock.Unlock()
				return nil
			}

			switch entry.Status {
			case StatusExecuted:
				// Send cached reply to client (even if we're a backup)
				resp.Ack = true
				resp.Message = "already executed"

				reply := common.Reply{
					Ack:           true,
					ViewNumber:    entry.ViewNumber, // or node.CurrentView
					TransactionID: entry.Txn.TransactionID,
					NodeId:        node.NodeID,
					Result:        entry.Reply,
				}

				payload, _ := json.Marshal(reply)
				replySigned := common.ReplySigned{
					ReplyPayload:   reply,
					ReplySignature: generateSignatureHelper(node.PrivateKey, payload, node.InvalidSignatureAttackFlag),
				}
				slog.Info("node already executed request, replying from logs")
				// async, no need to hold the lock
				go node.replyToCilentLocked(&replySigned)
				node.Log.ClientRequestIndexLock.Unlock()
				return nil

			case StatusCommitted, StatusPrepared, StatusPrePrepared:
				// We have it in the pipeline but not executed yet.
				slog.Warn("transaction in logs but not executed", "transactionID", txnKey, "seqNum", idx)
				resp.Ack = false
				resp.Message = "in-progress"
				node.Log.ClientRequestIndexLock.Unlock()
				return nil
			}
		}
	}
	node.Log.ClientRequestIndexLock.Unlock()

	if leaderID != nodeID {
		slog.Info("not the leader and seeing new transaction, start timer and reject")
		resp.Ack = false
		resp.Message = "not leader"

		//there was a broadcast, something is wrong with the leader, start timer
		if txnID := txn.TransactionID.UniqueID; txnID != "" {
			node.handleAddWaitingReqAndTimerLocked(txnID)
		}

		node.unlock()
		locked = false

		return nil

		//NOTE: Remove request forwarding because it was causing bugs
		// txnID := txn.TransactionID.UniqueID
		// node.handleAddWaitingReqAndTimerLocked(txnID)
		// node.mu.Unlock()
		// locked = false

		// slog.Info("forwarding client request to leader", "nodeID", nodeID, "leaderID", leaderID, "txnID", txnID)
		// if err := node.forwardClientRequest(leaderID, req); err != nil {
		// 	slog.Error("failed to forward to leader", "nodeID", nodeID, "leaderID", leaderID, "error", err)
		// 	resp.Ack = false
		// 	resp.Message = fmt.Sprintf("forward-failed: %v", err)
		// } else {
		// 	resp.Ack = true
		// 	resp.Message = fmt.Sprintf("forwarded-to-leader n%d", leaderID)
		// }
		// return nil
	}

	node.HigestSequenceNumber++
	seq := node.HigestSequenceNumber

	//NOTE: resize leader logs
	for len(node.Log.LogEntries) <= seq {
		node.Log.LogEntries = append(node.Log.LogEntries, nil)
	}

	// add the client transaction to leader's log
	entry := &LogEntry{
		IsNoOp:     false,
		ViewNumber: view,
		Txn:        txn,
		Status:     StatusPrePrepared,
		Digest:     digest,
	}

	//NOTE: to this log entry we also add  the pre-prepare gthe leader send and the prepare messages collected
	node.Log.LogEntries[seq] = entry
	slog.Info("assigned sequence number", "nodeID", nodeID, "txnID", txn.TransactionID.UniqueID, "seq", seq, "view", view)

	//TODO: this logic fails in case of equivocation when the leader put two sequence numbers for same messages
	node.Log.ClientRequestIndexLock.Lock()
	node.Log.ClientRequestIndex[txnKey] = seq
	node.Log.ClientRequestIndexLock.Unlock()
	node.handleAddWaitingReqAndTimerLocked(txnKey)
	slog.Info("pending request registered", "nodeID", nodeID, "pending", node.pendingRequests)

	privKey := node.PrivateKey
	selfAddr := node.Address

	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	digestBytes := append([]byte(nil), digest[:]...)

	prePrepare := PrePrepare{
		ViewNumber:  view,
		SequenceNum: seq,
		Digest:      digest,
	}

	prePreparePayload, err := json.Marshal(prePrepare)
	if err != nil {
		return err
	}

	// ======================== MAYBE ATTACK: SIGNATURE FOR PRE-PREPARE MESSAGE ========================
	ppSig := generateSignatureHelper(privKey, prePreparePayload, node.InvalidSignatureAttackFlag)

	prePrepareMsg := PrePrepareMessage{
		PrePrepareSig: PrePrepareSigned{
			PrePrepare:  prePrepare,
			PPSignature: ppSig,
		},
		Transaction: txn,
	}

	f := 2
	requiredPrepare := 2 * f

	prepareMatches := make(map[int]int)
	prepareCertificates := make(map[int][]PrepareSigned)
	preparedSeq := -1

	targetPeersPP := getPeerlistExcludingSelf(selfAddr, node.Peers, node.InDarkAttackFlag, node.InDarkTargets)

	// ======================== ATTACK: EQUIVOCATION ========================
	equivocationFlag := node.EquivocationAttackFlag
	equivocationTargets := append([]int(nil), node.EquivocationSeqNtarget...)

	equivTargetSet := make(map[int]struct{}, len(equivocationTargets))
	for _, id := range equivocationTargets {
		equivTargetSet[id] = struct{}{}
	}

	var (
		altPrePrepareMsg *PrePrepareMessage
	)
	seqAlt := seq + 1

	if equivocationFlag && len(equivocationTargets) > 0 {
		// update leader's highest seqNumber so that next request leader can assign fresh seq number
		node.HigestSequenceNumber = seqAlt
		slog.Info("leader equivocating seq increased to seqAlt", "seq", seqAlt)

		//add the same request at next seq number
		for len(node.Log.LogEntries) <= seqAlt {
			node.Log.LogEntries = append(node.Log.LogEntries, nil)
		}

		// add the client transaction to leader's log
		secondEntry := &LogEntry{
			IsNoOp:     false,
			ViewNumber: view,
			Txn:        txn,
			Status:     StatusPrePrepared,
			Digest:     digest,
		}

		//
		node.Log.LogEntries[seqAlt] = secondEntry
		//TODO: Decide if you want to insert it in the list of seen requests to seq# map yes

		altPrePrepare := PrePrepare{
			ViewNumber:  view,
			SequenceNum: seqAlt,
			Digest:      digest,
		}

		altPayload, err := json.Marshal(altPrePrepare)
		if err != nil {
			return err
		}

		altSig := generateSignatureHelper(privKey, altPayload, node.InvalidSignatureAttackFlag)

		altPrePrepareMsg = &PrePrepareMessage{
			PrePrepareSig: PrePrepareSigned{
				PrePrepare:  altPrePrepare,
				PPSignature: altSig,
			},
			Transaction: txn,
		}
	}

	// ======================== ATTACK: TIMING BEFORE PRE-PREPARE MESSAGE ========================
	if node.TimingAttackFlag {
		delayTa(node.TimeDelay)
	}

	seqPreprepareMappin := make(map[int]PrePrepareMessage)

	// ======================== PRE-PREPARED BROADCAST ========================
	for _, addr := range targetPeersPP {
		if addr == "" || addr == selfAddr {
			continue
		}

		//Equivocation attack, send different seq for same message
		peerID := 0
		if port := extractPortFast(addr); port != "" {
			if id, ok := portToNodeIDMap[port]; ok {
				peerID = id
			}
		}
		sendAlt := altPrePrepareMsg != nil
		if sendAlt {
			if _, ok := equivTargetSet[peerID]; ok {
				sendAlt = false
			}
		}

		msgToSend := prePrepareMsg
		if sendAlt {
			msgToSend = *altPrePrepareMsg
		}
		seqPreprepareMappin[msgToSend.PrePrepareSig.PrePrepare.SequenceNum] = msgToSend

		node.warnIfMuHeld("dial-preprepare")
		node.warnIfMuHeld("dial-prepared")
		node.warnIfMuHeld("dial-commit")
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for pre-prepare failed", "nodeID", nodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)

		var prepareResp PrepareSigned
		slog.Info("sending pre-prepare", "target", addr, "sequence num", msgToSend.PrePrepareSig.PrePrepare.SequenceNum)
		callErr := client.Call("Node.PrePrepare", &msgToSend, &prepareResp)
		client.Close()
		if callErr != nil {
			slog.Error("pre-prepare RPC failed", "nodeID", nodeID, "peer", addr, "error", callErr)
			continue
		}

		resp := prepareResp.PrePrepareResponse

		if !resp.Ack {
			slog.Warn("got a NACK", "from nodeID", resp.NodeID)
			continue
		}

		if !(resp.ViewNumber == view) {
			slog.Warn("view number does not match in prepare response", "from nodeID", resp.NodeID)
			continue
		}

		if !(resp.SequenceNumber == msgToSend.PrePrepareSig.PrePrepare.SequenceNum) {
			slog.Warn("sequence number does not match in prepare response", "from nodeID", resp.NodeID)
			continue
		}

		if !bytes.Equal(resp.Digest[:], digestBytes) {
			slog.Warn("digest does not match", "from nodeID", resp.NodeID)
			continue
		}

		pub := node.getPublicKey(prepareResp.PrePrepareResponse.NodeID)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}

		preparePayload, err := json.Marshal(prepareResp.PrePrepareResponse)
		if err != nil {
			continue
		}

		if !ed25519.Verify(pub, preparePayload, prepareResp.PRSignature) {
			continue
		}

		//NOTE: all the prepare certificates, irrespective if we recieved more than quorum response
		prepareCertificates[resp.SequenceNumber] = append(prepareCertificates[resp.SequenceNumber], clonePrepareSigned(prepareResp))

		prepareMatches[resp.SequenceNumber]++
		//due to quorum only one request can be prepared at one sequence number
		if preparedSeq == -1 && prepareMatches[resp.SequenceNumber] >= requiredPrepare {
			preparedSeq = resp.SequenceNumber
		}
	}

	if preparedSeq != -1 {
		slog.Info("recieved prepares", "counts", prepareMatches, "prepared seq", preparedSeq, "matchingCount", prepareMatches[preparedSeq])
	}

	if preparedSeq == -1 {
		resp.Ack = false
		resp.Message = "insufficient-prepares"
		return nil
	}

	if crashLeader {
		resp.Ack = true
		resp.Message = "accepted"
		slog.Warn("crash attack active: leader suppressing prepared broadcast", "nodeID", nodeID, "seq", seq, "view", view)
		return nil
	}

	certBundle := prepareCertificates[preparedSeq]
	if len(certBundle) < requiredPrepare {
		resp.Ack = false
		resp.Message = "insufficient-prepare-cert"
		slog.Warn("insufficient prepare certificate data", "nodeID", nodeID, "seq", preparedSeq, "have", len(certBundle), "need", requiredPrepare)
		return nil
	}

	//NOTE: we do not need to trim the certBundle if we got more than the quorum size
	// if len(certBundle) > requiredPrepare {
	// 	certBundle = certBundle[:requiredPrepare]
	// }

	//NOTE: Leader marks it's own logs as prepared after recieving and puts the prepared messages cert and the corresponding pre-prepare message in logs
	if preparedSeq < len(node.Log.LogEntries) && node.Log.LogEntries[preparedSeq] != nil {
		logEntry := node.Log.LogEntries[preparedSeq]
		logEntry.Status = StatusPrepared
		logEntry.PrepareCert = clonePrepareCerts(certBundle)
		logEntry.PrePrepareCert = clonePrePrepareSigned(seqPreprepareMappin[preparedSeq].PrePrepareSig)
		node.Log.LogEntries[preparedSeq] = logEntry
	}

	preparedMsg := PreparedMessage{
		ViewNumber:     view,
		SequenceNumber: preparedSeq,
		Digest:         digestBytes,
		NodeID:         nodeID,
	}

	preparedPayload, err := json.Marshal(preparedMsg)
	if err != nil {
		return err
	}

	// ======================== MAYBE ATTACK: SIGNATURE FOR PREPARED MESSAGE ========================
	preparedSig := generateSignatureHelper(privKey, preparedPayload, node.InvalidSignatureAttackFlag)

	preparedSigned := PreparedMessageSigned{
		PrepareMessage: preparedMsg,
		PrepareMsgSign: preparedSig,
		PrepareCert:    clonePrepareCerts(certBundle),
	}

	requiredCommit := 2 * f
	commitMatches := make(map[int]int)
	commitCertBundle := make([]CommitSigned, 0, requiredCommit+1)
	commitCertNodes := make(map[int]struct{})

	// ======================== ATTACK: TIMING BEFORE PREPARED MESSAGE ========================
	if node.TimingAttackFlag {
		delayTa(node.TimeDelay)
	}

	// ======================== PREPARED BROADCAST ========================
	// ======================== ATTACK: CRASH, LEADER AVOIDS SENDING PREPARE TO OTHERS ========================
	slog.Info("prepared broadcast")
	for _, addr := range targetPeersPP {
		if addr == "" || addr == selfAddr {
			slog.Info("do not iterate over self")
			continue
		}

		node.warnIfMuHeld("dial-prepared")
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for prepared failed", "nodeID", nodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)

		var commitResp CommitSigned
		slog.Info("calling prepared for ", "port", addr)
		callErr := client.Call("Node.Prepared", &preparedSigned, &commitResp)
		client.Close()
		if callErr != nil {
			slog.Error("prepared RPC failed", "nodeID", nodeID, "peer", addr, "error", callErr)
			continue
		}

		resp := commitResp

		if !resp.Commit.Ack {
			slog.Warn("NACK recieved", "from nodeID", commitResp.Commit.NodeID)
			continue
		}

		if len(resp.Commit.Digest) != len(digestBytes) {
			slog.Warn("digest does not match", "from nodeID", commitResp.Commit.NodeID)
			continue
		}

		if !bytes.Equal(commitResp.Commit.Digest, digestBytes) {
			slog.Warn("digest does not match", "from nodeID", commitResp.Commit.NodeID)
			continue
		}

		if !(resp.Commit.ViewNumber == view) {
			slog.Warn("view number does not match in commit response", "from nodeID", commitResp.Commit.NodeID)
			continue
		}

		if !(resp.Commit.SequenceNumber == preparedSeq) {
			slog.Warn("sequence number does not match in commit response", "from nodeID", commitResp.Commit.NodeID)
			continue
		}

		pub := node.getPublicKey(commitResp.Commit.NodeID)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}

		commitPayload, err := json.Marshal(commitResp.Commit)
		if err != nil {
			continue
		}

		if !ed25519.Verify(pub, commitPayload, commitResp.CommitSignature) {
			slog.Warn("commit signature and payload do not match", "from nodeID", commitResp.Commit.NodeID)
			continue
		}
		if _, exists := commitCertNodes[resp.Commit.NodeID]; !exists {
			commitCertNodes[resp.Commit.NodeID] = struct{}{}
			commitCertBundle = append(commitCertBundle, cloneCommitSigned(resp))
		}
		commitMatches[resp.Commit.SequenceNumber]++
		slog.Info("recieved matching commit message increasing count", "seq", resp.Commit.SequenceNumber, "count", commitMatches[resp.Commit.SequenceNumber])
	}

	matchingCommit := commitMatches[preparedSeq]
	slog.Info("recieved commit messages", "counts", commitMatches, "targetSeq", preparedSeq, "matching commits", matchingCommit)

	if matchingCommit < requiredCommit {
		slog.Warn("insufficient commits quorum not reached", "seq number", preparedSeq)
		resp.Ack = false
		resp.Message = "insufficient-commits"
		return nil
	}

	if len(commitCertBundle) < requiredCommit {
		slog.Warn("insufficient unique commit certificates", "seq number", preparedSeq, "certs", len(commitCertBundle))
		resp.Ack = false
		resp.Message = "insufficient-commit-certs"
		return nil
	}

	selfCommit := Commit{
		Ack:            true,
		ViewNumber:     view,
		SequenceNumber: preparedSeq,
		Digest:         append([]byte(nil), digestBytes...),
		NodeID:         nodeID,
	}
	selfCommitPayload, err := json.Marshal(selfCommit)
	if err != nil {
		return err
	}
	selfCommitSignature := generateSignatureHelper(privKey, selfCommitPayload, node.InvalidSignatureAttackFlag)
	commitCertBundle = append(commitCertBundle, CommitSigned{
		Commit:          selfCommit,
		CommitSignature: selfCommitSignature,
	})
	commitCertNodes[nodeID] = struct{}{}

	if preparedSeq < len(node.Log.LogEntries) && node.Log.LogEntries[preparedSeq] != nil {
		logEntry := node.Log.LogEntries[preparedSeq]
		logEntry.Status = StatusCommitted
		node.Log.LogEntries[preparedSeq] = logEntry
	}

	committed := Committed{
		ViewNumber:     view,
		SequenceNumber: preparedSeq,
		Digest:         digestBytes,
		NodeID:         nodeID,
	}
	committedPayload, err := json.Marshal(committed)
	if err != nil {
		return err
	}

	// ======================== MAYBE ATTACK: SIGNATURE FOR COMMITTED MESSAGE ========================
	commitSig := generateSignatureHelper(privKey, committedPayload, node.InvalidSignatureAttackFlag)

	committedSigned := CommitedSigned{
		Committed:       committed,
		CommitSignature: commitSig,
		CommitCert:      commitCertBundle,
	}

	// ======================== ATTACK: TIMING BEFORE COMMIT MESSAGE ========================
	//timing attack before sending committed message
	if node.TimingAttackFlag {
		delayTa(node.TimeDelay)
	}

	// ======================== COMMITED BROADCAST ========================
	for _, addr := range targetPeersPP {
		if addr == "" {
			continue
		}
		if addr == selfAddr {
			continue
		}

		node.warnIfMuHeld("dial-commit")
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			slog.Error("dial peer for commit failed", "nodeID", nodeID, "peer", addr, "error", err)
			continue
		}
		client := rpc.NewClient(conn)

		var reply CommitedReply
		slog.Info("calling Committed RPC for node", "nodeID", addr)
		commitErr := client.Call("Node.Commited", &committedSigned, &reply)
		client.Close()
		if commitErr != nil {
			slog.Error("commit RPC failed", "nodeID", nodeID, "peer", addr, "error", commitErr)
		}
	}

	// Leader needs to mark itself committed/executed as well.
	var selfReply CommitedReply
	//NOTE: some operations are redundant in function and the log changes we made above
	if err := node.commitedLocked(&committedSigned, &selfReply); err != nil {
		slog.Error("self commit failed", "nodeID", nodeID, "error", err)
	}

	resp.Ack = true
	resp.Message = "successful"

	return nil
}

func (node *Node) forwardClientRequest(leaderID int, req *common.ClientReqSigned) error {
	addr := node.addressForNodeID(leaderID)
	if addr == "" {
		return fmt.Errorf("unknown leader address for node %d", leaderID)
	}

	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}

	node.warnIfMuHeld("dial-forward-client")
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	var reply common.SubmitTxnAck
	if err := client.Call("Node.SubmitNormal", req, &reply); err != nil {
		return err
	}
	if !reply.Ack {
		return fmt.Errorf("forward rejected: %s", reply.Message)
	}
	return nil
}

func (node *Node) addressForNodeID(targetID int) string {
	if targetID == node.NodeID {
		return node.Address
	}

	port, ok := nodeIDtoPortMap[targetID]
	if !ok || port == "" {
		return ""
	}

	for _, addr := range node.Peers {
		if extractPortFast(addr) == port {
			return addr
		}
	}

	return ":" + port
}
