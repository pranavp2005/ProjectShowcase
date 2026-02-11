package server

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"log/slog"
	"net"
	"net/rpc"
	"time"

	"pranavpateriya.com/bft/common"
	"pranavpateriya.com/bft/server/persistence/bank"
)

func (node *Node) Commited(req *CommitedSigned, resp *CommitedReply) error {
	slog.Info("commit called")
	node.lock()
	defer node.unlock()
	return node.commitedLocked(req, resp)
}

// commitedLocked assumes node.mu is already held by the caller.
func (node *Node) commitedLocked(req *CommitedSigned, resp *CommitedReply) error {
	nodeID := node.NodeID
	viewNum := node.CurrentView

	slog.Info("CommitedLocked called", "seq Number", req.Committed.SequenceNumber)

	if node.Status == NodeStopped {
		slog.Info("Node is stopped", "nodeID", nodeID)
		resp.Ack = false
		resp.Message = "node is stopped"
		return nil
	}

	//NOTE: I will re use this function during view change so do not check this, the consensus is driven by the leader from Submit normal, so a check in Pre-prepare is fine
	// if node.ViewChangeInProgress {
	// 	slog.Info("commitedLocked view change in progress", "nodeID", nodeID, "new-view", viewNum)
	// 	resp.Ack = false
	// 	resp.Message = "view change in progress"
	// 	return nil
	// }

	//verify the signature
	leaderID := node.getLeader()
	publicKey := node.getPublicKey(leaderID)
	if len(publicKey) != ed25519.PublicKeySize {
		slog.Error("leader public key missing or invalid", "nodeID", nodeID, "leaderID", leaderID)
		resp.Ack = false
		resp.Message = "signature auth failed"
		return nil
	}

	commitPayload, err := json.Marshal(req.Committed)
	if err != nil {
		slog.Error("failed to marshal commited for signature verification", "nodeID", nodeID, "error", err)
		resp.Ack = false
		resp.Message = "failed to convert comiited struct to bytes"
		return err
	}

	if !ed25519.Verify(publicKey, commitPayload, req.CommitSignature) {
		slog.Warn("invalid pre-prepare signature", "nodeID", nodeID, "leaderID", leaderID)
		resp.Ack = false
		resp.Message = "invalid signature"
		return nil
	}

	//check if view is the same
	if req.Committed.ViewNumber != viewNum {
		slog.Warn("view number different", "nodeID", nodeID, "leaderID", leaderID, "currentView", viewNum, "recieved view", req.Committed.ViewNumber)
		resp.Ack = false
		resp.Message = "view number is different"
		return nil
	}

	//check if n is valid watermark range
	if req.Committed.SequenceNumber > (node.LastStableCheckpoint + node.CheckpointInterval) {
		slog.Warn("sequence number higher than watermark", "nodeID", nodeID, "leaderID", leaderID, "waterMark", (node.LastStableCheckpoint + node.CheckpointInterval), "recieved seq", req.Committed.SequenceNumber)
		resp.Ack = false
		resp.Message = "sequence number more than watermark"
		return nil
	}

	//NOTE: keep it simple no need for functions n all
	requiredCommitCert := 2*2 + 1

	if _, ok := node.verifyCommitCertificate(req.Committed, req.CommitCert, requiredCommitCert); !ok {
		slog.Warn("commit certificate invalid", "nodeID", nodeID, "seq", req.Committed.SequenceNumber)
		resp.Ack = false
		resp.Message = "invalid commit certificate"
		return nil
	}

	//no need to resize logs here since we definitely need to have a prepared entry in the log
	if req.Committed.SequenceNumber >= len(node.Log.LogEntries) {
		slog.Warn("node does not have prepared log and trying to commit", "nodeID", nodeID, "leaderID", leaderID, "recieved seq", req.Committed.SequenceNumber)
		resp.Ack = false
		resp.Message = "message not prepared at seq number "
		return nil
	}

	// since the leader has sent a commited message => the leader must have recieved 2f+1 matching commit messages
	// check if node has prepare message and add prepare to logs
	logEntry := node.Log.LogEntries[req.Committed.SequenceNumber]

	// in order to accept the commited message, I should have a prepared message with matching digest and view and sequence number
	validStatus := logEntry.Status == StatusPrepared || logEntry.Status == StatusCommitted || logEntry.Status == StatusExecuted
	if !validStatus || logEntry.ViewNumber != req.Committed.ViewNumber || logEntry.Digest != Digest(req.Committed.Digest) {
		//reject Prepared:
		slog.Warn("request not prepared or view number or digest did not match", "nodeID", nodeID, "leaderID", leaderID, "recieved seq", req.Committed.SequenceNumber, "log status", logEntry.Status)
		resp.Ack = false
		resp.Message = "status-view number-digest does not match"
		return nil
	} else {
		if logEntry.Status == StatusPrepared {
			logEntry.Status = StatusCommitted
			node.Log.LogEntries[req.Committed.SequenceNumber] = logEntry
		}
	}

	// ======================== ATTACK: IN DARK ========================
	// we do not do anything after recieving response of Commited RPC so no need really to block the Communication

	//make commit messages
	resp.Ack = true
	slog.Info("sequence number commited, proceeding to execution", "seqNum", req.Committed.SequenceNumber)
	resp.Message = "all good"

	go node.execute()

	return nil
}

func (node *Node) execute() {
	slog.Info("execute called")
	node.Log.LogMutex.Lock()

	lastExecuted := node.Log.ExecutedIndex
	for lastExecuted >= 0 {
		if lastExecuted >= len(node.Log.LogEntries) {
			lastExecuted--
			continue
		}
		entry := node.Log.LogEntries[lastExecuted]
		if entry != nil && entry.Status == StatusExecuted {
			break
		}
		lastExecuted--
	}

	startIdx := lastExecuted + 1
	if startIdx < 0 {
		startIdx = 0
	}
	slog.Info("execution loop started", "nodeID", node.NodeID, "startSeq", startIdx)

	replies := make([]common.ReplySigned, 0)
	executedSomething := false

	for idx := startIdx; idx < len(node.Log.LogEntries); idx++ {
		entry := node.Log.LogEntries[idx]

		//Case: Gap in logs
		if entry == nil || isEmptyLogEntry(entry) {
			break
		}

		//NOTE: update the clientRequestIndex log to
		node.Log.ClientRequestIndexLock.Lock()
		node.Log.ClientRequestIndex[entry.Txn.TransactionID.UniqueID] = idx
		node.Log.ClientRequestIndexLock.Unlock()

		// Status is not committed or executed, the status executed check is not really required but leave it here for now
		if entry.Status != StatusCommitted && entry.Status != StatusExecuted {
			break
		}
		// If transaction is executed then do nothing
		if entry.Status == StatusExecuted {
			node.Log.ExecutedIndex = idx
			continue
		}

		txn := entry.Txn
		result := common.NodeResponse{
			TransactionType: txn.TransactionVal.TransactionType,
		}

		switch result.TransactionType {
		case common.RequestTransfer:
			transfer := txn.TransactionVal.TransactionTransfer
			if transfer == nil {
				result.Status = false
				result.Message = "invalid transfer payload"
			} else {
				fromBal, _, err := node.Bank.Transfer(
					transfer.Sender,
					transfer.Receiver,
					transfer.Amount,
				)
				if err != nil {
					switch err {
					case bank.ErrInsufficient:
						result.Status = true
						result.Message = "insufficient-balance"
						slog.Warn("insufficient balance but txn executed in log", "from", transfer.Sender, "to", transfer.Receiver, "amount", transfer.Amount)
					case bank.ErrNegativeAmount:
						//NOTE: this case should not happen
						result.Status = true
						result.Message = "negative-balance"
						slog.Warn("negative balance but txn executed in log", "from", transfer.Sender)
					case bank.ErrUnknownAccount:
						//NOTE: this case should not happen
						result.Status = true
						result.Message = "unknown-account"
						slog.Warn("unkonwn bank acct txn executed in log", "from", transfer.Sender)
					default:
						result.Status = false
						result.Message = err.Error()
					}
				} else {
					result.Status = true
					result.Message = "transfer executed"
					result.Balance = int(fromBal)
				}
			}
		case common.RequestBalance:
			read := txn.TransactionVal.TransactionRead
			if read == nil {
				result.Status = false
				result.Message = "invalid balance payload"
				slog.Error("invalid payload for balance request")
			} else {
				bal, err := node.Bank.Balance(read.AcctNum)
				if err != nil {
					result.Status = false
					result.Message = err.Error()
					slog.Error("error in reading balance", "acctID", read.AcctNum)
				} else {
					slog.Info("balance fetched", "acctID", read.AcctNum)
					result.Status = true
					result.Message = "balance fetched"
					result.Balance = int(bal)
				}
			}
		case common.RequestNoOp:
			result.Status = true
			result.Message = "noop"
			slog.Debug("executed no-op entry", "nodeID", node.NodeID, "seq", idx)
		default:
			result.Status = false
			result.Message = "unknown transaction type"
			slog.Error("unknown transaction type", "transactionType", result.TransactionType)
		}

		// statemachine tried to execute a transaction, remove that transaction from waiting to execute list and reset timer if needed
		if txnID := entry.Txn.TransactionID.UniqueID; txnID != "" {
			node.lock()
			node.handleRemoveWaitingReqAndTimerLocked(txnID)
			node.unlock()
		}

		entry.Reply = result
		entry.Status = StatusExecuted
		node.Log.LogEntries[idx] = entry
		node.Log.ExecutedIndex = idx
		executedSomething = true
		slog.Info("executed transaction", "nodeID", node.NodeID, "seq", idx, "txnID", txn.TransactionID.UniqueID, "type", result.TransactionType, "status", result.Status, "message", result.Message)

		//successful execution
		if result.Status {
			reply := common.Reply{
				Ack:           true,
				ViewNumber:    entry.ViewNumber,
				TransactionID: txn.TransactionID,
				NodeId:        node.NodeID,
				Result:        result,
			}
			if reply.ViewNumber == 0 {
				reply.ViewNumber = node.CurrentView
			}
			payload, err := json.Marshal(reply)
			if err != nil {
				slog.Error("failed to marshal reply for signing", "nodeID", node.NodeID, "seqNum", idx, "error", err)
			} else {
				signature := generateSignatureHelper(node.PrivateKey, payload, node.InvalidSignatureAttackFlag)
				replies = append(replies, common.ReplySigned{
					ReplyPayload:   reply,
					ReplySignature: signature,
				})
				slog.Info("queued execution reply", "nodeID", node.NodeID, "client", txn.TransactionID.ClientID, "txnID", txn.TransactionID.UniqueID, "seq", idx)
			}
		}
	}

	node.Log.LogMutex.Unlock()

	if executedSomething {
		node.lock()
		node.pendingRequests = make(map[string]struct{})
		node.resetViewChangeBackoffLocked()
		node.unlock()
	}

	//reply to client about all the executed transactions

	// ============= WORKING CODE =============
	// for _, reply := range replies {
	// 	go func() {
	// 		addr := node.ClientPort
	// 		timeout := node.RPCTimeout
	// 		if timeout <= 0 {
	// 			timeout = defaultRPCTimeout
	// 		}
	// 		// node.maybeDelay(isByz, behaviour)
	// 		conn, err := net.DialTimeout("tcp", addr, timeout)
	// 		if err != nil {
	// 			slog.Error("failed to dial client for exec result", "nodeID", node.NodeID, "addr", addr, "error", err)
	// 			return
	// 		}
	// 		defer conn.Close()

	// 		rpcClient := rpc.NewClient(conn)
	// 		defer rpcClient.Close()

	// 		var resp common.ReplyReply
	// 		if err := rpcClient.Call("ExecResult.ExecutionResult", &reply, &resp); err != nil {
	// 			slog.Error("failed to invoke client exec result", "nodeID", node.NodeID, "addr", addr, "error", err)
	// 		} else {
	// 			slog.Debug("delivered execution reply", "nodeID", node.NodeID, "client", reply.ReplyPayload.TransactionID.ClientID, "txnID", reply.ReplyPayload.TransactionID.UniqueID)
	// 		}
	// 	}()
	// }

	// ============= New CODE =============
	for _, reply := range replies {
		go node.replyToCilentLocked(&reply)
	}
}

func (node *Node) replyToCilentLocked(reply *common.ReplySigned) {
	addr := node.ClientPort
	timeout := node.RPCTimeout
	if timeout <= 0 {
		timeout = defaultRPCTimeout
	}
	// node.maybeDelay(isByz, behaviour)
	node.warnIfMuHeld("dial-client-reply")
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		slog.Error("failed to dial client for exec result", "nodeID", node.NodeID, "addr", addr, "error", err)
		return
	}
	defer conn.Close()

	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()

	var resp common.ReplyReply
	if err := rpcClient.Call("ExecResult.ExecutionResult", &reply, &resp); err != nil {
		slog.Error("failed to invoke client exec result", "nodeID", node.NodeID, "addr", addr, "error", err)
	} else {
		slog.Debug("delivered execution reply", "nodeID", node.NodeID, "client", reply.ReplyPayload.TransactionID.ClientID, "txnID", reply.ReplyPayload.TransactionID.UniqueID)
	}
}

const defaultRPCTimeout = 2 * time.Second

func (node *Node) verifyCommitCertificate(committed Committed, bundle []CommitSigned, required int) ([]CommitSigned, bool) {
	if len(bundle) < required || required <= 0 {
		return nil, false
	}

	valid := make([]CommitSigned, 0, required)
	unique := make(map[int]struct{}, required)

	for _, signed := range bundle {
		commit := signed.Commit
		if !commit.Ack ||
			commit.ViewNumber != committed.ViewNumber ||
			commit.SequenceNumber != committed.SequenceNumber ||
			!bytes.Equal(commit.Digest, committed.Digest) {
			continue
		}

		if _, exists := unique[commit.NodeID]; exists {
			continue
		}

		pub := node.getPublicKey(commit.NodeID)
		if len(pub) != ed25519.PublicKeySize {
			continue
		}

		payload, err := json.Marshal(commit)
		if err != nil {
			continue
		}

		if !ed25519.Verify(pub, payload, signed.CommitSignature) {
			continue
		}

		unique[commit.NodeID] = struct{}{}
		valid = append(valid, cloneCommitSigned(signed))

		if len(unique) >= required {
			break
		}
	}

	if len(unique) < required {
		return nil, false
	}

	return valid, true
}
