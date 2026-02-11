package server

import (
	"encoding/hex"
	"fmt"
	"log/slog"

	"pranavpateriya.com/bft/server/persistence/bank"
)

func (n *Node) ReplicaFailure(args *ReplicaFailureArgs, reply *ReplicaFailureReply) error {
	n.lock()
	alreadyStopped := n.Status == NodeStopped
	if !alreadyStopped {
		n.Status = NodeStopped
		n.Role = RoleReplica
	}
	n.unlock()

	if reply != nil {
		if alreadyStopped {
			reply.Stopped = false
			reply.Message = "node already stopped"
		} else {
			reply.Stopped = true
			reply.Message = "node transitioned to stopped"
		}
	}
	return nil
}

func (n *Node) ReplicaResume(args *ReplicaResumeArgs, reply *ReplicaResumeReply) error {
	n.lock()
	wasStopped := n.Status == NodeStopped
	n.Status = NodeActive
	if wasStopped {
		n.Role = RoleReplica
	}
	n.unlock()

	if reply != nil {
		if wasStopped {
			reply.Resumed = true
			reply.Message = "node resumed as backup"
		} else {
			reply.Resumed = false
			reply.Message = "node already active"
		}
	}
	return nil
}

func (n *Node) PrintRole(args *PrintRoleArgs, reply *PrintRoleRes) error {
	n.lock()
	defer n.unlock()
	slog.Info("print role", "nodeID", n.NodeID, "role", n.Role, "status", n.Status)
	return nil
}

func (n *Node) PrintDB(args *PrintDBArgs, reply *PrintDBReply) error {
	if reply == nil {
		return nil
	}
	reply.NodeID = n.NodeID
	if n.Bank == nil {
		reply.Balances = map[string]int{}
		return nil
	}
	reply.Balances = n.Bank.Snapshot()
	return nil
}

func (n *Node) PrintLog(args *PrintLogArgs, reply *PrintLogReply) error {
	if reply == nil {
		return nil
	}
	reply.NodeID = n.NodeID

	n.Log.LogMutex.Lock()
	defer n.Log.LogMutex.Unlock()

	entries := make([]PrintLogEntry, 0, len(n.Log.LogEntries))
	for seq, entry := range n.Log.LogEntries {
		// Handle nil entries
		if entry == nil {
			entries = append(entries, PrintLogEntry{
				Sequence: seq,
				Status:   StatusNone,
			})
			continue
		}

		// Include all entries (empty or not)
		txn := entry.Txn
		logEntry := PrintLogEntry{
			Sequence:      seq,
			View:          entry.ViewNumber,
			Status:        entry.Status,
			ClientID:      txn.TransactionID.ClientID,
			TransactionID: txn.TransactionID.UniqueID,
			Type:          txn.TransactionVal.TransactionType,
			Digest:        digestToHex(entry.Digest),
		}

		if transfer := txn.TransactionVal.TransactionTransfer; transfer != nil {
			logEntry.Sender = transfer.Sender
			logEntry.Receiver = transfer.Receiver
			logEntry.Amount = transfer.Amount
		}
		if read := txn.TransactionVal.TransactionRead; read != nil {
			logEntry.ReadAccount = read.AcctNum
		}

		entries = append(entries, logEntry)
	}

	reply.Entries = entries
	return nil
}

func (n *Node) PrintStatus(args *PrintStatusArgs, reply *PrintStatusReply) error {
	if reply == nil {
		return nil
	}

	seq := -1
	if args != nil {
		seq = args.Sequence
	}
	reply.NodeID = n.NodeID
	reply.Sequence = seq

	n.Log.LogMutex.Lock()
	defer n.Log.LogMutex.Unlock()

	if seq >= 0 && seq < len(n.Log.LogEntries) {
		entry := n.Log.LogEntries[seq]
		if entry != nil && !isEmptyLogEntry(entry) {
			reply.Status = entry.Status
			reply.TransactionID = entry.Txn.TransactionID.UniqueID
			return nil
		}
	}

	reply.Status = StatusNone
	reply.TransactionID = ""
	return nil
}

func (n *Node) PrintView(args *PrintViewArgs, reply *PrintViewReply) error {
	if reply == nil {
		return nil
	}
	reply.NodeID = n.NodeID

	n.lock()
	history := copyViewRecords(n.ViewChangeHistory)
	n.unlock()

	reply.Views = history
	return nil
}

func (n *Node) ResetState(args *ResetStateArgs, reply *ResetStateReply) error {
	n.Log.LogMutex.Lock()
	defer n.Log.LogMutex.Unlock()

	n.lock()
	n.stopReplicaTimerLocked("replica-reset-state")
	n.resetViewChangeBackoffLocked()
	n.Status = NodeActive
	n.Role = RoleReplica
	n.CurrentView = 1
	n.ViewChangeInProgress = false
	n.HigestSequenceNumber = -1
	n.LastStableCheckpoint = 0
	n.CheckpointInterval = DefaultHighWatermark
	n.pendingRequests = make(map[string]struct{})
	n.pendingViewNumber = 0
	n.ViewChangeHistory = nil
	n.viewChangeCache = nil
	n.recoveryViewProofs = nil
	// n.cancelAllViewChangeTimersLocked()
	// n.viewChangeTimers = nil
	n.viewChangeSent = nil
	n.nextViewChangeTimeout = 0
	n.higherViewVotes = nil
	n.newViewODigest = nil
	n.Log.LogEntries = make([]*LogEntry, 0)
	n.Log.ClientRequestIndex = make(map[string]int)
	n.Log.ExecutedIndex = -1
	n.InitLivenessState()
	err := n.resetBankStoreLocked()
	n.unlock()

	if reply != nil {
		if err != nil {
			reply.Flushed = false
			reply.Message = fmt.Sprintf("bank reset failed: %v", err)
		} else {
			reply.Flushed = true
			reply.Message = "state reset"
		}
	}

	if err != nil {
		slog.Error("bank reset failed", "nodeID", n.NodeID, "error", err)
	}
	return err
}

func (n *Node) resetBankStoreLocked() error {
	if n.Bank == nil {
		store, err := bank.NewBankStore(n.NodeID)
		if err != nil {
			return err
		}
		n.Bank = store
		return nil
	}

	type resettable interface {
		Reset() error
	}

	if store, ok := n.Bank.(resettable); ok {
		return store.Reset()
	}

	store, err := bank.NewBankStore(n.NodeID)
	if err != nil {
		return err
	}
	n.Bank = store
	return nil
}

func copyViewRecords(history []NewViewRecord) []NewViewRecord {
	if len(history) == 0 {
		return nil
	}
	result := make([]NewViewRecord, len(history))
	for i, rec := range history {
		copied := NewViewRecord{
			View:    rec.View,
			Primary: rec.Primary,
			Time:    rec.Time,
		}
		if len(rec.ViewChangeSenders) > 0 {
			copied.ViewChangeSenders = append([]int(nil), rec.ViewChangeSenders...)
		}
		if len(rec.IncludedProofs) > 0 {
			copied.IncludedProofs = make(map[int]string, len(rec.IncludedProofs))
			for seq, digest := range rec.IncludedProofs {
				copied.IncludedProofs[seq] = digest
			}
		}
		result[i] = copied
	}
	return result
}

func digestToHex(d Digest) string {
	if d == ([32]byte{}) {
		return ""
	}
	dst := make([]byte, hex.EncodedLen(len(d)))
	hex.Encode(dst, d[:])
	return string(dst)
}
