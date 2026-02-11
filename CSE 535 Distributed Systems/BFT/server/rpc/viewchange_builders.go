package server

// buildPrepareSetLocked scans the log for prepared-or-later entries and builds
// the PrepareSet expected within a view-change message. Caller must hold
// node.mu before invoking this helper.
func (node *Node) buildPrepareSetLocked() PrepareSet {
	if node == nil {
		return nil
	}

	logEntries := node.Log.LogEntries
	if len(logEntries) == 0 {
		return nil
	}

	result := make(PrepareSet)

	for seq, entry := range logEntries {
		if entry == nil || isEmptyLogEntry(entry) {
			continue
		}

		if entry.Status != StatusPrepared &&
			entry.Status != StatusCommitted &&
			entry.Status != StatusExecuted {
			continue
		}

		if len(entry.PrepareCert) == 0 || entry.PrePrepareCert.PPSignature == nil {
			continue
		}

		proof := PrepareProof{
			PrePrepareMessage: PrePrepareMessage{
				PrePrepareSig: clonePrePrepareSigned(entry.PrePrepareCert),
				Transaction:   entry.Txn,
			},
			Prepares: clonePrepareCerts(entry.PrepareCert),
		}

		result[seq] = clonePrepareProof(proof)
	}

	if len(result) == 0 {
		return nil
	}
	return result
}
