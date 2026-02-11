package server

import (
	"crypto/ed25519"
	"log/slog"
)

func (node *Node) ViewChange(req *ViewChangeSigned, resp *ViewChangeReply) error {
	if req == nil || resp == nil {
		return nil
	}

	node.lock()

	resp.ViewNumber = node.CurrentView

	view := req.ViewChange.NewViewNumber
	sender := req.ViewChange.FromReplica

	if view < node.CurrentView+1 {
		node.unlock()
		resp.Ack = false
		resp.Reason = "stale-view"
		return nil
	}

	pub := node.getPublicKey(sender)
	if len(pub) != ed25519.PublicKeySize {
		node.unlock()
		resp.Ack = false
		resp.Reason = "unknown-sender"
		return nil
	}

	valid, err := verifyStruct(req.ViewChange, req.Signature, pub)
	if err != nil || !valid {
		node.unlock()
		resp.Ack = false
		resp.Reason = "invalid-signature"
		return nil
	}

	node.storeViewChangeLocked(req)
	count := len(node.viewChangeCache[view])

	f := (getTotalNodes() - 1) / 3
	slog.Info("view-change votes", "nodeID", node.NodeID, "view", view, "votes", count, "needFPlus1", f+1, "need2FPlus1", 2*f+1)
	var (
		shouldBroadcastVC bool
		broadcastVC       *ViewChangeSigned
		broadcastPeers    []string
	)
	if count >= f+1 {
		node.ViewChangeInProgress = true
		if node.pendingViewNumber < view {
			oldPending := node.pendingViewNumber
			if oldPending == 0 {
				oldPending = node.CurrentView
			}
			node.pendingViewNumber = view
			slog.Info("pending view bump", "nodeID", node.NodeID, "oldView", oldPending, "newView", node.pendingViewNumber, "reason", "view-change-f+1")
		}
		if !node.viewChangeSent[view] {
			broadcastVC, broadcastPeers = node.prepareViewChangeForLocked(view, "view-change-f+1-observed")
			if broadcastVC != nil {
				shouldBroadcastVC = true
			}
		}
	}

	var (
		shouldTryNewView bool
		// shouldRestart    bool
		// timerView        = view
	)

	if count >= 2*f+1 {
		if leaderForView(view) == node.NodeID {
			shouldTryNewView = true
		} else {
			node.bumpFollowerSuspicionTimeoutLocked()
			// shouldRestart = true
		}
	}

	resp.Ack = true
	resp.Reason = "cached"

	node.unlock()

	if shouldBroadcastVC {
		go node.broadcastViewChange(broadcastVC, broadcastPeers)
	}
	if shouldTryNewView {
		go node.tryConstructAndBroadcastNewView(view)
	} 
	// else if shouldRestart {
	// 	node.lock()
	// 	node.stopReplicaTimerLocked("view-change-quorum")
	// 	node.startReplicaTimerLocked(timerView, "suspect-new-view")
	// 	node.unlock()
	// }

	return nil
}
