package server

import (
	"log/slog"
	"time"
)

func (node *Node) handleAddWaitingReqAndTimerLocked(txnID string) {
	if node == nil || txnID == "" {
		return
	}

	if node.pendingRequests == nil {
		node.pendingRequests = make(map[string]struct{})
	}

	node.pendingRequests[txnID] = struct{}{}
	if node.replicaTimerRunning {
		return
	}
	node.startReplicaTimerLocked(node.CurrentView, "pending-request")
}

func (node *Node) handleRemoveWaitingReqAndTimerLocked(txnID string) {
	if node == nil || txnID == "" || node.pendingRequests == nil {
		return
	}

	delete(node.pendingRequests, txnID)
	if len(node.pendingRequests) > 0 {
		return
	}

	node.stopReplicaTimerLocked("no-pending-requests")
	node.resetViewChangeBackoffLocked()
}

func (node *Node) stopReplicaTimerLocked(reason string) {
	if !node.replicaTimerRunning {
		return
	}
	if node.replicaTimerStop != nil {
		close(node.replicaTimerStop)
	}
	node.replicaTimerRunning = false
	node.replicaTimerStop = nil
	slog.Info("replica timer stopped", "nodeID", node.NodeID, "currentView", node.CurrentView, "pendingView", node.pendingViewNumber, "reason", reason)
}

func (node *Node) viewTimerLoop(viewAtStart int, stop <-chan struct{}) {
	_ = viewAtStart
	duration := node.timerDurationUnlocked()
	if duration <= 0 {
		duration = 5 * time.Second
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-stop:
		return
	}

	node.lock()
	if node.replicaTimerStop != stop {
		node.unlock()
		return
	}

	node.replicaTimerRunning = false
	node.replicaTimerStop = nil

	if len(node.pendingRequests) == 0 {
		node.unlock()
		return
	}

	previous := node.pendingViewNumber
	if previous == 0 || previous < node.CurrentView {
		previous = node.CurrentView
	}

	if node.pendingViewNumber <= node.CurrentView {
		node.pendingViewNumber = node.CurrentView + 1
	} else {
		node.pendingViewNumber++
	}

	slog.Info("view bump pending", "nodeID", node.NodeID, "oldView", previous, "newView", node.pendingViewNumber, "reason", "timer-expired")

	node.bumpFollowerSuspicionTimeoutLocked()
	target := node.pendingViewNumber
	node.unlock()

	go node.initiateViewChangeUnlocked(target)
}

func (node *Node) startReplicaTimerLocked(viewAtStart int, reason string) {
	if node == nil || node.replicaTimerRunning {
		return
	}

	node.timerDurationLocked()

	stopCh := make(chan struct{})
	node.replicaTimerStop = stopCh
	node.replicaTimerRunning = true

	slog.Info("replica timer started", "nodeID", node.NodeID, "currentView", node.CurrentView, "pendingView", node.pendingViewNumber, "timeout", node.nextViewChangeTimeout, "reason", reason)

	node.unlock()
	go node.viewTimerLoop(viewAtStart, stopCh)
	node.lock()
}

func (node *Node) timerDurationLocked() time.Duration {
	base := node.ViewChangeTimeout
	if base <= 0 {
		base = 5 * time.Second
	}
	if node.nextViewChangeTimeout <= 0 || node.nextViewChangeTimeout < base {
		node.nextViewChangeTimeout = base
	}
	return node.nextViewChangeTimeout
}

func (node *Node) timerDurationUnlocked() time.Duration {
	node.lock()
	defer node.unlock()
	return node.timerDurationLocked()
}

func (node *Node) bumpFollowerSuspicionTimeoutLocked() {
	base := node.ViewChangeTimeout
	if base <= 0 {
		base = 5 * time.Second
	}
	maxTimeout := 5 * base

	current := node.nextViewChangeTimeout
	if current <= 0 {
		current = base
	}

	next := current * 2
	if next > maxTimeout {
		next = maxTimeout
	}
	node.nextViewChangeTimeout = next
}
