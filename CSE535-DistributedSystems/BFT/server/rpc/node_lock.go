package server

import (
	"log/slog"
)

func (node *Node) lock() {
	if node == nil {
		return
	}
	node.mu.Lock()
	node.muHeld.Store(true)
}

func (node *Node) unlock() {
	if node == nil {
		return
	}
	node.muHeld.Store(false)
	node.mu.Unlock()
}

func (node *Node) warnIfMuHeld(op string) {
	if node == nil {
		return
	}
	if node.muHeld.Load() {
		slog.Warn("network op attempted while node.mu held", "nodeID", node.NodeID, "op", op, "view", node.CurrentView)
	}
}
