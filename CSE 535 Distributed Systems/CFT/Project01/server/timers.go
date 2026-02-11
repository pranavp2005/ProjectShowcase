// file: timers.go
package main

import (
	"log/slog"
	"math/rand"
	"time"

	"pranavpateriya.com/multipaxos/common"
)

// StartStrategy1Timer starts the backup liveness timer + activity loop.
// Call this on every node at startup.
func (n *Node) StartStrategy1Timer(t, tp, hbInterval time.Duration) {
	n.mu.Lock()
	n.t = t
	n.tp = tp
	n.hbInterval = hbInterval

	if n.activityCh == nil {
		n.activityCh = make(chan struct{}, 1)
	}
	if n.stopCh == nil {
		n.stopCh = make(chan struct{})
	}

	// Start immediately so "no leader" will cause expiry after t.
	if n.timer != nil {
		_ = n.timer.Stop()
	}

	jitter := time.Duration(rand.Int63n(int64(n.t / 5))) // up to +20%
	if rand.Intn(2) == 0 {
		n.t += jitter
	} else {
		if n.t > jitter {
			n.t -= jitter
		}
	}
	n.timer = time.NewTimer(n.t)

	n.expireAt = time.Now().Add(n.t)
	n.timerRunning = true
	n.mu.Unlock()

	go n.livenessLoop()
}

func (n *Node) livenessLoop() {
	for {
		select {
		case <-n.stopCh:
			if n.timer != nil && !n.timer.Stop() {
				select {
				case <-n.timer.C:
				default:
				}
			}
			return

		case <-n.activityCh:
			// Leader activity seen — reset timer
			n.mu.Lock()
			if n.timer != nil && !n.timer.Stop() {
				select {
				case <-n.timer.C:
				default:
				}
			}
			n.timer.Reset(n.t)
			n.expireAt = time.Now().Add(n.t)
			n.mu.Unlock()

		case <-n.timer.C:
			// Timer expired -> maybe try to become leader (Strategy 1)
			n.mu.Lock()
			n.timer.Reset(n.t)
			n.expireAt = time.Now().Add(n.t)
			role := n.Role
			status := n.Status
			seenPrepareAgo := time.Since(n.LastSeenPrepare)
			shouldSelfElect := (seenPrepareAgo >= n.tp)
			myID := n.NodeID
			n.mu.Unlock()

			if status != common.StatusActive {
				continue
			}
			if role == common.RoleLeader {
				// Leaders don't use liveness self-election
				continue
			}
			LogProtocol(n.NodeID, "Timer expired: role=%s status=%s shouldElect=%v sincePrepare=%s", role, status, shouldSelfElect, seenPrepareAgo)

			if shouldSelfElect {
				go func() {
					LogProtocol(n.NodeID, "Starting election due to timer expiry")
					if ok, b, err := n.tryBecomeLeader(); err != nil {
						slog.Warn("election failed", slog.Int("node", myID), slog.Any("err", err))
					} else if ok {
						slog.Info("became leader", slog.Int("node", myID), slog.Any("ballot", b))
						// Start heartbeats
						go n.runHeartbeats()
					}
				}()
			} else {
				// We recently saw a Prepare; let that proposer win.
				slog.Info("timer expired but recent PREPARE seen; holding off self-election",
					slog.Int("node", myID), slog.Duration("sincePrepare", seenPrepareAgo))
			}
		}
	}
}

// Called on leader activity (Accept/Commit/NewView/Heartbeat with current or higher ballot)
func (n *Node) bumpOnLeaderActivity() {
	select {
	case n.activityCh <- struct{}{}:
	default:
	}
}

// Utility: remaining time before expiry (used by Prepare handler for blocking)
// let this be here for debugging
func (n *Node) timeToExpiry() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Until(n.expireAt)
}

// StopStrategy1Timer halts the liveness loop and clears timer state safely.
func (n *Node) StopStrategy1Timer() {
	n.mu.Lock()
	stopCh := n.stopCh
	timer := n.timer
	n.stopCh = nil
	n.timer = nil
	n.timerRunning = false
	n.expireAt = time.Time{}
	n.mu.Unlock()

	if timer != nil {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	if stopCh != nil {
		close(stopCh)
	}
}
