package server

import (
	"crypto/ed25519"
	"log/slog"
	"slices"
	"time"
)

type SetByzantine struct {
	IsByzantine bool

	InvalidSignatureAttackFlag bool

	InDarkAttackFlag bool
	InDarkTarget     []int

	CrashAttackFlag bool

	TimingAttackFlag bool
	TimeDelay        int //this will be mili seconds

	EquivocationAttackFlag bool
	EquivocationSeqNtarget []int
}

type SetByzantineReply struct {
	Ack     bool
	Message string
}

func (node *Node) SetByzantineBehaviour(req *SetByzantine, resp *SetByzantineReply) error {
	node.lock()
	defer node.unlock()

	slog.Info("setting byzantine behaviour in node", "byzConfig", req)

	node.IsByzantine = req.IsByzantine

	node.InvalidSignatureAttackFlag = req.InvalidSignatureAttackFlag

	node.InDarkAttackFlag = req.InDarkAttackFlag
	node.InDarkTargets = req.InDarkTarget

	node.CrashAttackFlag = req.CrashAttackFlag

	node.TimingAttackFlag = req.TimingAttackFlag
	node.TimeDelay = req.TimeDelay

	node.EquivocationAttackFlag = req.EquivocationAttackFlag
	node.EquivocationSeqNtarget = req.EquivocationSeqNtarget

	resp.Ack = true
	resp.Message = "set byzantine flags"

	return nil
}

func (node *Node) UnSetByzantineBehaviour(req *SetByzantine, resp *SetByzantineReply) error {
	node.lock()
	defer node.unlock()

	slog.Info("clear byzantine behaviour in node")

	node.IsByzantine = false

	node.InvalidSignatureAttackFlag = false

	node.InDarkAttackFlag = false
	node.InDarkTargets = []int{}

	node.CrashAttackFlag = false

	node.TimingAttackFlag = false
	node.TimeDelay = 0

	node.EquivocationAttackFlag = false
	node.EquivocationSeqNtarget = []int{}

	resp.Ack = true
	resp.Message = "unset byzantine flags"

	return nil
}

// =============== Byzantine Helper functions ===============

// =============== Invalid Signature Attack===============

// generateSignatureHelper takes the payload checks if it need to currupt the signature based on flag
// returns either the correct or currupted signature
func generateSignatureHelper(privateKey ed25519.PrivateKey, payload []byte, signAttack bool) []byte {
	// Option 1: Corrupt the valid signature (simplest)
	validSig := ed25519.Sign(privateKey, payload)
	if signAttack {
		validSig[0] ^= 0xFF // Flip first byte
	}
	return validSig
}

// =============== In-Dark from Leader Attack ===============
func extractPortFast(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[i+1:]
		}
	}
	return addr
}

func getPeerlistExcludingSelf(self string, allPeers []string, inDarkFlag bool, inDarkPeers []int) []string {
	selfPort := extractPortFast(self)
	capacity := len(allPeers) - 1
	if inDarkFlag {
		capacity = len(allPeers) - 1 - len(inDarkPeers)
	}
	result := make([]string, 0, capacity)

	if !inDarkFlag {
		for _, peer := range allPeers {
			if extractPortFast(peer) != selfPort {
				result = append(result, peer)
			}
		}
		return result
	}

	inDarkSet := make(map[int]struct{}, len(inDarkPeers))
	for _, darkNodeID := range inDarkPeers {
		inDarkSet[darkNodeID] = struct{}{}
	}

	for _, peer := range allPeers {
		if extractPortFast(peer) == selfPort {
			continue
		}

		port := extractPortFast(peer)
		if nodeID, exists := portToNodeIDMap[port]; exists {
			if _, isDark := inDarkSet[nodeID]; isDark {
				continue
			}
		}

		result = append(result, peer)
	}

	return result
}

func getPeerlistIncludingSelf(self string, allPeers []string, inDarkFlag bool, inDarkPeers []int) []string {
	capacity := len(allPeers)
	if inDarkFlag {
		capacity = len(allPeers) - len(inDarkPeers)
	}
	result := make([]string, 0, capacity)

	if !inDarkFlag {
		return append(result, allPeers...)
	}

	inDarkSet := make(map[int]struct{}, len(inDarkPeers))
	for _, darkNodeID := range inDarkPeers {
		inDarkSet[darkNodeID] = struct{}{}
	}

	for _, peer := range allPeers {
		port := extractPortFast(peer)
		if nodeID, exists := portToNodeIDMap[port]; exists {
			if _, isDark := inDarkSet[nodeID]; isDark {
				continue
			}
		}

		result = append(result, peer)
	}

	return result
}

// =============== In-Dark from Replica Attack ===============
// TODO: install this in Pre-PrepareRPC, Prepare and Commit RPC as well
func shouldRespondToLeader(inDarkFlag bool, leaderNodeID int, inDarkPeers []int) bool {
	if !inDarkFlag {
		return true
	}
	return !slices.Contains(inDarkPeers, leaderNodeID)
}

// =============== Timing Attack ===============
// delayTa sleeps for timeDelay milliseconds and returns true when done.
func delayTa(timeDelay int) bool {
	time.Sleep(time.Duration(timeDelay) * time.Millisecond)
	return true
}

// =============== In-Dark from Replica ===============

// =============== In-Dark from Replica ===============
