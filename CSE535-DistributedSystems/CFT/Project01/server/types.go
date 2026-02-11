// file name types.go
package main

import (
	"log/slog"
	"sync"
	"time"

	"pranavpateriya.com/multipaxos/common"
	"pranavpateriya.com/multipaxos/server/persistence/statemachine"
)

// TODO: Have separate locks for each
type Node struct {
	mu            sync.Mutex
	readOnlyMutex sync.RWMutex

	Status common.NodeStatus
	Role   common.NodeRole
	NodeID int
	// LastKnownLeader int //don't need this for now

	SystemStartup bool
	Log           NodeLog

	SelfBallot Ballot // set when you are leader

	PortNumber int
	Address    string

	SequenceNumber int
	Promised       Ballot // highest ballot seen/promised
	RPCTimeout     time.Duration

	// Cluster
	Peers []string
	Bank  statemachine.BankStore

	// ===== Timer Strategy 1 (liveness) =====
	timer        *time.Timer // liveness timer (backups)
	expireAt     time.Time   // when current timer expires
	timerRunning bool
	t            time.Duration // liveness timeout (e.g., 900ms)
	tp           time.Duration // do-not-self-elect if a PREPARE seen within tp (e.g., 300ms)
	hbInterval   time.Duration // heartbeat interval sent by leader (e.g., 300ms)
	activityCh   chan struct{} // edge-trigger to reset timer on leader activity
	stopCh       chan struct{} // node shutdown

	// Strategy-1 bookkeeping
	LastSeenPrepare time.Time // last time ANY Prepare was seen
	NewViewHistory  []NewViewRecord
}

type HeartbeatArgs struct {
	Ballot   Ballot
	LeaderID int
}

type HeartbeatReply struct {
	Ok       bool
	Promised Ballot
}

// the sequence number is just the index
type LogEntry struct {
	IsNoOp      bool
	TxnVal      common.TransactionReq
	Status      common.SequenceStatus
	BallotNum   Ballot
	ClientReply common.TransactionClientReply //will store after execution
}

type NodeLog struct {
	LogMutex           sync.Mutex
	LogEntries         []*LogEntry
	ExecutedIndex      int
	ClientRequestIndex map[string]int
}

type NewViewRecord struct {
	FromLeader int
	Ballot     Ballot
	ReceivedAt time.Time
	Entries    []NewViewEntry
}

type NewViewEntry struct {
	Sequence    int
	IsNoOp      bool
	Status      common.SequenceStatus
	Ballot      Ballot
	Transaction common.TransactionReq
}

type Ballot struct {
	N      int // monotonically increasing
	NodeID int // tie-breaker since we only have 5 nodes
}

type LeaderFailureArgs struct {
	Reason string
}

type LeaderFailureReply struct {
	Stopped bool
	Message string
}

type LeaderResumeArgs struct {
	Reason string
}

type PrintRoleArgs struct {
}
type PrintRoleRes struct {
}

type LeaderResumeReply struct {
	Resumed bool
	Message string
}

// Is b less than other ballor
func (b *Ballot) Less(other *Ballot) bool {
	if other == nil {
		slog.Info("the other ballot number is nil")
		return false // A non-nil ballot is less than a nil one.
	}

	// Check if this ballot is nil (e.g., if the method is called on a nil receiver).
	if b == nil {
		slog.Info("ballot b is nil")
		return true // A nil ballot is less than a non-nil one.
	}

	if b.N != other.N {
		return b.N < other.N
	}
	return b.NodeID < other.NodeID
}

func (b Ballot) GreaterEq(other *Ballot) bool {
	// A nil ballot is considered "less than" a non-nil ballot.
	if other == nil {
		return true
	}

	if b.N != other.N {
		return b.N > other.N
	}
	return b.NodeID >= other.NodeID
}

// type AcceptedValues struct {
// 	AcceptedTxn    common.Transaction
// 	AcceptedBallot Ballot
// }

// -------- Leader Election RPCS --------
type PrepareArgs struct {
	Ballot Ballot
	// NextSlot int // leader asks for accepted info from >= NextSlot
}

type AcceptLog struct {
	Slot   int
	Ballot Ballot
	Value  common.TransactionReq
}

type Promise struct {
	Ok          bool
	Promised    Ballot
	AccpetLog   []AcceptLog // this only contains
	MinProposal Ballot      // higher promised if NACK
}

type NewViewArgs struct {
	FromLeader int
	Ballot     Ballot
	// The leader-normalized log: for each seq, the chosen value with leader's ballot.
	Entries []*LogEntry // must be contiguous [1..maxSeen], fill gaps with no-ops
}

type NewViewReply struct {
	Ok            bool
	AcceptedSeqs  []int // which seq were recorded/affirmed at this backup
	AlreadyHadSeq []int // seqs the backup already had (idempotence)
	From          int
	Promised      Ballot // still promised to this ballot
	Message       string
}

// -------- Normal Operations RPCS --------
type AcceptArgs struct {
	Ballot      Ballot
	SequenceNum int
	Value       common.TransactionReq
}

type Accepted struct {
	Ok            bool
	Promised      Ballot // higher promised if NACK
	SequenceNum   int
	Message       string
	ClientRequest common.TransactionReq
	NodeRepliedID int
}

// TODO: In case I plan to make Accepted a different RPC
// type AcceptedArgs struct {
// 	Tx common.Transaction
// }

// type AcceptedReply struct {
// 	Success    bool
// 	Message    string
// 	LeaderHint string // optionally filled by a non-leader server
// }

// -------- Commit RPCS --------
type CommitArgs struct {
	BallotNumber   Ballot
	SequenceNumber int
	ClientRequest  common.TransactionReq
}

type CommitReply struct {
	Status   bool
	Message  string
	Promised Ballot
}

type Execution struct {
	Result  bool
	Message common.ExecutionResultMessage
	Ballot  Ballot
}
