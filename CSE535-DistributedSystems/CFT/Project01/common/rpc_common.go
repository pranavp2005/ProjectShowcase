package common

import "time"

// -------- Transaction RPCS --------
type SubmitTxArgs struct{ Tx Transaction }
type SubmitTxReply struct {
	Success    bool
	Message    string
	LeaderHint string
}

// -------- Introspection RPCS --------
type LogEntryMetadata struct {
	Sequence       int
	Status         SequenceStatus
	BallotN        int
	BallotNode     int
	IsNoOp         bool
	TransactionReq TransactionReq
}

type PrintLogArgs struct{}
type PrintLogReply struct {
	Success bool
	Message string
	Entries []LogEntryMetadata
}

type PrintDBArgs struct{}
type PrintDBReply struct {
	Success  bool
	Message  string
	Balances map[string]int64
}

type PrintStatusArgs struct {
	Sequence int
}
type PrintStatusReply struct {
	Success       bool
	Message       string
	NodeID        int
	Sequence      int
	Status        SequenceStatus
	ExecutedIndex int
}

type NewViewMetadata struct {
	FromLeader int
	BallotN    int
	BallotNode int
	ReceivedAt time.Time
	Entries    []LogEntryMetadata
}

type PrintViewArgs struct{}
type PrintViewReply struct {
	Success bool
	Message string
	Views   []NewViewMetadata
}

// -------- Internal Control RPCS --------
type KilNodeArgs struct {
	Tx Transaction
}
type KillNodeReply struct {
	Success    bool
	Message    string
	LeaderHint string // optionally filled by a non-leader server
}
