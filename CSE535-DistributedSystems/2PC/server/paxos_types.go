package main

import (
	"context"
	"net/rpc"
	"sync"
	"time"

	"pranavpateriya.com/distributed-sys/common"
	"pranavpateriya.com/distributed-sys/persistence"
	"pranavpateriya.com/distributed-sys/server/utils"
)

type clusterLeader struct {
	NodeID  int
	Address string
}

type Node struct {
	NodeID    int
	ClusterID int

	ConfigRole common.NodeRole

	NodeAddress       string
	lastHeartbeatUnix int64

	Status common.NodeStatus

	PaxosModule *PaxosModule

	TwoPCModule *TwoPCModule

	ClusterLeaders map[int]clusterLeader // clusterID -> leader info

	//TODO: Populate this map with details about peers from different cluster
	ClusterAddresses map[int][]string // clusterID -> addresses for broadcast

	AccountIDCluster map[int]int // accountID -> clusterID based on sharding

	//Connection pool
	RPCClients map[string]*rpc.Client
	rpcMu      sync.Mutex

	heartbeatCancelMu sync.Mutex
	heartbeatCancel   context.CancelFunc
}

// TODO: hook up PaxosModule with persistence storage
type PaxosModule struct {
	mu sync.Mutex

	Role common.NodeRole

	PrepareBallotNumber Ballot //PBN
	ActiveBallotNumber  Ballot //ABN update rule: when a node wins election update ABN, if the node recives a NACK beacuse of higher ballot number update ABN

	CurrentLogSlot int

	ReplicationLog NodeLog

	HeartbeatTimer time.Duration

	txnIndex *utils.TxnIndex

	Bank persistence.BankStore
}

type TwoPCModule struct {
	mu sync.Mutex

	CoordinatorTimer time.Duration

	WalService persistence.WalService

	txnIndex *utils.TxnIndex //it holds if transaction was aborted 0 is aborted 1 is prepared
}

type NodeLog struct {
	LogMutex           sync.Mutex
	ExecMutex          sync.Mutex
	LogEntries         []*LogEntry
	LastExecuted       int
	ClientRequestIndex map[string]int
}

type LogEntry struct {
	IsNoOp bool

	BallotNum      Ballot
	SequenceNumber int
	TxID           string

	TxnVal common.TransactionVal

	Status              common.SequenceStatus
	TwoPCSProtocolStage ProtocolPart
	ClientReply         common.ClientReply // TODO: check if this is needed at all will store after execution
}

type Ballot struct {
	SequenceNumber int
	NodeID         int
}

// RPCs

type ProtocolPart string

const (
	TwoPCPrepared  ProtocolPart = "P"
	TwoPCAborted   ProtocolPart = "A"
	TwoPCCommitted ProtocolPart = "C"
	Intrashard     ProtocolPart = "I"
)

// Messages

// type AcceptTxn struct {
// 	Accept Accept
// 	Transaction common.TransactionVal
// }
