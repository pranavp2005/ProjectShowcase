package server

import (
	"crypto/ed25519"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"pranavpateriya.com/bft/common"
	"pranavpateriya.com/bft/server/persistence/bank"
)

// ============================== Node to Port Map ==============================

var nodeIDtoPortMap map[int]string
var portToNodeIDMap map[string]int

// NOTE:
const DefaultHighWatermark = 100

func init() {
	nodeIDtoPortMap = make(map[int]string)
	portToNodeIDMap = make(map[string]int)

	for nodeID := 1; nodeID <= 7; nodeID++ {
		port := fmt.Sprintf("%d", 8000+nodeID)
		nodeIDtoPortMap[nodeID] = port
		portToNodeIDMap[port] = nodeID
	}
}

// ====================================================================
type NodeStatus string

const (
	NodeStopped NodeStatus = "stopped"
	NodeActive  NodeStatus = "active"
)

type NodeRole string

const (
	RoleLeader  NodeRole = "leader"
	RoleReplica NodeRole = "replica"
)

type Node struct {
	//mutexes
	mu            sync.Mutex
	muHeld        atomic.Bool
	readOnlyMutex sync.RWMutex

	NodeID     int
	PortNumber int
	Address    string
	Peers      []string //Peers is not modified once assgigned during startup
	ClientPort string

	PrivateKey         ed25519.PrivateKey //this is static and will not change during runtime
	NodePublicKeyMaps  map[int]ed25519.PublicKey
	ClientPublicKeyMap map[string]ed25519.PublicKey

	Status               NodeStatus
	Role                 NodeRole
	CurrentView          int
	HigestSequenceNumber int //only useful for the leader, sice leader inceases the Highest sequence number when it gets client request

	LastStableCheckpoint int //always stays 0 because I am not implementing checkpointing

	ViewChangeInProgress bool

	Log NodeLog

	RPCTimeout time.Duration

	Bank bank.BankStore

	// ===== Byzantine Flags =====
	//0. Is Byzantine
	IsByzantine bool

	//1. Invalid Signature
	InvalidSignatureAttackFlag bool

	//2. InDarkAttack
	InDarkAttackFlag bool
	InDarkTargets    []int //this list will not change until Byzantine RPC is called so it can be considered safe to access without deep copy

	//3. CrashAttack
	CrashAttackFlag bool

	//4. TimingAttack
	TimingAttackFlag bool
	TimeDelay        int //this will be mili seconds

	//5. Equivocation
	EquivocationAttackFlag bool
	EquivocationSeqNtarget []int //in equivocation the primary sends seq n to this array all other nodes it sends n+1

	// ===== Timer(liveness) =====

	ViewChangeHistory []NewViewRecord

	CheckpointInterval int //Highwatermark = last stable checkpoint(default 0) + CheckpointInterval

	viewChangeCache    map[int]map[int]*ViewChangeSigned
	recoveryViewProofs map[int]map[int]PrepareProof

	ViewChangeTimeout time.Duration

	pendingRequestsLock sync.Mutex
	pendingRequests     map[string]struct{}

	replicaTimerStop    chan struct{}
	replicaTimerRunning bool

	viewChangeSent        map[int]bool
	nextViewChangeTimeout time.Duration
	higherViewVotes       map[int]map[int]struct{}

	pendingViewNumber int

	// Expected O digests chosen for a view: view -> seq -> digest
	newViewODigest map[int]map[int]Digest
}

type PublicKeyMaps struct {
	NodeKeys   map[int]ed25519.PublicKey
	ClientKeys map[string]ed25519.PublicKey
}

// NodeLog bundles the log entries associated with a replica.
type NodeLog struct {
	LogMutex               sync.Mutex
	LogEntries             []*LogEntry
	ExecutedIndex          int
	ClientRequestIndexLock sync.Mutex
	ClientRequestIndex     map[string]int
}

// StatusCode captures the replica-local status of a specific request.
type LogStatusCode string

const (
	// StatusNone indicates no information for the sequence/request.
	StatusNone LogStatusCode = "X"
	// StatusPrePrepared indicates the pre-prepare phase completed.
	StatusPrePrepared LogStatusCode = "PP"
	// StatusPrepared indicates quorum prepares collected.
	StatusPrepared LogStatusCode = "P"
	// StatusCommitted indicates quorum commits collected.
	StatusCommitted LogStatusCode = "C"
	// StatusExecuted indicates request executed applies to state.
	StatusExecuted LogStatusCode = "E"
)

type LogEntry struct {
	IsNoOp         bool
	ViewNumber     int
	Txn            common.ClientReq
	Status         LogStatusCode
	Reply          common.NodeResponse //will store after execution
	Digest         Digest
	PrepareCert    []PrepareSigned
	PrePrepareCert PrePrepareSigned
}

// PrepareProof bundles the certificates proving a request is prepared.
type PrepareProof struct {
	PrePrepareMessage PrePrepareMessage
	Prepares          []PrepareSigned
}

// PrepareSet contains prepare proofs keyed by sequence number.
type PrepareSet map[int]PrepareProof

// ============================== Pre-Prepare ==============================
type Digest = [32]byte

type PrePrepare struct {
	ViewNumber  int
	SequenceNum int
	Digest      Digest
}

type PrePrepareSigned struct {
	PrePrepare  PrePrepare
	PPSignature []byte
}

type PrePrepareMessage struct {
	PrePrepareSig PrePrepareSigned
	Transaction   common.ClientReq
}

// Prepare is the response to pre-prepare
// <PREPARE, v, n, d, i>
type Prepare struct {
	Ack            bool
	NodeID         int
	SequenceNumber int
	Digest         [32]byte
	ViewNumber     int
}

// <PREPARE, v, n, d, i>sigma-l
type PrepareSigned struct {
	PrePrepareResponse Prepare
	PRSignature        []byte
}

// ============================== Prepared ==============================

// <PREPARED, v, n, d, i>
type PreparedMessage struct {
	ViewNumber     int    // v
	SequenceNumber int    // n
	Digest         []byte // d
	NodeID         int    // i
}

// Leader sends prepared and get commmit in reply
// <PREPARED, v, n, d, i>sigma-l
type PreparedMessageSigned struct {
	PrepareMessage PreparedMessage
	PrepareMsgSign []byte
	PrepareCert    []PrepareSigned //certificate bundle that leader recieved enough prepare messages
}

// <COMMIT, v,n, D,i>
type Commit struct {
	Ack            bool
	ViewNumber     int
	SequenceNumber int
	Digest         []byte
	NodeID         int
}

// <COMMIT, v,n, D,i>sigma-i
type CommitSigned struct {
	Commit          Commit
	CommitSignature []byte
}

// ============================== Commited ==============================

type Committed struct {
	ViewNumber     int
	SequenceNumber int
	Digest         []byte
	NodeID         int
}

type CommitedSigned struct {
	Committed       Committed
	CommitSignature []byte
	CommitCert      []CommitSigned
}

type CommitedReply struct {
	Ack     bool
	Message string
}

// ============================== Set Byzantine ==============================

// SetByzantineRequest carries the configuration for enabling Byzantine behaviour.
type SetByzantineRequest struct {
	Attacks map[string]string
}

// SetByzantineResponse acknowledges the state change.
type SetByzantineResponse struct {
	Ack         bool
	NodeID      int
	ViewNumber  int
	IsByzantine bool
}

// ============================== Un-Set Byzantine ==============================

// SetNotByzantineRequest is a placeholder for future extensions.
type SetNotByzantineRequest struct{}

// ============================== Replica failure and resume =====================

type ReplicaFailureArgs struct {
	Reason string
}

type ReplicaFailureReply struct {
	Stopped bool
	Message string
}

type ReplicaResumeArgs struct {
	Reason string
}

type ReplicaResumeReply struct {
	Resumed bool
	Message string
}

// ============================== Role / Print Introspection =====================

type PrintRoleArgs struct {
}

type PrintRoleRes struct {
}

type PrintLogArgs struct {
	IncludeEmpty bool
}

type PrintLogEntry struct {
	Sequence      int
	View          int
	Status        LogStatusCode
	ClientID      string
	TransactionID string
	Type          common.RequestType
	Sender        string
	Receiver      string
	Amount        int
	ReadAccount   string
	Digest        string
}

type PrintLogReply struct {
	NodeID  int
	Entries []PrintLogEntry
}

type PrintDBArgs struct {
}

type PrintDBReply struct {
	NodeID   int
	Balances map[string]int
}

type PrintStatusArgs struct {
	Sequence int
}

type PrintStatusReply struct {
	NodeID        int
	Sequence      int
	Status        LogStatusCode
	TransactionID string
}

type PrintViewArgs struct {
}

type PrintViewReply struct {
	NodeID int
	Views  []NewViewRecord
}

type ResetStateArgs struct {
	Reason string
}

type ResetStateReply struct {
	Flushed bool
	Message string
}

// ============================== Checkpoint ==============================
// <CHECKPOINT, n, d, i>
type Checkpoint struct {
	LastExecuteForCheckpoint int
	StateDigest              [32]byte
	NodeId                   int
}

// <CHECKPOINT, n, d, i>sigma-i
type CheckpointSigned struct {
	Checkpoint          Checkpoint
	CheckpointSignature []byte
}

// ============================== View Change ==============================

// <VIEW-CHANGE, v+1, n, C, P, i>
type ViewChange struct {
	NewViewNumber        int
	LastStableCheckpoint int
	// Checkpointproof
	Pset        PrepareSet
	FromReplica int
}

// <VIEW-CHANGE, v+1, n, C, P, i>sigma-i
type ViewChangeSigned struct {
	ViewChange ViewChange
	Signature  []byte
}

type ViewChangeReply struct {
	Ack        bool
	ViewNumber int
	Reason     string
}

// <NEW-VIEW, v+1, V, O>
type NewView struct {
	NewViewNumber        int
	Primary              int
	LastStableCheckpoint int
	ProofDigests         map[int]string
	ViewChangeSenders    []int
	ViewChanges          []ViewChangeSigned
}

// <NEW-VIEW, v+1, V, O>sigma-p
type NewViewSigned struct {
	NewView   NewView
	Signature []byte
}

type NewViewReply struct {
	Ack        bool
	ViewNumber int
	Reason     string
}

// NewViewRecord retains the details of a constructed new-view message for PrintView.
type NewViewRecord struct {
	View              int
	Primary           int
	ViewChangeSenders []int
	IncludedProofs    map[int]string
	Time              time.Time
}
