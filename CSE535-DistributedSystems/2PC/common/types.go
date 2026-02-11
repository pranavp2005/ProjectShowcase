package common

type RequestType int

const (
	RequestTypeRead                RequestType = 0
	RequestTypeInstraShardTransfer RequestType = 1
	RequestTypeCrossshardTransfer  RequestType = 2
)

type ConsistencyLevel int

const (
	Linearizable ConsistencyLevel = 0 //send to leader, leader checks locks and then sends response
	Eventual     ConsistencyLevel = 1 //send to any
	Majority     ConsistencyLevel = 2 //send to all, take majority response
)

// m = <REQUEST,t,τ,c>
// TODO: optimize this so that we can remove the consistency level field from
type ClientReq struct {
	RequestType    RequestType    //part of t
	TranscationVal TransactionVal // part of t
	TransactionID  TransactionID  // τ,c
}

type ClientReqConsLevel struct {
	ClientReq        ClientReq
	ConsistencyLevel ConsistencyLevel
}

// τ,c
type TransactionVal struct {
	From   int
	To     int
	Amount int
}

// part of t
type TransactionID struct {
	TransactionID string
	ClientID      string
}

type ClientReply struct {
	RequestType RequestType
	Balance     int //used only in case of RequestTypeRead and valid only if status is true

	Status  bool
	Message string
}

type NodeRole int

const (
	RoleLeader NodeRole = 1
	RoleBackup NodeRole = 0
)

type NodeStatus int

const (
	StatusActive  NodeStatus = 1
	StatusStopped NodeStatus = 0
)

type SequenceStatus string

const (
	StatusAccepted  SequenceStatus = "A"
	StatusCommitted SequenceStatus = "C"
	StatusExecuted  SequenceStatus = "E"
	StatusNoStatus  SequenceStatus = "X" //Used to represent a gap in log
)

type ResetBalanceReq struct {
	Balances map[int]int
}

type ResetbalanceResp struct {
	Status  bool
	Message string
}

type NodeControlReq struct{}
type NodeControlResp struct {
	Status  bool
	Message string
}

type RecoverWithRoleReq struct {
	Role NodeRole
}

type GetAllAccountsReq struct{}
type GetAllAccountsResp struct {
	Status   bool
	Message  string
	Balances map[int]int
}

type NodeStatusReq struct{}
type NodeStatusResp struct {
	Status    bool
	Message   string
	NodeID    int
	ClusterID int
	Role      NodeRole
	NodeState NodeStatus
}

type SetAccountsInShardReq struct {
	Balances map[int]int
}
type SetAccountsInShardResp struct {
	Status  bool
	Message string
}

type PrintViewReq struct{}

// ViewMessage captures a view-change event observed by a node.
type ViewMessage struct {
	FromNode int
	Detail   string
}

type PrintViewResp struct {
	Status       bool
	Message      string
	ViewMessages []ViewMessage
}

type PrintBalanceReq struct {
	AccountID int
}

type PrintBalanceResp struct {
	Status  bool
	Message string
	Balance int
}

type PrintLogReq struct{}
type PrintLogSlotReq struct {
	Slot int
}

type PaxosBallot struct {
	SequenceNumber int
	NodeID         int
}

type PaxosLogEntry struct {
	Index       int
	IsNoOp      bool
	TxnVal      TransactionVal
	Status      SequenceStatus
	TwoPCStatus string
	Ballot      PaxosBallot
}

type PrintLogResp struct {
	Status  bool
	Message string
	Log     []PaxosLogEntry
}

type PrintLogSlotResp struct {
	Status  bool
	Message string
	Entry   PaxosLogEntry
}
