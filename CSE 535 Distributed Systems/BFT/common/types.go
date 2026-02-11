package common

type RequestType string

const (
	// RequestTransfer represents a state-changing money transfer.
	RequestTransfer RequestType = "transfer"
	// RequestBalance represents a read-only balance query.
	RequestBalance RequestType = "balance"
	// RequestNoOp represents a no-op placeholder used during view recovery.
	RequestNoOp RequestType = "noop"
)

// TODO: Where
// <REQUEST, o, t, c>sigma-p
type ClientReqSigned struct {
	ClientRequestPayload   ClientReq
	ClientRequestSignature []byte
}

// <REQUEST, o, t, c>
type ClientReq struct {
	TransactionVal TransactionVal
	TransactionID  TransactionID
}

// o
type TransactionVal struct {
	TransactionType     RequestType
	TransactionTransfer *TransferTransaction
	TransactionRead     *ReadTransaction
}

// t,c
type TransactionID struct {
	UniqueID string // t
	ClientID string // c
}

type TransferTransaction struct {
	Sender   string
	Receiver string
	Amount   int
}

type ReadTransaction struct {
	AcctNum string
}

type SubmitTxnAck struct {
	Ack     bool
	Message string
}

// ======================== Execute RPC ===========================

// <REPLY v, t, c, i, r>sigma-i
type ReplySigned struct {
	ReplyPayload   Reply
	ReplySignature []byte
}

// <REPLY v, t, c, i, r>
type Reply struct {
	Ack           bool
	ViewNumber    int           // v
	TransactionID TransactionID // t&c
	NodeId        int           // i
	Result        NodeResponse  //r
}

type NodeResponse struct {
	TransactionType RequestType
	Status          bool
	Message         string
	Balance         int
}

type ReplyReply struct{}
