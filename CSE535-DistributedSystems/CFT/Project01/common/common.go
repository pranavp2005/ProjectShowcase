//filename common.go
package common

import (
	"time"
)

type ExecutionResultMessage string
const (
	InsufficientBalance ExecutionResultMessage = "UNSUCCESSFUL: INSUFFICIENT BALANCE"
	InternalError ExecutionResultMessage = "UNSUCCESSFUL: SOMETHING WRONG WITH EXECUTOR"
	UnknownAccount ExecutionResultMessage = "UNSUCCESSFUL: ACCOUNT DOES NOT EXIST"
	NegativeTxnAmt ExecutionResultMessage = "UNSUCCESSFUL: NEGATIVE AMOUNT SENT"
	
	NoQuorumError ExecutionResultMessage = "UNSUCCESSFUL: MAJORITY IS DOWN"
	
	TransferSuccess ExecutionResultMessage = "SUCCESSFUL: TRANSFER DONE"
	NoOpSuccess ExecutionResultMessage = "SUCCESSFUL: NOOP EXECUTED"

	SelfAcceptFailed ExecutionResultMessage = "UNSUCCESSFUL: SELF ACCEPT FAILED"

	GapInLogs ExecutionResultMessage = "UNSUCCESSFUL: THERE IS A GAP IN LOGS"
	TransactionSuccessful ExecutionResultMessage = "SUCCESSFUL: TRANSACTION SUCCESSFUL DONE"

	BadIndex ExecutionResultMessage = "UNSUCCESSFUL: EXECUTION START INDEX CORRUPTED"
	ProposalError ExecutionResultMessage = "UNSUCCESSFUL: FAILED TO GET ACCEPTANCE AND COMMIT AND EXECUTE"

)

type TransactionReq struct {
	Transaction Transaction
	TransactionID TransactionID
}

type Transaction struct {
	From      string
	To        string
	Amount    int64
}

type TransactionID struct {
	Timestamp time.Time
	ClientID string
	TransactionId string
}

type TransactionClientReply struct {
	Executed bool
	Message ExecutionResultMessage
}

func Majority(n int) int {
	return n/2 + 1
}

