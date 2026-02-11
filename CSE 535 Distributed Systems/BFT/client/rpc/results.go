//filename: results.go
package client

import (
	"sync"

	"pranavpateriya.com/bft/common"
)

type ExecResult struct {
	mu      sync.Mutex
	streams map[string]chan common.ReplySigned
}

func NewExecResult() *ExecResult {
	return &ExecResult{
		streams: make(map[string]chan common.ReplySigned),
	}
}

func (e *ExecResult) Register(txID string) <-chan common.ReplySigned {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.streams == nil {
		e.streams = make(map[string]chan common.ReplySigned)
	}
	//NOTE: buffered channel, so that we don't block
	ch := make(chan common.ReplySigned, 8)
	e.streams[txID] = ch
	return ch
}

func (e *ExecResult) Unregister(txID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ch, ok := e.streams[txID]; ok {
		delete(e.streams, txID)
		close(ch)
	}
}

//NOTE: if buffer chanel is full the reply is dropped
func (e *ExecResult) ExecutionResult(req *common.ReplySigned, resp *common.ReplyReply) error {
    if req == nil { return nil }
    txID := req.ReplyPayload.TransactionID.UniqueID

    e.mu.Lock()
    ch, ok := e.streams[txID]
    if ok {
        select {
        case ch <- *req: // non-blocking send under lock prevents close/send race
        default:
        }
    }
    e.mu.Unlock()
    return nil
}