package utils

import "sync"

// Tune these if needed; keep numShards a power of 2 for fast masking.
const shardBits = 6                  // 2^6 = 64 shards
const numShards = 1 << shardBits     // 64

type txnShard struct {
    mu sync.RWMutex
    m  map[string]int
}

type TxnIndex struct {
    shards [numShards]txnShard
}

// NewTxnIndex initializes all shard maps.
func NewTxnIndex() *TxnIndex {
    ti := &TxnIndex{}
    for i := 0; i < numShards; i++ {
        ti.shards[i].m = make(map[string]int)
    }
    return ti
}

// Lightweight FNV-1a style hash (no allocations).
// Good enough for sharding + fast.
func hashString(s string) uint32 {
    const (
        offset32 = 2166136261
        prime32  = 16777619
    )
    var h uint32 = offset32
    for i := 0; i < len(s); i++ {
        h ^= uint32(s[i])
        h *= prime32
    }
    return h
}

func (ti *TxnIndex) shardFor(txnID string) *txnShard {
    h := hashString(txnID)
    idx := h & (numShards - 1) // faster than %
    return &ti.shards[idx]
}

//
// 1) Add a transactionID - sequence number pair
//
func (ti *TxnIndex) Add(txnID string, seq int) {
    sh := ti.shardFor(txnID)
    sh.mu.Lock()
    sh.m[txnID] = seq
    sh.mu.Unlock()
}

//
// 2) Delete a transactionID - sequence number pair
//
func (ti *TxnIndex) Delete(txnID string) {
    sh := ti.shardFor(txnID)
    sh.mu.Lock()
    delete(sh.m, txnID)
    sh.mu.Unlock()
}

//
// 3) Get sequence number corresponding to a transactionID
//
func (ti *TxnIndex) Get(txnID string) (int, bool) {
    sh := ti.shardFor(txnID)
    sh.mu.RLock()
    seq, ok := sh.m[txnID]
    sh.mu.RUnlock()
    return seq, ok
}
