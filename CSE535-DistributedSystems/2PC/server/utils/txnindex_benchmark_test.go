// txnindex_benchmark_test.go
package utils

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// ---------- Test data & helpers ----------

const benchmarkNumKeys = 100_000

var benchKeys []string

// sink vars to prevent compiler optimizations
var (
	sinkInt  int
	sinkBool bool
)

func init() {
	benchKeys = make([]string, benchmarkNumKeys)
	for i := 0; i < benchmarkNumKeys; i++ {
		benchKeys[i] = "txn-" + strconv.Itoa(i)
	}
}

// LockedMap is a "normal" Go map protected by a single RWMutex.
// This is our baseline to compare against TxnIndex.
type LockedMap struct {
	mu sync.RWMutex
	m  map[string]int
}

func NewLockedMap() *LockedMap {
	return &LockedMap{
		m: make(map[string]int),
	}
}

func (lm *LockedMap) Add(k string, v int) {
	lm.mu.Lock()
	lm.m[k] = v
	lm.mu.Unlock()
}

func (lm *LockedMap) Get(k string) (int, bool) {
	lm.mu.RLock()
	v, ok := lm.m[k]
	lm.mu.RUnlock()
	return v, ok
}

func (lm *LockedMap) Delete(k string) {
	lm.mu.Lock()
	delete(lm.m, k)
	lm.mu.Unlock()
}

// ---------- Sequential benchmarks ----------

// BenchmarkTxnIndexSequential measures Add+Get+Delete in a single goroutine
// using your sharded TxnIndex implementation.
func BenchmarkTxnIndexSequential(b *testing.B) {
	ti := NewTxnIndex()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := benchKeys[i%benchmarkNumKeys]
		ti.Add(k, i)
		v, ok := ti.Get(k)
		// store in global sinks to prevent compiler optimizations
		sinkInt = v
		sinkBool = ok
		ti.Delete(k)
	}
}

// BenchmarkLockedMapSequential measures Add+Get+Delete in a single goroutine
// using a single locked map.
func BenchmarkLockedMapSequential(b *testing.B) {
	lm := NewLockedMap()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := benchKeys[i%benchmarkNumKeys]
		lm.Add(k, i)
		v, ok := lm.Get(k)
		sinkInt = v
		sinkBool = ok
		lm.Delete(k)
	}
}

// ---------- Parallel benchmarks ----------

// BenchmarkTxnIndexParallel stresses TxnIndex with concurrent Add+Get+Delete
// using testing.B.RunParallel.
func BenchmarkTxnIndexParallel(b *testing.B) {
	ti := NewTxnIndex()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// each goroutine gets its own RNG to avoid contention
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			i := r.Int()
			k := benchKeys[i%benchmarkNumKeys]

			ti.Add(k, i)
			v, ok := ti.Get(k)
			sinkInt = v
			sinkBool = ok
			ti.Delete(k)
		}
	})
}

// BenchmarkLockedMapParallel stresses the single-locked map with the same
// concurrent workload.
func BenchmarkLockedMapParallel(b *testing.B) {
	lm := NewLockedMap()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			i := r.Int()
			k := benchKeys[i%benchmarkNumKeys]

			lm.Add(k, i)
			v, ok := lm.Get(k)
			sinkInt = v
			sinkBool = ok
			lm.Delete(k)
		}
	})
}
