package main

import (
	"log/slog"
	"sync/atomic"
)

type idSet struct {
	maxID int
	words []uint64 // Each word is 64 bits => manages 64 IDs
}

const maxAcctID = 9000

var acctLocks = newIDSet(maxAcctID)

func newIDSet(maxID int) *idSet {
	wordCount := (maxID + 63) / 64
	return &idSet{
		maxID: maxID,
		words: make([]uint64, wordCount),
	}
}

// (1) Try to insert ID if not already present
func tryAddIfAbsent(id int) bool {
	if id <= 0 || id > acctLocks.maxID {
		return false
	}
	i := id - 1
	wordIdx := i >> 6
	bitPos := uint(i & 63)
	mask := uint64(1) << bitPos
	ptr := &acctLocks.words[wordIdx]

	for {
		old := atomic.LoadUint64(ptr)
		if old&mask != 0 {
			slog.Debug("lock exists", "account", id)
			return false // already present (locked)
		}
		newVal := old | mask
		if atomic.CompareAndSwapUint64(ptr, old, newVal) {
			slog.Debug("lock acquired", "account", id)
			return true // successfully inserted
		}
	}
}

// (2) Remove ID if present
func removeID(id int) bool {
	if id <= 0 || id > acctLocks.maxID {
		return false
	}
	i := id - 1
	wordIdx := i >> 6
	bitPos := uint(i & 63)
	mask := uint64(1) << bitPos
	ptr := &acctLocks.words[wordIdx]

	for {
		old := atomic.LoadUint64(ptr)
		if old&mask == 0 {
			slog.Debug("lock remove noop", "account", id)
			return false // already absent
		}
		newVal := old &^ mask
		if atomic.CompareAndSwapUint64(ptr, old, newVal) {
			slog.Debug("lock released", "account", id)
			return true // removed successfully
		}
	}
}

// (3) Check if ID is present (locked)
func isLocked(id int) bool {
	if id <= 0 || id > acctLocks.maxID {
		return false
	}
	i := id - 1
	wordIdx := i >> 6
	bitPos := uint(i & 63)
	mask := uint64(1) << bitPos
	word := atomic.LoadUint64(&acctLocks.words[wordIdx])
	return word&mask != 0
}

// (4) Try to lock two IDs together in a consistent order to avoid deadlock
func tryAddPairIfBothAbsent(a, b int) bool {
	// Validate IDs
	if a <= 0 || a > acctLocks.maxID || b <= 0 || b > acctLocks.maxID {
		return false
	}
	// Order them to avoid deadlocks
	if a == b {
		return tryAddIfAbsent(a)
	}
	if a > b {
		a, b = b, a
	}

	i1, i2 := a-1, b-1
	wordIdx1 := i1 >> 6
	wordIdx2 := i2 >> 6
	bitPos1 := uint(i1 & 63)
	bitPos2 := uint(i2 & 63)

	mask1 := uint64(1) << bitPos1
	mask2 := uint64(1) << bitPos2

	// Same word: use one atomic operation
	if wordIdx1 == wordIdx2 {
		mask := mask1 | mask2
		ptr := &acctLocks.words[wordIdx1]
		for {
			old := atomic.LoadUint64(ptr)
			if old&mask != 0 {
				return false // either bit already set
			}
			newVal := old | mask
			if atomic.CompareAndSwapUint64(ptr, old, newVal) {
				return true // both bits set atomically
			}
		}
	}

	// Different words: try first, then second; if second fails, roll back first
	if !tryAddIfAbsent(a) {
		slog.Debug("lock pair failed first", "a", a, "b", b)
		return false
	}
	if !tryAddIfAbsent(b) {
		slog.Debug("lock pair rollback second failed", "a", a, "b", b)
		removeID(a)
		return false
	}
	slog.Debug("lock pair acquired", "a", a, "b", b)
	return true
}

// unlockBoth unlocks accountIDs a and b, whether they are present or not.
func unlockBoth(a, b int) {
	if a > 0 && a <= acctLocks.maxID {
		_ = removeID(a)
	}
	if b > 0 && b <= acctLocks.maxID && b != a {
		_ = removeID(b)
	}
	slog.Debug("locks released", "a", a, "b", b)
}
