package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// =============== TYPES ===============
type BankStore interface {
	GetBalance(acctID int) (int, error)
	SetBalance(acctID int, balance int) error
	AddToBalance(acctID int, delta int) error
	BalanceGreaterThan(acctID, amt int) bool
	TransferBalance(from, to, amt int) error
	DeleteAccount(acctID int) error
	Snapshot() map[int]int
}

type WALEntry struct {
	TxnID          string
	FromPresent    bool
	FromOldBalance int
	ToPresent      bool
	ToOldBalance   int
}

type WalService interface {
	LogPrepare(txnID string, fromPresent bool, fromOld int, toPresent bool, toOld int) error
	Read(txnID string) (WalRecord, error)
	Clear(txnID string) error
}

type WalRecord struct {
	FromPresent    bool
	FromOldBalance int
	ToPresent      bool
	ToOldBalance   int
}
type BoltStore struct {
	db *bolt.DB
}

var (
	balanceBucket = []byte("balances")
	walBucket     = []byte("wal")
)

// =============== FUNCTIONS ===============

// NewBoltStore opens (or creates) a BoltDB-backed store and ensures all buckets exist.
func NewBoltStore(dbPath string, perm os.FileMode) (*BoltStore, error) {
	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	store := &BoltStore{db: db}
	if err := store.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(balanceBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(walBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (b *BoltStore) Close() error {
	if b == nil || b.db == nil {
		return nil
	}
	return b.db.Close()
}

func (b *BoltStore) GetBalance(acctID int) (int, error) {
	var balance int
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}
		raw := bucket.Get(intKey(acctID))
		if raw == nil {
			balance = 0
			return nil
		}
		balance = int(binary.BigEndian.Uint64(raw))
		return nil
	})
	return balance, err
}

func (b *BoltStore) SetBalance(acctID int, balance int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(balance))
		return bucket.Put(intKey(acctID), buf)
	})
}

// ReplaceAllBalances overwrites the entire balance bucket in a single transaction.
func (b *BoltStore) ReplaceAllBalances(balances map[int]int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}
		// Clear existing balances.
		if err := bucket.ForEach(func(k, _ []byte) error { return bucket.Delete(k) }); err != nil {
			return err
		}
		// Write new balances.
		buf := make([]byte, 8)
		for acct, bal := range balances {
			binary.BigEndian.PutUint64(buf, uint64(bal))
			if err := bucket.Put(intKey(acct), buf); err != nil {
				return err
			}
		}
		return nil
	})
}

// SeedIfEmpty populates [start,end] with the given balance in a single transaction when empty.
func (b *BoltStore) SeedIfEmpty(start, end, balance int) (bool, error) {
	seeded := false
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}
		if bucket.Stats().KeyN > 0 {
			return nil
		}

		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(balance))
		for id := start; id <= end; id++ {
			if err := bucket.Put(intKey(id), val); err != nil {
				return err
			}
		}
		seeded = true
		return nil
	})
	return seeded, err
}

func (b *BoltStore) AddToBalance(acctID int, delta int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}
		key := intKey(acctID)
		current := bucket.Get(key)
		val := int64(0)
		if current != nil {
			val = int64(binary.BigEndian.Uint64(current))
		}
		val += int64(delta)

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		return bucket.Put(key, buf)
	})
}

func (b *BoltStore) BalanceGreaterThan(acctID, amt int) bool {
	bal, err := b.GetBalance(acctID)
	if err != nil {
		return false
	}
	return bal > amt
}

func (b *BoltStore) TransferBalance(from, to, amt int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}

		fromKey := intKey(from)
		toKey := intKey(to)

		fromBal := 0
		if raw := bucket.Get(fromKey); raw != nil {
			fromBal = int(binary.BigEndian.Uint64(raw))
		}
		if fromBal < amt {
			return fmt.Errorf("insufficient funds: have %d need %d", fromBal, amt)
		}

		toBal := 0
		if raw := bucket.Get(toKey); raw != nil {
			toBal = int(binary.BigEndian.Uint64(raw))
		}

		fromBuf := make([]byte, 8)
		toBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(fromBuf, uint64(fromBal-amt))
		binary.BigEndian.PutUint64(toBuf, uint64(toBal+amt))

		if err := bucket.Put(fromKey, fromBuf); err != nil {
			return err
		}
		return bucket.Put(toKey, toBuf)
	})
}

func (b *BoltStore) DeleteAccount(acctID int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", balanceBucket)
		}
		return bucket.Delete(intKey(acctID))
	})
}

func (b *BoltStore) Snapshot() map[int]int {
	snapshot := make(map[int]int)
	_ = b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(balanceBucket)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			snapshot[int(binary.BigEndian.Uint64(k))] = int(binary.BigEndian.Uint64(v))
			return nil
		})
	})
	return snapshot
}

func (b *BoltStore) PutWAL(entry WALEntry) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(walBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", walBucket)
		}
		return bucket.Put([]byte(entry.TxnID), payload)
	})
}

func (b *BoltStore) Get(txnID string) (WALEntry, bool, error) {
	var entry WALEntry
	found := false
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(walBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", walBucket)
		}
		data := bucket.Get([]byte(txnID))
		if data == nil {
			return nil
		}
		found = true
		return json.Unmarshal(data, &entry)
	})
	return entry, found, err
}

func (b *BoltStore) Delete(txnID string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(walBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", walBucket)
		}
		return bucket.Delete([]byte(txnID))
	})
}

// ===== WalService implementation =====
func (b *BoltStore) LogPrepare(txnID string, fromPresent bool, fromOld int, toPresent bool, toOld int) error {
	entry := WALEntry{
		TxnID:          txnID,
		FromPresent:    fromPresent,
		FromOldBalance: fromOld,
		ToPresent:      toPresent,
		ToOldBalance:   toOld,
	}
	return b.PutWAL(entry)
}

func (b *BoltStore) Read(txnID string) (WalRecord, error) {
	entry, found, err := b.Get(txnID)
	if err != nil {
		return WalRecord{}, err
	}
	if !found {
		return WalRecord{}, fmt.Errorf("wal entry not found")
	}
	return WalRecord{
		FromPresent:    entry.FromPresent,
		FromOldBalance: entry.FromOldBalance,
		ToPresent:      entry.ToPresent,
		ToOldBalance:   entry.ToOldBalance,
	}, nil
}

func (b *BoltStore) Clear(txnID string) error {
	return b.Delete(txnID)
}

func intKey(v int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

//THE FOLLOWING FUNCTIONS ARE LEGACY CODE
// --- Basic Operations ---
