// filename monimoni.go
package statemachine

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
)

// ====== Shared Types and Interface ======

type AccountID string
type Dollars int64

var (
	ErrUnknownAccount = errors.New("unknown account")
	ErrInsufficient   = errors.New("insufficient funds")
	ErrNegativeAmount = errors.New("amount must be > 0")
)

type BankStore interface {
	Transfer(from, to AccountID, amount Dollars) (Dollars, Dollars, error)
	Snapshot(ctx context.Context) map[AccountID]Dollars
}

// ====== CSV Store Implementation ======

type csvStore struct {
	mu     sync.RWMutex
	file   string
	data   map[AccountID]Dollars
	header []string
	order  []AccountID
}

func NewCSVStore(filePath string) (*csvStore, error) {
	s := &csvStore{
		file:   filePath,
		data:   make(map[AccountID]Dollars),
		header: []string{"Account", "Balance"},
		order:  defaultAccountOrder(),
	}

	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		// create empty file with header
		f, err := os.Create(filePath)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		w := csv.NewWriter(f)
		_ = w.Write(s.header)
		w.Flush()
	} else {
		if err := s.loadFromFile(); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// NewBankStore loads the default balances CSV bundled with the statemachine package
// and returns a BankStore backed by it.
func NewBankStore(nodeId int) (BankStore, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("unable to resolve statemachine package path")
	}
	base := filepath.Dir(file)
	store, err := NewCSVStore(filepath.Join(base, "balances"+fmt.Sprintf("%d", nodeId)+".csv"))
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (s *csvStore) loadFromFile() error {
	f, err := os.Open(s.file)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return err
	}

	s.data = make(map[AccountID]Dollars)
	order := make([]AccountID, 0, len(rows))
	for i, row := range rows {
		if i == 0 {
			continue // skip header
		}
		if len(row) != 2 {
			continue
		}
		amt, _ := strconv.ParseInt(row[1], 10, 64)
		acct := AccountID(row[0])
		s.data[acct] = Dollars(amt)
		order = append(order, acct)
	}
	if len(order) > 0 {
		s.order = order
	} else {
		s.order = defaultAccountOrder()
	}
	return nil
}

func (s *csvStore) flushToFile() error {
	f, err := os.Create(s.file)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write(s.header)

	for _, acct := range s.order {
		bal, ok := s.data[acct]
		if !ok {
			continue
		}
		_ = w.Write([]string{string(acct), fmt.Sprintf("%d", bal)})
	}
	w.Flush()
	return w.Error()
}

func (s *csvStore) Transfer(from, to AccountID, amount Dollars) (Dollars, Dollars, error) {
	if amount <= 0 {
		return 0, 0, ErrNegativeAmount
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fromBal, okFrom := s.data[from]
	toBal, okTo := s.data[to]
	if !okFrom || !okTo {
		return 0, 0, ErrUnknownAccount
	}
	if fromBal < amount {
		return 0, 0, ErrInsufficient
	}

	fromBal -= amount
	toBal += amount
	s.data[from] = fromBal
	s.data[to] = toBal
	if !containsAccount(s.order, from) {
		s.order = append(s.order, from)
	}
	if !containsAccount(s.order, to) {
		s.order = append(s.order, to)
	}

	if err := s.flushToFile(); err != nil {
		return 0, 0, err
	}
	return fromBal, toBal, nil
}

func (s *csvStore) Snapshot(ctx context.Context) map[AccountID]Dollars {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[AccountID]Dollars, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	return cp
}

func defaultAccountOrder() []AccountID {
	return []AccountID{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
}

func containsAccount(list []AccountID, acct AccountID) bool {
	for _, existing := range list {
		if existing == acct {
			return true
		}
	}
	return false
}
