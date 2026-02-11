package bank

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

	"slices"
)

// ====== Shared Types and Interface ======

const defaultInitialBalance = 10

var (
	ErrUnknownAccount = errors.New("unknown account")
	ErrInsufficient   = errors.New("insufficient funds")
	ErrNegativeAmount = errors.New("amount must be > 0")
)

type BankStore interface {
	Transfer(from, to string, amount int) (int, int, error)
	Balance(acct string) (int, error)
	Snapshot() map[string]int
}

// ====== CSV Store Implementation ======

type csvStore struct {
	mu     sync.RWMutex
	file   string
	data   map[string]int
	header []string
	order  []string
}

func NewCSVStore(filePath string) (*csvStore, error) {
	s := &csvStore{
		file:   filePath,
		data:   make(map[string]int),
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

	slog.Info("bank store initialized", "file", filePath)
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
	slog.Info("bank store ready", "nodeID", nodeId, "file", store.file)
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

	s.data = make(map[string]int)
	order := make([]string, 0, len(rows))
	for i, row := range rows {
		if i == 0 {
			continue // skip header
		}
		if len(row) != 2 {
			continue
		}
		amt, _ := strconv.ParseInt(row[1], 10, 64)
		acct := string(row[0])
		s.data[acct] = int(amt)
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

func (s *csvStore) Transfer(from, to string, amount int) (int, int, error) {
	if amount <= 0 {
		return 0, 0, ErrNegativeAmount
	}

	slog.Debug("bank transfer requested", "store", s.file, "from", from, "to", to, "amount", amount)
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
	slog.Info("bank transfer applied", "store", s.file, "from", from, "to", to, "amount", amount, "fromBalance", fromBal, "toBalance", toBal)
	return fromBal, toBal, nil
}

func (s *csvStore) Snapshot() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]int, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	return cp
}

func (s *csvStore) Balance(acct string) (int, error) {
	s.mu.RLock()
	bal, ok := s.data[acct]
	s.mu.RUnlock()
	if !ok {
		return 0, ErrUnknownAccount
	}
	slog.Debug("bank balance read", "store", s.file, "account", acct, "balance", bal)
	return bal, nil
}

func defaultAccountOrder() []string {
	return []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
}

func containsAccount(list []string, acct string) bool {
	return slices.Contains(list, acct)
}

// Reset overwrites every account with the default initial balance and flushes the CSV file.
func (s *csvStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]int, len(defaultAccountOrder()))
	s.order = defaultAccountOrder()
	for _, acct := range s.order {
		s.data[acct] = defaultInitialBalance
	}
	if err := s.flushToFile(); err != nil {
		return err
	}
	slog.Warn("bank store reset", "store", s.file)
	return nil
}
