// file name: parser.go
package main

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"pranavpateriya.com/bft/common"
)

type TestSet struct {
	Number         int
	Transactions   []TransactionSpec
	LiveNodes      []int
	ByzantineNodes []int
	Attacks        []AttackSpec
}

type TransactionSpec struct {
	Raw      string
	Kind     common.RequestType
	Client   string
	Sender   string
	Receiver string
	Amount   int
	Account  string
}

type AttackSpec struct {
	Kind    string
	Targets []int
	DelayMS int
}

// NOTE: Default time delay attack keep it small so that view change does not get trigerred
const defaultTimingDelayMS = 10

func parseTestCases(path string) ([]TestSet, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var (
		sets    []TestSet
		current *TestSet
	)

	for idx, row := range rows {
		if idx == 0 {
			continue // skip header
		}
		if len(row) < 5 {
			slog.Error("row has less than 5 elements", "row number", row)
			continue
		}

		setStr := strings.TrimSpace(row[0])
		if setStr != "" {
			num, err := strconv.Atoi(setStr)
			if err != nil {
				return nil, fmt.Errorf("row %d: invalid set number %q", idx+1, row[0])
			}

			live, err := parseNodeList(row[2])
			if err != nil {
				return nil, fmt.Errorf("set %d: %w", num, err)
			}
			if len(live) == 0 {
				live = defaultNodeList()
			}

			byz, err := parseNodeList(row[3])
			if err != nil {
				return nil, fmt.Errorf("set %d: %w", num, err)
			}

			attacks, err := parseAttackList(row[4])
			if err != nil {
				return nil, fmt.Errorf("set %d: %w", num, err)
			}

			if current != nil {
				sets = append(sets, *current)
			}

			current = &TestSet{
				Number:         num,
				LiveNodes:      live,
				ByzantineNodes: byz,
				Attacks:        attacks,
			}
		}

		if current == nil {
			continue
		}

		txStr := strings.TrimSpace(row[1])
		if txStr == "" {
			continue
		}

		tx, err := parseTransaction(txStr)
		if err != nil {
			return nil, fmt.Errorf("set %d: %w", current.Number, err)
		}
		current.Transactions = append(current.Transactions, tx)
	}

	if current != nil {
		sets = append(sets, *current)
	}
	return sets, nil
}

func parseTransaction(raw string) (TransactionSpec, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "\"")
	trimmed = strings.TrimSuffix(trimmed, "\"")
	trimmed = strings.TrimSpace(trimmed)
	trimmed = strings.TrimPrefix(trimmed, "(")
	trimmed = strings.TrimSuffix(trimmed, ")")

	parts := strings.Split(trimmed, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	switch len(parts) {
	case 1:
		acct := normalizeAccount(parts[0])
		if acct == "" {
			return TransactionSpec{}, fmt.Errorf("invalid balance transaction %q", raw)
		}
		return TransactionSpec{
			Raw:     raw,
			Kind:    common.RequestBalance,
			Client:  acct,
			Account: acct,
		}, nil
	case 3:
		from := normalizeAccount(parts[0])
		to := normalizeAccount(parts[1])
		if from == "" || to == "" {
			return TransactionSpec{}, fmt.Errorf("invalid transfer %q", raw)
		}
		amt, err := strconv.Atoi(parts[2])
		if err != nil || amt <= 0 {
			return TransactionSpec{}, fmt.Errorf("invalid amount in %q", raw)
		}
		return TransactionSpec{
			Raw:      raw,
			Kind:     common.RequestTransfer,
			Client:   from,
			Sender:   from,
			Receiver: to,
			Amount:   amt,
		}, nil
	default:
		return TransactionSpec{}, fmt.Errorf("malformed transaction %q", raw)
	}
}

func parseNodeList(raw string) ([]int, error) {
	token := strings.TrimSpace(raw)
	token = strings.TrimPrefix(token, "\"")
	token = strings.TrimSuffix(token, "\"")
	token = strings.TrimSpace(token)
	token = strings.Trim(token, "[]")
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}

	parts := strings.Split(token, ",")
	result := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "n")
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid node id %q", part)
		}
		result = append(result, id)
	}
	return result, nil
}

func parseAttackList(raw string) ([]AttackSpec, error) {
	token := strings.TrimSpace(raw)
	token = strings.TrimPrefix(token, "\"")
	token = strings.TrimSuffix(token, "\"")
	token = strings.Trim(token, "[]")
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}

	parts := strings.Split(token, ";")
	result := make([]AttackSpec, 0, len(parts))

	for _, part := range parts {
		original := strings.TrimSpace(part)
		if original == "" {
			continue
		}
		partLower := strings.ToLower(original)
		switch {
		case strings.HasPrefix(partLower, "time"):
			delay := defaultTimingDelayMS
			if idx := strings.Index(partLower, "("); idx >= 0 {
				if end := strings.Index(partLower, ")"); end > idx {
					val := strings.TrimSpace(partLower[idx+1 : end])
					if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
						delay = parsed
					}
				}
			}
			result = append(result, AttackSpec{Kind: "time", DelayMS: delay})
		case strings.HasPrefix(partLower, "dark"):
			targets, err := parseNodeTargets(partLower)
			if err != nil {
				return nil, err
			}
			result = append(result, AttackSpec{Kind: "dark", Targets: targets})
		case strings.HasPrefix(partLower, "equivocation"):
			targets, err := parseNodeTargets(partLower)
			if err != nil {
				return nil, err
			}
			result = append(result, AttackSpec{Kind: "equivocation", Targets: targets})
		case partLower == "sign":
			result = append(result, AttackSpec{Kind: "sign"})
		case partLower == "crash":
			result = append(result, AttackSpec{Kind: "crash"})
		default:
			return nil, fmt.Errorf("unknown attack %q", original)
		}
	}

	return result, nil
}

func parseNodeTargets(expr string) ([]int, error) {
	start := strings.Index(expr, "(")
	end := strings.Index(expr, ")")
	if start < 0 || end <= start+1 {
		return nil, fmt.Errorf("invalid target list %q", expr)
	}
	body := strings.TrimSpace(expr[start+1 : end])
	body = strings.ReplaceAll(body, " ", "")
	if body == "" {
		return nil, nil
	}
	parts := strings.Split(body, ",")
	targets := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimPrefix(part, "n")
		if part == "" {
			continue
		}
		id, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid node target in %q", expr)
		}
		targets = append(targets, id)
	}
	return targets, nil
}

func normalizeAccount(token string) string {
	t := strings.TrimSpace(token)
	if t == "" {
		return ""
	}
	t = strings.TrimPrefix(strings.ToUpper(t), "CLIENT")
	return strings.ToUpper(t)
}

func defaultNodeList() []int {
	nodes := make([]int, 0, 7)
	for i := 1; i <= 7; i++ {
		nodes = append(nodes, i)
	}
	return nodes
}

func (t TransactionSpec) Description() string {
	switch t.Kind {
	case common.RequestTransfer:
		return fmt.Sprintf("Transfer %s -> %s (%d)", t.Sender, t.Receiver, t.Amount)
	case common.RequestBalance:
		return fmt.Sprintf("Balance %s", t.Account)
	default:
		return t.Raw
	}
}

func (t TransactionSpec) ClientID() string {
	return strings.ToUpper(t.Client)
}
