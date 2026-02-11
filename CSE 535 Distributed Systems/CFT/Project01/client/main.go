package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	common "pranavpateriya.com/multipaxos/common"
	"pranavpateriya.com/multipaxos/config"
)

func main() {
	var (
		actionFlag   = flag.String("action", "submit", "operation: submit|log|db|status|view|leaderfail|backupfail")
		targetLeader = flag.String("leader-node", "", "leader node for failure/resume actions")
		leaderFlag   = flag.String("leader", "", "initial leader (node id like n1 or addr host:port)")
		nodesFlag    = flag.String("nodes", "", "comma-separated fallback nodes or addresses")
		fromFlag     = flag.String("from", "A", "source account id")
		toFlag       = flag.String("to", "B", "destination account id")
		amountFlag   = flag.Int64("amount", 1, "transfer amount in cents")
		countFlag    = flag.Int("n", 1, "number of transactions to submit sequentially")
		timeoutFlag  = flag.Duration("timeout", 3*time.Second, "per-RPC timeout")
		intervalFlag = flag.Duration("interval", 250*time.Millisecond, "pause between submissions")
		sequenceFlag = flag.Int("sequence", 0, "sequence number for status queries")
		reasonFlag   = flag.String("reason", "client requested failure", "reason string for failure RPCs")
	)
	flag.Parse()

	action := strings.ToLower(strings.TrimSpace(*actionFlag))
	leaderTarget := strings.TrimSpace(*targetLeader)
	var targets []string
	if action == "leaderfail" || action == "stop-leader" || action == "leaderresume" || action == "start-leader" || action == "backupfail" || action == "stop-backup" || action == "backupresume" || action == "start-backup" {
		if leaderTarget != "" {
			targets = buildTargets(leaderTarget, *nodesFlag)
		} else {
			targets = buildTargets(*leaderFlag, *nodesFlag)
		}
	} else {
		targets = buildTargets(*leaderFlag, *nodesFlag)
	}

	switch action {
	case "submit":
		if *amountFlag <= 0 {
			log.Fatalf("amount must be > 0 (got %d)", *amountFlag)
		}
		if *countFlag <= 0 {
			log.Fatalf("n must be >= 1 (got %d)", *countFlag)
		}
		if len(targets) == 0 {
			log.Fatal("no server targets resolved; provide -leader, -nodes, or configure config.ServerMap")
		}
		log.Printf("using targets: %s", strings.Join(targets, ", "))

		template := common.TransactionReq{
			Transaction: common.Transaction{
				From:   strings.TrimSpace(*fromFlag),
				To:     strings.TrimSpace(*toFlag),
				Amount: *amountFlag,
			},
			TransactionID: common.TransactionID{},
		}

		for i := 1; i <= *countFlag; i++ {
			addr, reply, err := submitWithFailover(template, targets, *timeoutFlag, "")
			if err != nil {
				log.Printf("tx %d error via %s: %v", i, addr, err)
				if reply != nil && reply.Message != "" {
					log.Printf("tx %d server message: %s", i, reply.Message)
				}
			} else if reply != nil {
				log.Printf("tx %d ok via %s: %s", i, addr, reply.Message)
			}

			if reply != nil && reply.LeaderHint != "" {
				if hinted := normalizeTarget(reply.LeaderHint); hinted != "" {
					targets = reorderTargets(hinted, targets)
				}
			}

			if i != *countFlag && *intervalFlag > 0 {
				time.Sleep(*intervalFlag)
			}
		}

	case "submit-again":
		if *amountFlag <= 0 {
			log.Fatalf("amount must be > 0 (got %d)", *amountFlag)
		}
		if *countFlag <= 0 {
			log.Fatalf("n must be >= 1 (got %d)", *countFlag)
		}
		if len(targets) == 0 {
			log.Fatal("no server targets resolved; provide -leader, -nodes, or configure config.ServerMap")
		}
		log.Printf("using targets: %s", strings.Join(targets, ", "))

		template := common.TransactionReq{
			Transaction: common.Transaction{
				From:   strings.TrimSpace(*fromFlag),
				To:     strings.TrimSpace(*toFlag),
				Amount: *amountFlag,
			},
			TransactionID: common.TransactionID{},
		}
		now := time.Now()
		tID := fmt.Sprintf("%s-%d", clientID, now.UnixNano())

		for i := 1; i <= *countFlag; i++ {
			addr, reply, err := submitWithFailover(template, targets, *timeoutFlag, tID)
			if err != nil {
				log.Printf("tx %d error via %s: %v", i, addr, err)
				if reply != nil && reply.Message != "" {
					log.Printf("tx %d server message: %s", i, reply.Message)
				}
			} else if reply != nil {
				log.Printf("tx %d ok via %s: %s", i, addr, reply.Message)
			}

			if reply != nil && reply.LeaderHint != "" {
				if hinted := normalizeTarget(reply.LeaderHint); hinted != "" {
					targets = reorderTargets(hinted, targets)
				}
			}

			if i != *countFlag && *intervalFlag > 0 {
				time.Sleep(*intervalFlag)
			}
		}

	case "log":
		if len(targets) == 0 {
			log.Fatal("no server targets resolved for log query")
		}
		addr := targets[0]
		ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
		defer cancel()
		reply, err := FetchLog(ctx, addr)
		if err != nil {
			log.Fatalf("print log failed for %s: %v", addr, err)
		}
		fmt.Printf("Log entries for %s (%s):\n", addressLabel(addr), addr)
		for _, entry := range reply.Entries {
			status := string(entry.Status)
			if status == "" {
				status = string(common.StatusNoStatus)
			}
			fmt.Printf("  seq=%03d status=%s ballot=(%d,%d) noop=%t txn={%+v}\n",
				entry.Sequence, status, entry.BallotN, entry.BallotNode, entry.IsNoOp,
				entry.TransactionReq)
		}

	case "db":
		if len(targets) == 0 {
			log.Fatal("no server targets resolved for db query")
		}
		addr := targets[0]
		ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
		defer cancel()
		reply, err := FetchDB(ctx, addr)
		if err != nil {
			log.Fatalf("print db failed for %s: %v", addr, err)
		}
		keys := make([]string, 0, len(reply.Balances))
		for k := range reply.Balances {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Printf("Balances at %s (%s):\n", addressLabel(addr), addr)
		for _, k := range keys {
			fmt.Printf("  %s: %d\n", k, reply.Balances[k])
		}

	case "status":
		if *sequenceFlag < 0 {
			log.Fatalf("sequence must be >= 0 (got %d)", *sequenceFlag)
		}
		if len(targets) == 0 {
			targets = buildTargets("", "")
		}
		if len(targets) == 0 {
			log.Fatal("no server targets resolved for status query")
		}
		fmt.Printf("Status for sequence %d:\n", *sequenceFlag)
		for _, addr := range targets {
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			reply, err := FetchStatus(ctx, addr, *sequenceFlag)
			cancel()
			if err != nil {
				fmt.Printf("  %s (%s): error: %v\n", addressLabel(addr), addr, err)
				continue
			}
			fmt.Printf("  %s (%s): status=%s commit=%d executed=%d\n",
				addressLabel(addr), addr, reply.Status, reply.ExecutedIndex)
		}

	case "view":
		if len(targets) == 0 {
			log.Fatal("no server targets resolved for view query")
		}
		addr := targets[0]
		ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
		defer cancel()
		reply, err := FetchViews(ctx, addr)
		if err != nil {
			log.Fatalf("print view failed for %s: %v", addr, err)
		}
		fmt.Printf("New-view history at %s (%s):\n", addressLabel(addr), addr)
		if len(reply.Views) == 0 {
			fmt.Println("  (no new-view messages recorded)")
			return
		}
		for idx, view := range reply.Views {
			fmt.Printf("  [%d] from_leader=%d ballot=(%d,%d) at=%s entries=%d\n",
				idx+1, view.FromLeader, view.BallotN, view.BallotNode,
				view.ReceivedAt.Format(time.RFC3339), len(view.Entries))
			for _, entry := range view.Entries {
				fmt.Printf("      seq=%d status=%s noop=%t txn={%s->%s %d}\n",
					entry.Sequence, entry.Status, entry.IsNoOp,
					entry.TransactionReq.Transaction.From, entry.TransactionReq.Transaction.To, entry.TransactionReq.Transaction.Amount)
			}
		}

	case "leaderfail", "stop-leader":
		if len(targets) == 0 {
			log.Fatal("no server targets resolved; provide -leader or -nodes for leader failure")
		}
		reason := strings.TrimSpace(*reasonFlag)
		success := false
		for _, addr := range targets {
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			reply, err := SendLeaderFailure(ctx, addr, reason)
			cancel()
			if err != nil {
				log.Printf("leader failure via %s failed: %v", addr, err)
				continue
			}
			log.Printf("leader failure sent to %s: stopped=%v msg=%q", addr, reply.Stopped, reply.Message)
			success = true
			break
		}
		if !success {
			log.Fatal("leader failure RPC failed on all targets")
		}

	case "leaderresume", "start-leader":
		if len(targets) == 0 {
			log.Fatal("no targets resolved; provide -leader or -nodes for leader resume")
		}
		reason := strings.TrimSpace(*reasonFlag)
		success := false
		for _, addr := range targets {
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			reply, err := SendLeaderResume(ctx, addr, reason)
			cancel()
			if err != nil {
				log.Printf("leader resume via %s failed: %v", addr, err)
				continue
			}
			log.Printf("leader resume sent to %s: resumed=%v msg=%q", addr, reply.Resumed, reply.Message)
			success = true
			break
		}
		if !success {
			log.Fatal("leader resume RPC failed on all targets")
		}

	case "backupfail", "stop-backup":
		if len(targets) == 0 {
			log.Fatal("no server targets resolved; provide -leader or -nodes for backup failure")
		}
		reason := strings.TrimSpace(*reasonFlag)
		success := false
		for _, addr := range targets {
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			reply, err := SendBackupFailure(ctx, addr, reason)
			cancel()
			if err != nil {
				log.Printf("backup failure via %s failed: %v", addr, err)
				continue
			}
			log.Printf("backup failure sent to %s: stopped=%v msg=%q", addr, reply.Stopped, reply.Message)
			success = true
			break
		}
		if !success {
			log.Fatal("backup failure RPC failed on all targets")
		}

	case "backupresume", "start-backup":
		if len(targets) == 0 {
			log.Fatal("no targets resolved; provide -leader or -nodes for backup resume")
		}
		reason := strings.TrimSpace(*reasonFlag)
		success := false
		for _, addr := range targets {
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			reply, err := SendBackupResume(ctx, addr, reason)
			cancel()
			if err != nil {
				log.Printf("backup resume via %s failed: %v", addr, err)
				continue
			}
			log.Printf("backup resume sent to %s: resumed=%v msg=%q", addr, reply.Resumed, reply.Message)
			success = true
			break
		}
		if !success {
			log.Fatal("backup resume RPC failed on all targets")
		}

	default:
		log.Fatalf("unknown action %q", action)
	}
}

func submitWithFailover(tx common.TransactionReq, targets []string, timeout time.Duration, tID string) (string, *common.SubmitTxReply, error) {
	if len(targets) == 0 {
		return "", nil, fmt.Errorf("no targets supplied")
	}

	tried := make(map[string]struct{})
	current := targets[0]
	var lastErr error
	var reply *common.SubmitTxReply
	var resp *common.SubmitTxReply
	var err error

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if tID == "" {
			slog.Info("sending same")
			resp, err = SubmitTransaction(ctx, current, tx.Transaction)
		} else {
			slog.Info("sending repeat")
			resp, err = SubmitTransactionRepeat(ctx, current, tID, tx)
		}

		cancel()
		reply = resp

		if err == nil && resp != nil && resp.Success {
			return current, resp, nil
		}

		tried[current] = struct{}{}
		if err != nil {
			lastErr = err
		} else if resp != nil && resp.Message != "" {
			lastErr = fmt.Errorf(resp.Message)
		} else {
			lastErr = fmt.Errorf("submission failed")
		}

		if resp != nil && resp.LeaderHint != "" {
			if hinted := normalizeTarget(resp.LeaderHint); hinted != "" && hinted != current {
				if _, seen := tried[hinted]; !seen {
					current = hinted
					continue
				}
			}
		}

		next, ok := nextTarget(targets, tried)
		if !ok {
			return current, reply, lastErr
		}
		current = next
	}
}

func buildTargets(leader, nodes string) []string {
	seen := make(map[string]struct{})
	var targets []string

	if addr := normalizeTarget(leader); addr != "" {
		seen[addr] = struct{}{}
		targets = append(targets, addr)
	}

	if nodes != "" {
		for _, token := range strings.Split(nodes, ",") {
			if addr := normalizeTarget(token); addr != "" {
				if _, exists := seen[addr]; !exists {
					seen[addr] = struct{}{}
					targets = append(targets, addr)
				}
			}
		}
	}

	if len(targets) == 0 {
		keys := make([]string, 0, len(config.ServerMap))
		for k := range config.ServerMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			addr := config.ServerMap[key]
			if _, exists := seen[addr]; exists {
				continue
			}
			seen[addr] = struct{}{}
			targets = append(targets, addr)
		}
	}

	return targets
}

func normalizeTarget(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	if strings.Contains(input, ":") {
		return input
	}
	key := strings.ToLower(input)
	if !strings.HasPrefix(key, "n") {
		if _, err := strconv.Atoi(key); err == nil {
			key = "n" + key
		}
	}
	if addr, ok := config.ServerMap[key]; ok {
		return addr
	}
	return ""
}

func reorderTargets(leader string, current []string) []string {
	if leader == "" {
		return current
	}
	seen := map[string]struct{}{leader: {}}
	reordered := []string{leader}
	for _, addr := range current {
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		reordered = append(reordered, addr)
	}
	return reordered
}

func nextTarget(targets []string, tried map[string]struct{}) (string, bool) {
	for _, addr := range targets {
		if _, ok := tried[addr]; !ok {
			return addr, true
		}
	}
	return "", false
}

func addressLabel(addr string) string {
	for id, mapped := range config.ServerMap {
		if mapped == addr {
			return id
		}
	}
	return addr
}
