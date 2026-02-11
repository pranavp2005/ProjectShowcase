// filename: controller.go
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clientrpc "pranavpateriya.com/bft/client/rpc"
	"pranavpateriya.com/bft/common"
	serverrpc "pranavpateriya.com/bft/server/rpc"
)

type ControllerConfig struct {
	NodeAddrs        map[int]string
	FaultTolerance   int
	DialTimeout      time.Duration
	BroadcastTimeout time.Duration
	ReadTimeout      time.Duration
}

const defaultCommitTimeout = 25 * time.Second

var ErrMaxConsensusAttempts = errors.New("consensus retry limit reached")

type Controller struct {
	cfg         ControllerConfig
	execSrv     *clientrpc.ExecResult
	nodeKeys    map[int]ed25519.PublicKey
	clientKeys  map[string]ed25519.PrivateKey
	clientKeyMu sync.Mutex
	nodeOrder   []int
	nodeAddrs   map[int]string
	activeNodes map[int]struct{}
	totalNodes  int

	// NOTE: split quorums
	readQuorum   int // 2f+1 (fast read)
	commitQuorum int // f+1 (normal op completion)

	currentView   int
	perClientSeq  map[string]uint64
	mu            sync.Mutex
	dialTimeout   time.Duration
	readTimeout   time.Duration
	commitTimeout time.Duration
}

func NewController(cfg ControllerConfig, exec *clientrpc.ExecResult) (*Controller, error) {
	if len(cfg.NodeAddrs) == 0 {
		return nil, errors.New("no replica addresses provided")
	}
	if cfg.FaultTolerance <= 0 {
		return nil, fmt.Errorf("invalid fault tolerance %d", cfg.FaultTolerance)
	}

	nodeOrder := make([]int, 0, len(cfg.NodeAddrs))
	for id := range cfg.NodeAddrs {
		nodeOrder = append(nodeOrder, id)
	}
	sort.Ints(nodeOrder)

	nodeKeys, err := loadNodePublicKeys()
	if err != nil {
		return nil, err
	}

	readQ := 2*cfg.FaultTolerance + 1
	commitQ := cfg.FaultTolerance + 1
	commitTimeout := cfg.BroadcastTimeout
	if commitTimeout <= 0 {
		commitTimeout = defaultCommitTimeout
	}

	return &Controller{
		cfg:           cfg,
		execSrv:       exec,
		nodeKeys:      nodeKeys,
		clientKeys:    make(map[string]ed25519.PrivateKey),
		nodeOrder:     nodeOrder,
		nodeAddrs:     cfg.NodeAddrs,
		activeNodes:   make(map[int]struct{}),
		totalNodes:    len(nodeOrder),
		readQuorum:    readQ,
		commitQuorum:  commitQ,
		currentView:   1,
		perClientSeq:  make(map[string]uint64),
		dialTimeout:   cfg.DialTimeout,
		readTimeout:   cfg.ReadTimeout,
		commitTimeout: commitTimeout,
	}, nil
}

func (c *Controller) PrepareSet(set TestSet) error {
	slog.Info("preparing set", "set", set.Number, "transactions", len(set.Transactions))

	reason := fmt.Sprintf("reset-set-%d", set.Number)
	if err := c.resetNodes(reason); err != nil {
		return err
	}
	slog.Info("nodes reset")

	c.mu.Lock()
	c.currentView = 1
	c.mu.Unlock()

	if err := c.applyLiveNodes(set.LiveNodes, reason); err != nil {
		return err
	}
	if err := c.applyByzantineConfig(set.ByzantineNodes, set.Attacks); err != nil {
		return err
	}
	return nil
}

func (c *Controller) ExecuteSet(set TestSet) error {
	type item struct {
		idx int
		tx  TransactionSpec
	}

	// group by client
	groups := make(map[string][]item)
	for idx, tx := range set.Transactions {
		cid := tx.ClientID()
		groups[cid] = append(groups[cid], item{idx: idx, tx: tx})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(groups))

	for _, list := range groups {
		wg.Add(1)
		go func(list []item) {
			//NOTE: to figure out if client is closed loop for the same account ID
			// slog.Info("current list to execute is", "list" , list)
			defer wg.Done()
			for _, it := range list {
				if err := c.executeTransaction(set.Number, it.idx, it.tx); err != nil {
					errCh <- fmt.Errorf("client %s: %w", it.tx.ClientID(), err)
					return
				}
			}
		}(list)
	}

	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

func (c *Controller) executeTransaction(setNumber, idx int, tx TransactionSpec) error {
	req, err := c.buildClientRequest(tx)
	if err != nil {
		return err
	}
	signed, err := c.signRequest(req)
	if err != nil {
		return err
	}

	if tx.Kind == common.RequestBalance {
		if reply, err := c.tryFastRead(signed); err == nil && reply != nil {
			c.printReply(setNumber, idx, tx, reply, "read-quorum")
			return nil
		} else if err != nil {
			slog.Warn("fast read failed, falling back to consensus", "error", err, "tx", tx.Raw)
		}
	}

	reply, err := c.submitWithConsensus(signed)
	if err != nil {
		if errors.Is(err, ErrMaxConsensusAttempts) {
			slog.Warn("transaction skipped after exhausting retries", "set", setNumber, "txIndex", idx, "txn", tx.Description(), "error", err)
			return nil
		}
		return err
	}
	c.printReply(setNumber, idx, tx, reply, "consensus")
	return nil
}

func (c *Controller) PostSetMenu(reader *bufio.Reader, set TestSet) error {
	fmt.Println("Post-set commands: log <node|all>, db <node|all>, status <seq>, view <node|all>, reset [reason], continue, quit")
	for {
		fmt.Printf("[set %d post] > ", set.Number)
		line, err := readLine(reader)
		if err != nil {
			return err
		}
		cmd := strings.TrimSpace(line)
		switch strings.ToLower(cmd) {
		case "", "continue", "next":
			return nil
		case "quit", "exit":
			return io.EOF
		default:
			if err := c.handlePostCommand(cmd); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		}
	}
}

func (c *Controller) handlePostCommand(cmd string) error {
	fields := strings.Fields(strings.ToLower(cmd))
	if len(fields) == 0 {
		return nil
	}

	switch fields[0] {
	case "log":
		target := "all"
		if len(fields) > 1 {
			target = fields[1]
		}
		return c.printLogs(target)
	case "db":
		target := "all"
		if len(fields) > 1 {
			target = fields[1]
		}
		return c.printDatabases(target)
	case "status":
		if len(fields) < 2 {
			return errors.New("usage: status <sequence-number>")
		}
		seq, err := strconv.Atoi(fields[1])
		if err != nil {
			return fmt.Errorf("invalid sequence number %q", fields[1])
		}
		return c.printStatuses(seq)
	case "view":
		target := "all"
		if len(fields) > 1 {
			target = fields[1]
		}
		return c.printViews(target)
	case "reset": //for redundancy
		reason := "user-post-reset"
		if len(fields) > 1 {
			reason = strings.Join(fields[1:], " ")
		}
		return c.resetNodes(reason)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func (c *Controller) buildClientRequest(tx TransactionSpec) (common.ClientReq, error) {
	clientID := tx.ClientID()
	seq := c.nextClientSeq(clientID)
	txnID := common.TransactionID{
		ClientID: clientID,
		UniqueID: fmt.Sprintf("%s-%d-%d", clientID, time.Now().UnixNano(), seq),
	}

	req := common.ClientReq{
		TransactionID: txnID,
		TransactionVal: common.TransactionVal{
			TransactionType: tx.Kind,
		},
	}

	switch tx.Kind {
	case common.RequestTransfer:
		req.TransactionVal.TransactionTransfer = &common.TransferTransaction{
			Sender:   tx.Sender,
			Receiver: tx.Receiver,
			Amount:   tx.Amount,
		}
	case common.RequestBalance:
		req.TransactionVal.TransactionRead = &common.ReadTransaction{
			AcctNum: tx.Account,
		}

		//test

	default:
		return common.ClientReq{}, fmt.Errorf("unsupported transaction type %s", tx.Kind)
	}
	return req, nil
}

func (c *Controller) signRequest(req common.ClientReq) (common.ClientReqSigned, error) {
	key, err := c.getClientKey(req.TransactionID.ClientID)
	if err != nil {
		return common.ClientReqSigned{}, err
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return common.ClientReqSigned{}, err
	}

	return common.ClientReqSigned{
		ClientRequestPayload:   req,
		ClientRequestSignature: ed25519.Sign(key, payload),
	}, nil
}

func (c *Controller) getClientKey(clientID string) (ed25519.PrivateKey, error) {
	canonical := canonicalClientID(clientID)
	if canonical == "" {
		return nil, fmt.Errorf("invalid client id %q", clientID)
	}
	c.clientKeyMu.Lock()
	if key, ok := c.clientKeys[canonical]; ok {
		c.clientKeyMu.Unlock()
		return key, nil
	}
	c.clientKeyMu.Unlock()

	path := filepath.Join("keys", "clients", fmt.Sprintf("client%s_private.pem", canonical))
	key, err := loadEd25519PrivateKey(path)
	if err != nil {
		return nil, err
	}
	c.clientKeyMu.Lock()
	c.clientKeys[canonical] = key
	c.clientKeyMu.Unlock()
	return key, nil
}

func (c *Controller) tryFastRead(req common.ClientReqSigned) (*common.ReplySigned, error) {
	if len(c.activeNodes) == 0 {
		return nil, errors.New("no active replicas for read path")
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.readTimeout)
	defer cancel()

	type readResult struct {
		reply common.ReplySigned
		err   error
	}

	resultCh := make(chan readResult, len(c.activeNodes))
	for nodeID := range c.activeNodes {
		id := nodeID
		go func() {
			var reply common.ReplySigned
			err := c.callNode(id, "Node.SubmitRead", &req, &reply)
			resultCh <- readResult{reply: reply, err: err}
		}()
	}

	tracker := newReplyTracker(c.readQuorum, c.nodeKeys)
	expected := len(c.activeNodes)

	for processed := 0; processed < expected; processed++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("read quorum timeout: %w", ctx.Err())
		case res := <-resultCh:
			if res.err != nil {
				slog.Warn("read RPC failed", "error", res.err)
				continue
			}
			slog.Info("read reply received", "node", res.reply.ReplyPayload.NodeId, "ack", res.reply.ReplyPayload.Ack, "status", res.reply.ReplyPayload.Result.Status)
			// opportunistic view bump on any valid higher view (doesn't resend)
			if res.reply.ReplyPayload.ViewNumber > c.currentView {
				c.updateView(res.reply.ReplyPayload.ViewNumber)
			}
			match, err := tracker.Add(res.reply)
			if err != nil {
				slog.Warn("invalid read reply discarded", "error", err, "node", res.reply.ReplyPayload.NodeId)
				continue
			}
			if match != nil {
				c.updateView(match.ReplyPayload.ViewNumber)
				return match, nil
			}
		}
	}

	return nil, fmt.Errorf("insufficient matching read replies (need %d)", c.readQuorum)
}

func (c *Controller) submitWithConsensus(req common.ClientReqSigned) (*common.ReplySigned, error) {
	txnID := req.ClientRequestPayload.TransactionID.UniqueID
	replyCh := c.execSrv.Register(txnID)
	defer c.execSrv.Unregister(txnID)

	// initial attempt: send only to believed leader
	if err := c.sendToLeader(req); err != nil {
		slog.Warn("leader submit failed", "error", err)
	}

	tracker := newReplyTracker(c.commitQuorum, c.nodeKeys)
	timer := time.NewTimer(c.commitTimeout)
	defer timer.Stop()

	attempt := 0
	const maxAttempts = 3
	resentForView := -1 // avoid spamming resends on the same higher view

	for {
		select {
		case reply, ok := <-replyCh:
			if !ok {
				return nil, errors.New("execution channel closed unexpectedly")
			}

			// Proactive leader bump on any verified higher view
			if reply.ReplyPayload.ViewNumber > c.currentView && reply.ReplyPayload.ViewNumber != resentForView {
				c.updateView(reply.ReplyPayload.ViewNumber)
				_ = c.sendToLeader(req) // re-send to new leader
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(c.commitTimeout)
				resentForView = reply.ReplyPayload.ViewNumber
			}

			match, err := tracker.Add(reply)
			if err != nil {
				slog.Warn("invalid consensus reply discarded", "error", err, "node", reply.ReplyPayload.NodeId)
				continue
			}
			if match != nil {
				c.updateView(match.ReplyPayload.ViewNumber)
				return match, nil
			}

		case <-timer.C:
			if attempt >= maxAttempts {
				return nil, fmt.Errorf("%w: timeout waiting for %d matching replies after %d attempts",
					ErrMaxConsensusAttempts, c.commitQuorum, maxAttempts)
			}
			if attempt == 0 {
				slog.Warn("timeout; broadcasting request to all replicas")
			} else {
				slog.Warn("retry consensus broadcast", "attempt", attempt)
			}
			c.broadcastSubmit(req)
			attempt++
			timer.Reset(c.commitTimeout)
		}
	}
}

func (c *Controller) sendToLeader(req common.ClientReqSigned) error {
	leader := c.currentLeader()
	return c.callSubmitNormal(leader, &req)
}

func (c *Controller) broadcastSubmit(req common.ClientReqSigned) {
	for _, nodeID := range c.nodeOrder {
		id := nodeID
		go func() {
			if err := c.callSubmitNormal(id, &req); err != nil {
				slog.Debug("broadcast submit failed", "nodeID", id, "error", err)
			}
		}()
	}
}

func (c *Controller) callSubmitNormal(nodeID int, req *common.ClientReqSigned) error {
	var resp common.SubmitTxnAck
	if err := c.callNode(nodeID, "Node.SubmitNormal", req, &resp); err != nil {
		return err
	}
	if !resp.Ack {
		return fmt.Errorf("node n%d rejected request: %s", nodeID, resp.Message)
	}
	return nil
}

func (c *Controller) resetNodes(reason string) error {
	slog.Info("calling reset nodes")
	for _, nodeID := range c.nodeOrder {
		var reply serverrpc.ResetStateReply
		slog.Info("calling reset nodes", "nodeID", nodeID)
		if err := c.callNode(nodeID, "Node.ResetState", &serverrpc.ResetStateArgs{Reason: reason}, &reply); err != nil {
			return fmt.Errorf("reset node n%d: %w", nodeID, err)
		}
		if !reply.Flushed {
			slog.Warn("node reset reported warning", "nodeID", nodeID, "message", reply.Message)
		}
	}
	return nil
}

func (c *Controller) applyLiveNodes(live []int, reason string) error {
	desired := make(map[int]struct{}, len(live))
	for _, id := range live {
		desired[id] = struct{}{}
	}

	c.activeNodes = make(map[int]struct{})

	for _, nodeID := range c.nodeOrder {
		if _, ok := desired[nodeID]; ok {
			var reply serverrpc.ReplicaResumeReply
			if err := c.callNode(nodeID, "Node.ReplicaResume", &serverrpc.ReplicaResumeArgs{Reason: reason}, &reply); err != nil {
				return fmt.Errorf("resume node n%d: %w", nodeID, err)
			}
			c.activeNodes[nodeID] = struct{}{}
		} else {
			var reply serverrpc.ReplicaFailureReply
			if err := c.callNode(nodeID, "Node.ReplicaFailure", &serverrpc.ReplicaFailureArgs{Reason: reason}, &reply); err != nil {
				return fmt.Errorf("stop node n%d: %w", nodeID, err)
			}
		}
	}
	return nil
}

func (c *Controller) applyByzantineConfig(nodes []int, attacks []AttackSpec) error {
	byzSet := make(map[int]struct{}, len(nodes))
	for _, id := range nodes {
		byzSet[id] = struct{}{}
	}

	config := buildByzantineConfig(attacks)

	for _, nodeID := range c.nodeOrder {
		if _, ok := byzSet[nodeID]; ok {
			payload := cloneByzantineConfig(config)
			var reply serverrpc.SetByzantineReply
			if err := c.callNode(nodeID, "Node.SetByzantineBehaviour", &payload, &reply); err != nil {
				return fmt.Errorf("set byzantine on n%d: %w", nodeID, err)
			}
			slog.Info("enabled byzantine behaviour", "nodeID", nodeID, "attacks", attacks)
		} else {
			var reply serverrpc.SetByzantineReply
			if err := c.callNode(nodeID, "Node.UnSetByzantineBehaviour", &serverrpc.SetByzantine{}, &reply); err != nil {
				return fmt.Errorf("unset byzantine on n%d: %w", nodeID, err)
			}
		}
	}
	return nil
}

func buildByzantineConfig(attacks []AttackSpec) serverrpc.SetByzantine {
	cfg := serverrpc.SetByzantine{
		TimeDelay: defaultTimingDelayMS,
	}
	targetSet := make(map[int]struct{})
	equivSet := make(map[int]struct{})

	for _, attack := range attacks {
		switch attack.Kind {
		case "sign":
			cfg.InvalidSignatureAttackFlag = true
		case "crash":
			cfg.CrashAttackFlag = true
		case "dark":
			cfg.InDarkAttackFlag = true
			for _, t := range attack.Targets {
				targetSet[t] = struct{}{}
			}
		case "time":
			cfg.TimingAttackFlag = true
			if attack.DelayMS > 0 {
				cfg.TimeDelay = attack.DelayMS
			}
		case "equivocation":
			cfg.EquivocationAttackFlag = true
			for _, t := range attack.Targets {
				equivSet[t] = struct{}{}
			}
		}
	}

	if cfg.InDarkAttackFlag {
		cfg.InDarkTarget = mapKeys(targetSet)
	}
	if cfg.EquivocationAttackFlag {
		cfg.EquivocationSeqNtarget = mapKeys(equivSet)
	}
	if cfg.TimingAttackFlag && cfg.TimeDelay <= 0 {
		cfg.TimeDelay = defaultTimingDelayMS
	}
	if cfg.InvalidSignatureAttackFlag || cfg.CrashAttackFlag || cfg.InDarkAttackFlag ||
		cfg.TimingAttackFlag || cfg.EquivocationAttackFlag {
		cfg.IsByzantine = true
	}
	return cfg
}

func cloneByzantineConfig(cfg serverrpc.SetByzantine) serverrpc.SetByzantine {
	payload := cfg
	if len(cfg.InDarkTarget) > 0 {
		payload.InDarkTarget = append([]int(nil), cfg.InDarkTarget...)
	}
	if len(cfg.EquivocationSeqNtarget) > 0 {
		payload.EquivocationSeqNtarget = append([]int(nil), cfg.EquivocationSeqNtarget...)
	}
	return payload
}

func mapKeys(m map[int]struct{}) []int {
	if len(m) == 0 {
		return nil
	}
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func (c *Controller) printReply(set, idx int, tx TransactionSpec, reply *common.ReplySigned, path string) {
	payload := reply.ReplyPayload
	fmt.Printf("[Set %d Tx %d] %s via %s -> status=%v msg=%s balance=%d view=%d node=n%d\n",
		set, idx+1, tx.Description(), path, payload.Result.Status, payload.Result.Message,
		payload.Result.Balance, payload.ViewNumber, payload.NodeId)
}

func (c *Controller) printLogs(selector string) error {
	nodeIDs, err := c.resolveNodeSelector(selector)
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		var reply serverrpc.PrintLogReply
		if err := c.callNode(nodeID, "Node.PrintLog", &serverrpc.PrintLogArgs{}, &reply); err != nil {
			fmt.Printf("n%d log error: %v\n", nodeID, err)
			continue
		}
		fmt.Printf("Node n%d log entries (%d):\n", nodeID, len(reply.Entries))
		if len(reply.Entries) == 0 {
			fmt.Println("  <empty>")
			continue
		}
		for _, entry := range reply.Entries {
			fmt.Printf("  seq=%d view=%d status=%s client=%s txn=%s type=%s",
				entry.Sequence, entry.View, entry.Status, entry.ClientID, entry.TransactionID, entry.Type)
			if entry.Type == common.RequestTransfer {
				fmt.Printf(" sender=%s receiver=%s amount=%d", entry.Sender, entry.Receiver, entry.Amount)
			} else if entry.Type == common.RequestBalance {
				fmt.Printf(" account=%s", entry.ReadAccount)
			}
			if entry.Digest != "" {
				fmt.Printf(" digest=%s", entry.Digest)
			}
			fmt.Println()
		}
	}
	return nil
}

func (c *Controller) printDatabases(selector string) error {
	nodeIDs, err := c.resolveNodeSelector(selector)
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		var reply serverrpc.PrintDBReply
		if err := c.callNode(nodeID, "Node.PrintDB", &serverrpc.PrintDBArgs{}, &reply); err != nil {
			fmt.Printf("n%d db error: %v\n", nodeID, err)
			continue
		}
		fmt.Printf("Node n%d balances:\n", nodeID)
		if len(reply.Balances) == 0 {
			fmt.Println("  <empty>")
			continue
		}
		accounts := make([]string, 0, len(reply.Balances))
		for acct := range reply.Balances {
			accounts = append(accounts, acct)
		}
		sort.Strings(accounts)
		for _, acct := range accounts {
			fmt.Printf("  %s: %d\n", acct, reply.Balances[acct])
		}
	}
	return nil
}

func (c *Controller) printStatuses(seq int) error {
	for _, nodeID := range c.nodeOrder {
		var reply serverrpc.PrintStatusReply
		if err := c.callNode(nodeID, "Node.PrintStatus", &serverrpc.PrintStatusArgs{Sequence: seq}, &reply); err != nil {
			fmt.Printf("n%d status error: %v\n", nodeID, err)
			continue
		}
		fmt.Printf("n%d -> status=%s txn=%s\n", nodeID, reply.Status, reply.TransactionID)
	}
	return nil
}

func (c *Controller) printViews(selector string) error {
	nodeIDs, err := c.resolveNodeSelector(selector)
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		var reply serverrpc.PrintViewReply
		if err := c.callNode(nodeID, "Node.PrintView", &serverrpc.PrintViewArgs{}, &reply); err != nil {
			fmt.Printf("n%d view error: %v\n", nodeID, err)
			continue
		}
		fmt.Printf("Node n%d new-view history (%d entries):\n", nodeID, len(reply.Views))
		if len(reply.Views) == 0 {
			fmt.Println("  <none>")
			continue
		}
		for _, view := range reply.Views {
			fmt.Printf("  view=%d primary=n%d senders=%v proofs=%v at=%s\n",
				view.View, view.Primary, view.ViewChangeSenders, view.IncludedProofs, view.Time.Format(time.RFC3339))
		}
	}
	return nil
}

func (c *Controller) resolveNodeSelector(selector string) ([]int, error) {
	if selector == "" || strings.EqualFold(selector, "all") {
		return append([]int(nil), c.nodeOrder...), nil
	}
	sel := strings.TrimSpace(strings.ToLower(selector))
	sel = strings.TrimPrefix(sel, "n")
	id, err := strconv.Atoi(sel)
	if err != nil {
		return nil, fmt.Errorf("invalid node selector %q", selector)
	}
	for _, nodeID := range c.nodeOrder {
		if nodeID == id {
			return []int{id}, nil
		}
	}
	return nil, fmt.Errorf("unknown node n%d", id)
}

func (c *Controller) callNode(nodeID int, method string, req, resp interface{}) error {
	addr, ok := c.nodeAddrs[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %d", nodeID)
	}
	timeout := c.dialTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, req, resp)
}

func (c *Controller) nextClientSeq(clientID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.perClientSeq[clientID]++
	return c.perClientSeq[clientID]
}

func (c *Controller) currentLeader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return leaderForView(c.currentView, c.totalNodes)
}

func (c *Controller) updateView(newView int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if newView > c.currentView {
		c.currentView = newView
		slog.Info("detected view change", "view", newView, "leader", leaderForView(newView, c.totalNodes))
	}
}

func leaderForView(view, total int) int {
	if total <= 0 {
		return 1
	}
	if view <= 0 {
		view = 1
	}
	v := view % total
	if v == 0 {
		v = total
	}
	return v
}

func loadNodePublicKeys() (map[int]ed25519.PublicKey, error) {
	root := filepath.Join("keys", "nodes")
	keys := make(map[int]ed25519.PublicKey)

	for nodeID := 1; nodeID <= 7; nodeID++ {
		path := filepath.Join(root, fmt.Sprintf("node%d_public.pem", nodeID))
		key, err := loadEd25519PublicKey(path)
		if err != nil {
			return nil, fmt.Errorf("load node %d key: %w", nodeID, err)
		}
		keys[nodeID] = key
	}
	return keys, nil
}

func loadEd25519PublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("invalid public key pem %s", path)
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := pub.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%s is not an ed25519 key", path)
	}
	return key, nil
}

func loadEd25519PrivateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("invalid private key pem %s", path)
	}
	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := priv.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s is not an ed25519 private key", path)
	}
	return key, nil
}

type replyTracker struct {
	mu       sync.Mutex
	seen     map[int]struct{}
	groups   map[string]*replyGroup
	nodeKeys map[int]ed25519.PublicKey
	quorum   int
}

type replyGroup struct {
	count  int
	sample common.ReplySigned
}

func newReplyTracker(quorum int, keys map[int]ed25519.PublicKey) *replyTracker {
	return &replyTracker{
		seen:     make(map[int]struct{}),
		groups:   make(map[string]*replyGroup),
		nodeKeys: keys,
		quorum:   quorum,
	}
}

func (t *replyTracker) Add(reply common.ReplySigned) (*common.ReplySigned, error) {
	nodeID := reply.ReplyPayload.NodeId
	key, ok := t.nodeKeys[nodeID]
	if !ok || len(key) == 0 {
		return nil, fmt.Errorf("unknown node %d", nodeID)
	}

	payload, err := json.Marshal(reply.ReplyPayload)
	if err != nil {
		return nil, err
	}
	if !ed25519.Verify(key, payload, reply.ReplySignature) {
		return nil, fmt.Errorf("invalid signature from node %d", nodeID)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.seen[nodeID]; exists {
		return nil, nil
	}
	t.seen[nodeID] = struct{}{}

	keyStr := responseKey(reply)
	group := t.groups[keyStr]
	if group == nil {
		group = &replyGroup{sample: reply}
		t.groups[keyStr] = group
	}
	group.count++
	if group.count >= t.quorum {
		return &group.sample, nil
	}
	return nil, nil
}

func responseKey(reply common.ReplySigned) string {
	payload := reply.ReplyPayload
	result := payload.Result
	return fmt.Sprintf("%s|%t|%s|%d|%s",
		payload.TransactionID.UniqueID,
		result.Status,
		result.TransactionType,
		result.Balance,
		result.Message,
	)
}

func canonicalClientID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return ""
	}
	upper := strings.ToUpper(trimmed)
	return strings.TrimPrefix(upper, "CLIENT")
}
