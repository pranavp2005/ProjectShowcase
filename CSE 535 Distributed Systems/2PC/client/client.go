package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pranavpateriya.com/distributed-sys/common"
)

type rawNodeConfig struct {
	NumCluster           int                     `json:"num_cluster"`
	FVal                 int                     `json:"f_val"`
	InitialBalance       int                     `json:"initial_balance"`
	LegacyInitialBalance int                     `json:"inital_balance"`
	Nodes                map[string]rawNodeEntry `json:"nodes"`
	Shards               map[string][]int        `json:"shards"`
}

type rawNodeEntry struct {
	Address string   `json:"address"`
	Cluster int      `json:"cluster"`
	Role    *int     `json:"role,omitempty"`
	Peers   []string `json:"peers,omitempty"`
}

type nodeInfo struct {
	id       int
	cluster  int
	addr     string
	isLeader bool
}

type clusterInfo struct {
	id       int
	accounts []int
	nodes    []int
	leader   int
}

type shardRange struct {
	accounts []int
	cluster  int
}

type commandKind string

const (
	cmdTransfer commandKind = "transfer"
	cmdBalance  commandKind = "balance"
	cmdFail     commandKind = "fail"
	cmdRecover  commandKind = "recover"
)

type testCommand struct {
	kind        commandKind
	from        int
	to          int
	amount      int
	consistency common.ConsistencyLevel
	nodeID      int
	raw         string
}

type batchCommand struct {
	cmd testCommand
	idx int
}

type testSet struct {
	id        string
	commands  []testCommand
	liveNodes []int
}

type txnRecord struct {
	from   int
	to     int
	amount int
}

type viewEvent struct {
	clusterID int
	detail    string
	at        time.Time
}

type client struct {
	nodes            map[int]nodeInfo
	clusters         map[int]*clusterInfo
	shardRanges      []shardRange
	initialMapping   map[int]int
	accountToCluster map[int]int
	liveNodes        map[int]bool
	testSets         []testSet
	nextSet          int
	initialBalance   int

	modifiedAccounts map[int]struct{}
	txnDurations     []time.Duration
	txnHistory       []txnRecord
	viewEvents       []viewEvent
	txnStart         time.Time
	txnEnd           time.Time

	serverProcs map[int]*exec.Cmd
	configPath  string
	procMu      sync.Mutex
	mu          sync.Mutex
}

func main() {
	cfgPath := flag.String("config", "config/node_config.json", "path to node config")
	testsPath := flag.String("tests", "CSE535-F25-Project-3-Testcases.csv", "path to test cases csv")
	flag.Parse()

	c, err := newClient(*cfgPath, *testsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "client init failed: %v\n", err)
		os.Exit(1)
	}
	if err := c.interactive(); err != nil {
		fmt.Fprintf(os.Stderr, "client error: %v\n", err)
		os.Exit(1)
	}
}

func newClient(cfgPath, testPath string) (*client, error) {
	rawCfg, err := loadConfig(cfgPath)
	if err != nil {
		return nil, err
	}

	nodes := make(map[int]nodeInfo)
	clusters := make(map[int]*clusterInfo)
	for idStr, entry := range rawCfg.Nodes {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid node id %q: %w", idStr, err)
		}
		isLeader := entry.Role != nil && *entry.Role == 1
		nodes[id] = nodeInfo{id: id, cluster: entry.Cluster, addr: entry.Address, isLeader: isLeader}
		c, ok := clusters[entry.Cluster]
		if !ok {
			c = &clusterInfo{id: entry.Cluster}
			clusters[entry.Cluster] = c
		}
		c.nodes = append(c.nodes, id)
		if isLeader {
			c.leader = id
		}
	}

	for _, c := range clusters {
		sort.Ints(c.nodes)
		if c.leader == 0 && len(c.nodes) > 0 {
			c.leader = c.nodes[0]
		}
	}

	shardRanges := normalizeShards(rawCfg.Shards, clusters)

	for _, r := range shardRanges {
		if c, ok := clusters[r.cluster]; ok {
			c.accounts = r.accounts
		}
	}

	initialMapping := buildInitialMapping(shardRanges)
	initialBalance := rawCfg.InitialBalance
	if initialBalance == 0 {
		initialBalance = rawCfg.LegacyInitialBalance
	}
	if initialBalance == 0 {
		initialBalance = 10
	}

	testSets, err := loadTestSets(testPath)
	if err != nil {
		return nil, err
	}

	liveNodes := make(map[int]bool, len(nodes))
	for id := range nodes {
		liveNodes[id] = true
	}

	return &client{
		nodes:            nodes,
		configPath:       cfgPath,
		clusters:         clusters,
		shardRanges:      shardRanges,
		initialMapping:   initialMapping,
		accountToCluster: cloneMapping(initialMapping),
		liveNodes:        liveNodes,
		testSets:         testSets,
		initialBalance:   initialBalance,
		modifiedAccounts: make(map[int]struct{}),
		serverProcs:      make(map[int]*exec.Cmd),
	}, nil
}

func loadConfig(path string) (rawNodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return rawNodeConfig{}, err
	}
	var cfg rawNodeConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return rawNodeConfig{}, err
	}
	return cfg, nil
}

func normalizeShards(shards map[string][]int, clusters map[int]*clusterInfo) []shardRange {
	var ranges []shardRange
	for key, bounds := range shards {
		if len(bounds) == 0 {
			continue
		}

		accounts := bounds
		if len(bounds) == 2 && bounds[0] <= bounds[1] && bounds[1]-bounds[0] > 1 {
			size := bounds[1] - bounds[0] + 1
			accounts = make([]int, size)
			for i := 0; i < size; i++ {
				accounts[i] = bounds[0] + i
			}
		}
		rawID, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		clusterID := rawID
		if _, ok := clusters[clusterID]; !ok {
			if _, ok2 := clusters[rawID-1]; ok2 {
				clusterID = rawID - 1
			}
		}

		unique := make(map[int]struct{}, len(accounts))
		normalized := make([]int, 0, len(accounts))
		for _, acct := range accounts {
			if _, ok := unique[acct]; ok {
				continue
			}
			unique[acct] = struct{}{}
			normalized = append(normalized, acct)
		}
		sort.Ints(normalized)
		ranges = append(ranges, shardRange{
			accounts: normalized,
			cluster:  clusterID,
		})
	}
	return ranges
}

func buildInitialMapping(ranges []shardRange) map[int]int {
	mapping := make(map[int]int)
	for _, r := range ranges {
		for _, id := range r.accounts {
			mapping[id] = r.cluster
		}
	}
	return mapping
}

func cloneMapping(in map[int]int) map[int]int {
	out := make(map[int]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (c *client) startServers() error {
	c.procMu.Lock()
	if len(c.serverProcs) > 0 {
		c.procMu.Unlock()
		fmt.Println("servers already running")
		return nil
	}
	c.procMu.Unlock()

	logDir := filepath.Join("persistence", "log")
	stateDir := filepath.Join("persistence", "state")

	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	for clusterID := range c.clusters {
		_ = os.MkdirAll(filepath.Join(stateDir, fmt.Sprintf("cluster%d", clusterID)), 0o755)
	}

	nodeIDs := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Ints(nodeIDs)

	for _, id := range nodeIDs {
		cmd := exec.Command("go", "run", "./server", "-id", strconv.Itoa(id), "-config", c.configPath)

		if err := cmd.Start(); err != nil {
			c.stopServers()
			return fmt.Errorf("start node %d: %w", id, err)
		}

		c.procMu.Lock()
		c.serverProcs[id] = cmd
		c.procMu.Unlock()
	}

	fmt.Printf("started %d server processes\n", len(nodeIDs))
	return nil
}

func (c *client) stopServers() {
	c.procMu.Lock()
	procs := c.serverProcs
	c.serverProcs = make(map[int]*exec.Cmd)
	c.procMu.Unlock()

	for id, cmd := range procs {
		if cmd == nil || cmd.Process == nil {
			continue
		}

		_ = cmd.Process.Signal(os.Interrupt)

		done := make(chan struct{})
		go func(cmd *exec.Cmd) {
			_ = cmd.Wait()
			close(done)
		}(cmd)

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}

		fmt.Printf("stopped node %d\n", id)
	}
}

func (c *client) cleanupPersistence() error {
	logDir := filepath.Join("persistence", "log")
	stateDir := filepath.Join("persistence", "state")

	if err := os.RemoveAll(logDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove log dir: %w", err)
	}
	if err := os.RemoveAll(stateDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove state dir: %w", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return fmt.Errorf("recreate log dir: %w", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("recreate state dir: %w", err)
	}
	return nil
}

func (c *client) endServers() error {
	c.stopServers()
	if err := c.cleanupPersistence(); err != nil {
		return err
	}
	fmt.Println("servers stopped and state cleared")
	return nil
}

// clusterForAccount returns the cluster ID for the given account if known.
func (c *client) clusterForAccount(acct int) (int, bool) {
	id, ok := c.accountToCluster[acct]
	return id, ok
}

// activeNodesInCluster returns the node IDs in the cluster that are currently marked live.
func (c *client) activeNodesInCluster(clusterID int) []int {
	cl, ok := c.clusters[clusterID]
	if !ok {
		return nil
	}
	var live []int
	for _, id := range cl.nodes {
		if c.liveNodes[id] {
			live = append(live, id)
		}
	}
	return live
}

func loadTestSets(path string) ([]testSet, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var sets []testSet
	var current *testSet
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		setID := strings.TrimSpace(row[0])
		cmdStr := strings.TrimSpace(row[1])
		liveStr := ""
		if len(row) > 2 {
			liveStr = strings.TrimSpace(row[2])
		}

		// Skip header rows like: Set Number, Transactions, Live Nodes
		if strings.EqualFold(setID, "set number") || strings.EqualFold(cmdStr, "transactions") || strings.EqualFold(liveStr, "live nodes") {
			continue
		}

		if setID != "" {
			if current != nil {
				sets = append(sets, *current)
			}
			current = &testSet{id: setID}
			if liveStr != "" {
				current.liveNodes = parseNodeList(liveStr)
			}
		}

		if current == nil || cmdStr == "" {
			continue
		}
		cmd, err := parseCommand(cmdStr)
		if err != nil {
			return nil, fmt.Errorf("parse command %q: %w", cmdStr, err)
		}
		current.commands = append(current.commands, cmd)
	}
	if current != nil {
		sets = append(sets, *current)
	}

	return sets, nil
}

var nodeListRe = regexp.MustCompile(`\d+`)

func parseNodeList(raw string) []int {
	matches := nodeListRe.FindAllString(raw, -1)
	out := make([]int, 0, len(matches))
	for _, m := range matches {
		if id, err := strconv.Atoi(m); err == nil {
			out = append(out, id)
		}
	}
	sort.Ints(out)
	return out
}

func parseConsistencyLevel(raw string) common.ConsistencyLevel {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "linearizable":
		return common.Linearizable
	case "eventual":
		return common.Eventual
	case "majority":
		return common.Majority
	default: //default consistency is linearizable,
		return common.Linearizable
	}
}

func parseCommand(raw string) (testCommand, error) {
	raw = strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(raw, "F(") && strings.HasSuffix(raw, ")"):
		id, err := strconv.Atoi(strings.Trim(raw[2:len(raw)-1], "n "))
		if err != nil {
			return testCommand{}, err
		}
		return testCommand{kind: cmdFail, nodeID: id, raw: raw}, nil
	case strings.HasPrefix(raw, "R(") && strings.HasSuffix(raw, ")"):
		id, err := strconv.Atoi(strings.Trim(raw[2:len(raw)-1], "n "))
		if err != nil {
			return testCommand{}, err
		}
		return testCommand{kind: cmdRecover, nodeID: id, raw: raw}, nil
	}

	clean := strings.Trim(raw, "() ")
	parts := strings.Split(clean, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	switch len(parts) {
	case 1, 2:
		val, err := strconv.Atoi(parts[0])
		if err != nil {
			return testCommand{}, err
		}
		level := common.Linearizable
		if len(parts) == 2 {
			level = parseConsistencyLevel(parts[1])
		}
		return testCommand{kind: cmdBalance, from: val, consistency: level, raw: raw}, nil
	case 3, 4:
		from, err1 := strconv.Atoi(parts[0])
		to, err2 := strconv.Atoi(parts[1])
		amt, err3 := strconv.Atoi(parts[2])
		if err1 != nil || err2 != nil || err3 != nil {
			return testCommand{}, fmt.Errorf("unable to parse transfer %q", raw)
		}
		level := common.Linearizable //I don't actually need to set a consistency level in Transfer operation
		if len(parts) == 4 {
			level = parseConsistencyLevel(parts[3])
		}
		return testCommand{kind: cmdTransfer, from: from, to: to, amount: amt, consistency: level, raw: raw}, nil
	default:
		return testCommand{}, fmt.Errorf("unrecognized command %q", raw)
	}
}

func (c *client) interactive() error {
	fmt.Printf("Loaded %d test sets. Type 'start' to launch servers, 'end' to stop and clear state, 'init' to seed balances, 'next' to run the next set, 'printbalance <acct>', 'printlog <nodeID>', 'printlogslot <nodeID> <slot>', 'printstatus', 'printdb', 'printview', 'performance', 'printreshard', or 'quit'.\n", len(c.testSets))
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return scanner.Err()
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)

		switch {
		case lower == "quit" || lower == "exit":
			return nil
		case lower == "help":
			fmt.Println("Commands: start, end, init, next, printbalance <acctID>, printlog <nodeID>, printlogslot <nodeID> <slot>, printstatus, printdb, printview, performance, printreshard, quit")
		case lower == "start":
			if err := c.startServers(); err != nil {
				fmt.Printf("start failed: %v\n", err)
			}
		case lower == "end":
			if err := c.endServers(); err != nil {
				fmt.Printf("end failed: %v\n", err)
			}
		case lower == "init":
			if err := c.initClusters(); err != nil {
				fmt.Printf("init failed: %v\n", err)
			}
		case lower == "next":
			if err := c.runNextSet(); err != nil {
				fmt.Printf("set error: %v\n", err)
			}
		case strings.HasPrefix(lower, "printbalance"):
			fields := strings.Fields(line)
			if len(fields) != 2 {
				fmt.Println("usage: printbalance <acctID>")
				continue
			}
			acct, err := strconv.Atoi(fields[1])
			if err != nil {
				fmt.Printf("invalid account id: %v\n", err)
				continue
			}
			if err := c.printBalance(acct); err != nil {
				fmt.Printf("printbalance failed: %v\n", err)
			}
		case lower == "printdb":
			if err := c.printDB(); err != nil {
				fmt.Printf("printdb failed: %v\n", err)
			}
		case lower == "printview":
			c.printView()
		case lower == "performance":
			c.printPerformance()
		case strings.HasPrefix(lower, "printlogslot"):
			fields := strings.Fields(line)
			if len(fields) != 3 {
				fmt.Println("usage: printlogslot <nodeID> <slot>")
				continue
			}
			nodeID, err1 := strconv.Atoi(fields[1])
			slot, err2 := strconv.Atoi(fields[2])
			if err1 != nil || err2 != nil {
				fmt.Println("usage: printlogslot <nodeID> <slot>")
				continue
			}
			if err := c.printLogSlot(nodeID, slot); err != nil {
				fmt.Printf("printlogslot failed: %v\n", err)
			}
		case strings.HasPrefix(lower, "printlog"):
			fields := strings.Fields(line)
			if len(fields) != 2 {
				fmt.Println("usage: printlog <nodeID>")
				continue
			}
			nodeID, err := strconv.Atoi(fields[1])
			if err != nil {
				fmt.Println("usage: printlog <nodeID>")
				continue
			}
			if err := c.printLog(nodeID); err != nil {
				fmt.Printf("printlog failed: %v\n", err)
			}
		case lower == "printstatus":
			if err := c.printStatus(); err != nil {
				fmt.Printf("printstatus failed: %v\n", err)
			}
		case lower == "printreshard":
			if err := c.runReshard(); err != nil {
				fmt.Printf("reshard failed: %v\n", err)
			}
		default:
			fmt.Println("unknown command; type help")
		}
	}
}

func (c *client) runNextSet() error {
	if c.nextSet >= len(c.testSets) {
		fmt.Println("no more sets to run")
		return nil
	}
	set := c.testSets[c.nextSet]
	c.nextSet++

	fmt.Printf("Preparing set %s ...\n", set.id)
	if err := c.flushState(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}
	if len(set.liveNodes) > 0 {
		c.applyLiveNodes(set.liveNodes)
	}

	c.modifiedAccounts = make(map[int]struct{})
	c.txnDurations = nil
	c.txnHistory = nil
	c.viewEvents = nil
	c.txnStart = time.Time{}
	c.txnEnd = time.Time{}

	var batch []batchCommand
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.runBatch(batch, set.id); err != nil {
			fmt.Printf("batch error: %v\n", err)
		}
		batch = batch[:0]
	}

	for idx, cmd := range set.commands {
		switch cmd.kind {
		case cmdTransfer, cmdBalance:
			batch = append(batch, batchCommand{cmd: cmd, idx: idx})
		case cmdFail:
			flushBatch()
			c.markFailed(cmd.nodeID, set.id)
		case cmdRecover:
			flushBatch()
			c.markRecovered(cmd.nodeID, set.id)
		}
	}
	flushBatch()

	fmt.Printf("Set %s complete. Use printdb/printbalance/performance/printview, then type 'next' for the next set.\n", set.id)
	return nil
}

func (c *client) initClusters() error {
	fmt.Println("Initializing clusters with configured shard ranges ...")
	if err := c.flushState(); err != nil {
		return err
	}

	c.modifiedAccounts = make(map[int]struct{})
	c.txnDurations = nil
	c.txnHistory = nil
	c.viewEvents = nil
	c.txnStart = time.Time{}
	c.txnEnd = time.Time{}

	fmt.Println("Initialization complete.")
	return nil
}

func (c *client) runBatch(batch []batchCommand, setID string) error {
	if len(batch) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(batch))

	for _, item := range batch {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			switch item.cmd.kind {
			case cmdTransfer:
				err = c.handleTransfer(item.cmd, setID, item.idx)
			case cmdBalance:
				err = c.handleBalance(item.cmd, setID, item.idx)
			default:
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("%s failed: %w", item.cmd.raw, err)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("batch had %d errors (first: %v)", len(errs), errs[0])
}

func (c *client) flushState() error {
	c.accountToCluster = cloneMapping(c.initialMapping)
	for id := range c.liveNodes {
		c.liveNodes[id] = true
	}

	// ensure all nodes are recovered before resetting balances
	for nodeID := range c.nodes {
		role := c.nodeRole(nodeID)
		_ = c.recoverNodeWithRole(nodeID, role)
	}

	for _, cl := range c.clusters {
		if len(cl.accounts) == 0 {
			continue
		}
		balances := make(map[int]int, len(cl.accounts))
		for _, acct := range cl.accounts {
			balances[acct] = c.initialBalance
		}
		req := &common.ResetBalanceReq{Balances: balances}
		for _, nodeID := range cl.nodes {
			resp := &common.ResetbalanceResp{}
			addr := c.nodes[nodeID].addr
			if err := callRPCWithTimeout(addr, "Node.ReshardNode", req, resp, 15*time.Second); err != nil {
				slog.Warn("reset rpc failed", "node", nodeID, "error", err)
				continue
			}
			if !resp.Status {
				slog.Warn("reset rejected", "node", nodeID, "msg", resp.Message)
			}
		}
	}
	return nil
}

func (c *client) applyLiveNodes(live []int) {
	allowed := make(map[int]struct{}, len(live))
	for _, id := range live {
		allowed[id] = struct{}{}
	}
	for id := range c.nodes {
		if _, ok := allowed[id]; ok {
			c.liveNodes[id] = true
			role := c.nodeRole(id)
			_ = c.recoverNodeWithRole(id, role)
		} else {
			c.liveNodes[id] = false
			_ = c.failNode(id)
		}
	}
}

func (c *client) recordTransferMetrics(start time.Time, duration time.Duration, from, to, amount int) {
	end := start.Add(duration)
	c.mu.Lock()
	c.txnDurations = append(c.txnDurations, duration)
	c.txnHistory = append(c.txnHistory, txnRecord{from: from, to: to, amount: amount})
	c.modifiedAccounts[from] = struct{}{}
	c.modifiedAccounts[to] = struct{}{}
	if c.txnStart.IsZero() || start.Before(c.txnStart) {
		c.txnStart = start
	}
	if end.After(c.txnEnd) {
		c.txnEnd = end
	}
	c.mu.Unlock()
}

func (c *client) recordDuration(start time.Time, duration time.Duration) {
	end := start.Add(duration)
	c.mu.Lock()
	c.txnDurations = append(c.txnDurations, duration)
	if c.txnStart.IsZero() || start.Before(c.txnStart) {
		c.txnStart = start
	}
	if end.After(c.txnEnd) {
		c.txnEnd = end
	}
	c.mu.Unlock()
}

func (c *client) addViewEvent(ev viewEvent) {
	c.mu.Lock()
	c.viewEvents = append(c.viewEvents, ev)
	c.mu.Unlock()
}

func (c *client) markFailed(nodeID int, setID string) {
	c.liveNodes[nodeID] = false
	time.Sleep(1 * time.Second)
	if err := c.failNode(nodeID); err != nil {
		slog.Error("failed to fail node", nodeID)
	}
	time.Sleep(2 * time.Second)
	c.addViewEvent(viewEvent{
		clusterID: c.nodes[nodeID].cluster,
		detail:    fmt.Sprintf("node n%d marked failed during set %s", nodeID, setID),
		at:        time.Now(),
	})
}

func (c *client) markRecovered(nodeID int, setID string) {
	c.liveNodes[nodeID] = true
	time.Sleep(1 * time.Second)
	if err := c.recoverNode(nodeID); err != nil {
		slog.Error("failed to recover node", nodeID)
	}
	time.Sleep(2 * time.Second)
	c.addViewEvent(viewEvent{
		clusterID: c.nodes[nodeID].cluster,
		detail:    fmt.Sprintf("node n%d recovered during set %s", nodeID, setID),
		at:        time.Now(),
	})
}

func (c *client) handleTransfer(cmd testCommand, setID string, idx int) error {
	fromCluster := c.accountToCluster[cmd.from]
	toCluster := c.accountToCluster[cmd.to]

	reqType := common.RequestTypeInstraShardTransfer
	method := "Node.IntraShardTransfer"
	kind := "intrashard"
	if fromCluster != toCluster {
		reqType = common.RequestTypeCrossshardTransfer
		method = "Node.CrossShardTransfer"
		kind = "crossshard"
	}

	req := &common.ClientReqConsLevel{
		ClientReq: common.ClientReq{
			RequestType: reqType,
			TranscationVal: common.TransactionVal{
				From:   cmd.from,
				To:     cmd.to,
				Amount: cmd.amount,
			},
			TransactionID: common.TransactionID{
				TransactionID: fmt.Sprintf("set-%s-%d-%d", setID, idx, time.Now().UnixNano()),
				ClientID:      "client",
			},
		},
		ConsistencyLevel: cmd.consistency,
	}

	slog.Info("client sending transfer", "kind", kind, "from", cmd.from, "to", cmd.to, "amt", cmd.amount, "txn", req.ClientReq.TransactionID.TransactionID, "set", setID)
	start := time.Now()
	var resp common.ClientReply
	err := c.callCluster(fromCluster, method, req, &resp)
	duration := time.Since(start)
	c.recordTransferMetrics(start, duration, cmd.from, cmd.to, cmd.amount)

	if err != nil {
		slog.Warn("client transfer failed", "kind", kind, "from", cmd.from, "to", cmd.to, "error", err)
		return err
	}
	if !resp.Status {
		slog.Warn("client transfer rejected", "kind", kind, "from", cmd.from, "to", cmd.to, "msg", resp.Message)
		return fmt.Errorf("server rejected transfer: %s", resp.Message)
	}
	slog.Info("client transfer success", "kind", kind, "from", cmd.from, "to", cmd.to, "latency_ms", duration.Milliseconds())
	return nil
}

func (c *client) handleBalance(cmd testCommand, setID string, idx int) error {
	clusterID := c.accountToCluster[cmd.from]
	txnID := fmt.Sprintf("set-%s-%d-%d", setID, idx, time.Now().UnixNano())
	req := &common.ClientReqConsLevel{
		ClientReq: common.ClientReq{
			RequestType: common.RequestTypeRead,
			TranscationVal: common.TransactionVal{
				From: cmd.from,
			},
			TransactionID: common.TransactionID{
				TransactionID: txnID,
				ClientID:      "client",
			},
		},
		ConsistencyLevel: cmd.consistency,
	}

	slog.Info("client sending balance", "acct", cmd.from, "set", setID, "txn", txnID, "consistency", cmd.consistency)
	start := time.Now()

	var balance int
	var err error

	switch cmd.consistency {
	case common.Linearizable:
		balance, err = c.readLinearizable(clusterID, req)
	case common.Eventual:
		live := c.activeNodesInCluster(clusterID)
		if len(live) == 0 {
			err = fmt.Errorf("no live nodes in cluster %d", clusterID)
			break
		}
		c.mu.Lock()
		choice := live[rand.Intn(len(live))]
		c.mu.Unlock()
		balance, err = c.readFromNode(clusterID, choice, req)
	case common.Majority:
		balance, err = c.readFromMajority(clusterID, req)
	default:
		err = fmt.Errorf("unknown consistency level %v", cmd.consistency)
	}

	duration := time.Since(start)
	c.recordDuration(start, duration)
	if err != nil {
		slog.Warn("client balance failed", "acct", cmd.from, "error", err)
		return err
	}
	slog.Info("client balance success", "acct", cmd.from, "balance", balance, "latency_ms", duration.Milliseconds())
	fmt.Printf("Balance(%d) -> %d\n", cmd.from, balance)
	return nil
}

func (c *client) readFromNode(clusterID, nodeID int, req *common.ClientReqConsLevel) (int, error) {
	addr := c.nodes[nodeID].addr
	slog.Info("client balance attempt", "node", nodeID, "cluster", clusterID, "consistency", req.ConsistencyLevel)
	var resp common.ClientReply
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		err = callRPC(addr, "Node.ReadOnly", req, &resp)
		if err == nil && resp.Status {
			return resp.Balance, nil
		}

		if err == nil && !resp.Status {
			err = fmt.Errorf("read rejected: %s", resp.Message)
		}

		slog.Warn("client balance retry", "node", nodeID, "attempt", attempt, "error", err)
		time.Sleep(50 * time.Millisecond)
	}
	return 0, err
}

func (c *client) readFromMajority(clusterID int, req *common.ClientReqConsLevel) (int, error) {
	cl, ok := c.clusters[clusterID]
	if !ok || len(cl.nodes) == 0 {
		return 0, fmt.Errorf("cluster %d has no nodes", clusterID)
	}

	required := len(cl.nodes)/2 + 1
	counts := make(map[int]int)
	success := 0
	liveSeen := 0
	var firstErr error

	for _, nodeID := range cl.nodes {
		if !c.liveNodes[nodeID] {
			continue
		}
		liveSeen++

		bal, err := c.readFromNode(clusterID, nodeID, req)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		counts[bal]++
		success++
		if counts[bal] >= required {
			return bal, nil
		}
	}

	if liveSeen < required {
		return 0, fmt.Errorf("not enough live nodes to reach majority in cluster %d", clusterID)
	}
	if success == 0 {
		if firstErr != nil {
			return 0, firstErr
		}
		return 0, fmt.Errorf("no successful reads in cluster %d", clusterID)
	}

	var majorityBal int
	var majorityCnt int
	for bal, cnt := range counts {
		if cnt > majorityCnt {
			majorityCnt = cnt
			majorityBal = bal
		}
	}

	if majorityCnt < required {
		return 0, fmt.Errorf("no majority value in cluster %d", clusterID)
	}
	return majorityBal, nil
}

func (c *client) readLinearizable(clusterID int, req *common.ClientReqConsLevel) (int, error) {
	var resp common.ClientReply
	for attempt := 1; attempt <= 3; attempt++ {
		leader := c.clusters[clusterID].leader
		if leader == 0 || !c.liveNodes[leader] {
			if _, err := c.broadcastToCluster(clusterID, "Node.ReadOnly", req, &resp); err != nil {
				return 0, err
			}
		} else {
			addr := c.nodes[leader].addr
			slog.Info("client rpc attempt", "node", leader, "method", "Node.ReadOnly")
			err := callRPC(addr, "Node.ReadOnly", req, &resp)
			if err != nil || isMsg(&resp, "node-stopped") {
				if err != nil {
					slog.Warn("client rpc failed", "node", leader, "error", err)
				}
				if _, derr := c.broadcastToCluster(clusterID, "Node.ReadOnly", req, &resp); derr != nil {
					if err != nil {
						return 0, err
					}
					return 0, derr
				}
			} else if isMsg(&resp, "not-leader") {
				if _, derr := c.broadcastToCluster(clusterID, "Node.ReadOnly", req, &resp); derr != nil {
					return 0, derr
				}
			}
		}

		if strings.Contains(strings.ToLower(resp.Message), "abort") {
			return 0, fmt.Errorf("read aborted: %s", resp.Message)
		}
		if strings.Contains(strings.ToLower(resp.Message), "skip") {
			slog.Info("client read retry on skip", "cluster", clusterID, "attempt", attempt)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if !resp.Status {
			return 0, fmt.Errorf("read rejected: %s", resp.Message)
		}
		return resp.Balance, nil
	}
	return 0, fmt.Errorf("read retries exhausted in cluster %d", clusterID)
}

func isMsg(resp *common.ClientReply, target string) bool {
	return strings.EqualFold(strings.TrimSpace(resp.Message), target)
}

func (c *client) broadcastToCluster(clusterID int, method string, req interface{}, dest *common.ClientReply) (int, error) {
	cl, ok := c.clusters[clusterID]
	if !ok {
		return 0, fmt.Errorf("unknown cluster %d", clusterID)
	}

	var lastErr error
	for _, id := range cl.nodes {
		if !c.liveNodes[id] {
			continue
		}
		tmp := &common.ClientReply{}
		addr := c.nodes[id].addr
		slog.Info("client rpc broadcast attempt", "node", id, "method", method)
		if err := callRPC(addr, method, req, tmp); err != nil {
			lastErr = err
			c.addViewEvent(viewEvent{
				clusterID: clusterID,
				detail:    fmt.Sprintf("rpc to n%d (%s) failed: %v", id, method, err),
				at:        time.Now(),
			})
			continue
		}
		if isMsg(tmp, "node-stopped") {
			lastErr = fmt.Errorf("node %d stopped", id)
			c.liveNodes[id] = false
			continue
		}
		if isMsg(tmp, "not-leader") {
			lastErr = fmt.Errorf("node %d not leader", id)
			continue
		}
		*dest = *tmp
		cl.leader = id
		return id, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no live nodes in cluster %d", clusterID)
	}
	return 0, lastErr
}

// NOTE: call cluster retries 3 times after waiting for 500 ms
func (c *client) callCluster(clusterID int, method string, req interface{}, resp interface{}) error {
	cl, ok := c.clusters[clusterID]
	if !ok {
		return fmt.Errorf("unknown cluster %d", clusterID)
	}
	out, ok := resp.(*common.ClientReply)
	if !ok {
		return fmt.Errorf("unexpected response type %T", resp)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		leader := cl.leader
		if leader == 0 || !c.liveNodes[leader] {
			if _, err := c.broadcastToCluster(clusterID, method, req, out); err != nil {
				return err
			}
		} else {
			addr := c.nodes[leader].addr
			slog.Info("client rpc attempt", "node", leader, "method", method)
			err := callRPC(addr, method, req, out)
			if err != nil || isMsg(out, "node-stopped") {
				lastErr := err
				if _, derr := c.broadcastToCluster(clusterID, method, req, out); derr != nil {
					if lastErr != nil {
						return lastErr
					}
					return derr
				}
			} else if isMsg(out, "not-leader") {
				if _, derr := c.broadcastToCluster(clusterID, method, req, out); derr != nil {
					return derr
				}
			}
		}

		msg := strings.ToLower(out.Message)
		if strings.Contains(msg, "abort") {
			return fmt.Errorf("server aborted: %s", out.Message)
		}
		if strings.Contains(msg, "skip") {
			slog.Info("client retry on skip", "cluster", clusterID, "attempt", attempt)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if !out.Status {
			return fmt.Errorf("server rejected: %s", out.Message)
		}
		return nil
	}
	return fmt.Errorf("rpc retries exhausted for cluster %d", clusterID)
}

func callRPC(addr, method string, req, resp interface{}) error {
	return callRPCWithTimeout(addr, method, req, resp, 3*time.Second)
}

func callRPCWithTimeout(addr, method string, req, resp interface{}, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	callCh := make(chan error, 1)
	go func() {
		callCh <- client.Call(method, req, resp)
	}()

	select {
	case err := <-callCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("rpc timeout calling %s on %s", method, addr)
	}
}

func (c *client) printBalance(acct int) error {
	clusterID := c.accountToCluster[acct]
	cl, ok := c.clusters[clusterID]
	if !ok {
		return fmt.Errorf("no cluster for account %d", acct)
	}

	results := make([]string, 0, len(cl.nodes))
	for _, nodeID := range cl.nodes {
		req := &common.ClientReqConsLevel{
			ClientReq: common.ClientReq{
				RequestType: common.RequestTypeRead,
				TranscationVal: common.TransactionVal{
					From: acct,
				},
			},
			// Use majority so backups reply; linearizable would reject non-leaders.
			ConsistencyLevel: common.Majority,
		}
		var resp common.ClientReply
		err := callRPC(c.nodes[nodeID].addr, "Node.ReadOnly", req, &resp)
		if err != nil {
			results = append(results, fmt.Sprintf("n%d: error (%v)", nodeID, err))
			continue
		}
		if !resp.Status {
			results = append(results, fmt.Sprintf("n%d: %s", nodeID, resp.Message))
			continue
		}
		results = append(results, fmt.Sprintf("n%d: %d", nodeID, resp.Balance))
	}
	fmt.Printf("Balances for account %d -> %s\n", acct, strings.Join(results, ", "))
	return nil
}

func (c *client) printStatus() error {
	ids := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	results := make([]string, 0, len(ids))
	for _, id := range ids {
		addr := c.nodes[id].addr
		if live, ok := c.liveNodes[id]; ok && !live {
			results = append(results, fmt.Sprintf("n%d: marked failed (per set)", id))
			continue
		}
		var resp common.NodeStatusResp
		err := callRPC(addr, "Node.PrintStatus", &common.NodeStatusReq{}, &resp)
		if err != nil {
			results = append(results, fmt.Sprintf("n%d: error (%v)", id, err))
			continue
		}
		if !resp.Status {
			results = append(results, fmt.Sprintf("n%d: %s", id, resp.Message))
			continue
		}
		role := "backup"
		if resp.Role == common.RoleLeader {
			role = "leader"
		}
		state := "stopped"
		if resp.NodeState == common.StatusActive {
			state = "active"
		}
		results = append(results, fmt.Sprintf("n%d(c%d): %s/%s", resp.NodeID, resp.ClusterID, state, role))
	}
	fmt.Printf("Node status -> %s\n", strings.Join(results, ", "))
	return nil
}

func (c *client) printLog(nodeID int) error {
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %d", nodeID)
	}
	var resp common.PrintLogResp
	if err := callRPC(node.addr, "Node.PrintLog", &common.PrintLogReq{}, &resp); err != nil {
		return err
	}
	if !resp.Status {
		return fmt.Errorf(resp.Message)
	}
	if len(resp.Log) == 0 {
		fmt.Printf("Log for node %d is empty\n", nodeID)
		return nil
	}
	lines := make([]string, 0, len(resp.Log))
	for _, entry := range resp.Log {
		lines = append(lines, fmt.Sprintf("[%d] status=%s twoPC=%s ballot=%d/%d txn=(%d->%d amt=%d)",
			entry.Index, entry.Status, entry.TwoPCStatus, entry.Ballot.SequenceNumber, entry.Ballot.NodeID, entry.TxnVal.From, entry.TxnVal.To, entry.TxnVal.Amount))
	}
	fmt.Printf("Log for node %d:\n%s\n", nodeID, strings.Join(lines, "\n"))
	return nil
}

func (c *client) printLogSlot(nodeID, slot int) error {
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %d", nodeID)
	}
	var resp common.PrintLogSlotResp
	req := &common.PrintLogSlotReq{Slot: slot}
	if err := callRPC(node.addr, "Node.PrintLogSlot", req, &resp); err != nil {
		return err
	}
	if !resp.Status {
		return fmt.Errorf(resp.Message)
	}
	e := resp.Entry
	fmt.Printf("Log slot %d on node %d: status=%s twoPC=%s ballot=%d/%d txn=(%d->%d amt=%d)\n",
		e.Index, nodeID, e.Status, e.TwoPCStatus, e.Ballot.SequenceNumber, e.Ballot.NodeID, e.TxnVal.From, e.TxnVal.To, e.TxnVal.Amount)
	return nil
}

func (c *client) printDB() error {
	if len(c.modifiedAccounts) == 0 {
		fmt.Println("no modified accounts in this set")
		return nil
	}

	accts := make([]int, 0, len(c.modifiedAccounts))
	for acct := range c.modifiedAccounts {
		accts = append(accts, acct)
	}
	sort.Ints(accts)

	nodeIDs := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Ints(nodeIDs)

	for _, nodeID := range nodeIDs {
		req := &common.GetAllAccountsReq{}
		var resp common.GetAllAccountsResp
		err := callRPC(c.nodes[nodeID].addr, "Node.GetAllBalances", req, &resp)
		if err != nil || !resp.Status {
			msg := "rpc error"
			if err == nil {
				msg = resp.Message
			}
			fmt.Printf("n%d: %s\n", nodeID, msg)
			continue
		}
		fmt.Printf("n%d:", nodeID)
		for _, acct := range accts {
			if bal, ok := resp.Balances[acct]; ok {
				fmt.Printf(" %d=%d", acct, bal)
			}
		}
		fmt.Println()
	}
	return nil
}

func (c *client) printPerformance() {
	c.mu.Lock()
	count := len(c.txnDurations)
	if count == 0 {
		c.mu.Unlock()
		fmt.Println("no transactions recorded in this set")
		return
	}
	total := time.Duration(0)
	for _, d := range c.txnDurations {
		total += d
	}
	avg := total / time.Duration(count)
	start := c.txnStart
	end := c.txnEnd
	c.mu.Unlock()

	wall := end.Sub(start)
	if start.IsZero() || end.IsZero() || !end.After(start) {
		wall = total
	}
	if wall <= 0 {
		wall = time.Nanosecond
	}
	throughput := float64(count) / wall.Seconds()
	fmt.Printf("Performance: %d txns, avg latency %s, wall time %s, throughput %.2f txns/sec\n", count, avg, wall, throughput)
}

func (c *client) failNode(nodeID int) error {
	addr := c.nodes[nodeID].addr
	var resp common.NodeControlResp
	return callRPC(addr, "Node.Fail", &common.NodeControlReq{}, &resp)
}

func (c *client) recoverNode(nodeID int) error {
	addr := c.nodes[nodeID].addr
	var resp common.NodeControlResp
	return callRPC(addr, "Node.Recover", &common.NodeControlReq{}, &resp)
}

func (c *client) recoverNodeWithRole(nodeID int, role common.NodeRole) error {
	addr := c.nodes[nodeID].addr
	req := &common.RecoverWithRoleReq{Role: role}
	var resp common.NodeControlResp
	return callRPC(addr, "Node.RecoverWithRole", req, &resp)
}

func (c *client) nodeRole(nodeID int) common.NodeRole {
	info, ok := c.nodes[nodeID]
	if !ok {
		return common.RoleBackup
	}
	if info.isLeader {
		return common.RoleLeader
	}
	return common.RoleBackup
}

func (c *client) printView() {
	if len(c.viewEvents) == 0 {
		fmt.Println("no view-change events recorded for this set")
		return
	}
	for _, ev := range c.viewEvents {
		fmt.Printf("[%s] cluster %d: %s\n", ev.at.Format(time.RFC3339), ev.clusterID, ev.detail)
	}
}

func (c *client) runReshard() error {
	if len(c.txnHistory) == 0 {
		fmt.Println("no transaction history to guide resharding")
		return nil
	}

	interactions := make(map[int]map[int]int)
	for _, tx := range c.txnHistory {
		if interactions[tx.from] == nil {
			interactions[tx.from] = make(map[int]int)
		}
		if interactions[tx.to] == nil {
			interactions[tx.to] = make(map[int]int)
		}
		interactions[tx.from][tx.to]++
		interactions[tx.to][tx.from]++
	}

	newMapping := cloneMapping(c.accountToCluster)
	clusterCounts := make(map[int]int)
	for _, cl := range c.accountToCluster {
		clusterCounts[cl]++
	}

	for acct, peers := range interactions {
		currentCluster := newMapping[acct]
		bestCluster := currentCluster
		bestScore := 0
		for clusterID := range c.clusters {
			score := 0
			for other, weight := range peers {
				if newMapping[other] == clusterID {
					score += weight
				}
			}
			if score > bestScore {
				bestScore = score
				bestCluster = clusterID
			}
		}
		if bestCluster != currentCluster {
			clusterCounts[currentCluster]--
			clusterCounts[bestCluster]++
			newMapping[acct] = bestCluster
		}
	}

	moves := make([][3]int, 0)
	for acct, newCl := range newMapping {
		oldCl := c.accountToCluster[acct]
		if oldCl != newCl {
			moves = append(moves, [3]int{acct, oldCl, newCl})
		}
	}
	if len(moves) == 0 {
		fmt.Println("reshard: no moves suggested")
		return nil
	}

	c.accountToCluster = newMapping

	if err := c.writeShardsToConfig(newMapping); err != nil {
		return fmt.Errorf("update config shards: %w", err)
	}

	sort.Slice(moves, func(i, j int) bool { return moves[i][0] < moves[j][0] })
	parts := make([]string, 0, len(moves))
	for _, mv := range moves {
		parts = append(parts, fmt.Sprintf("(%d, c%d, c%d)", mv[0], mv[1], mv[2]))
	}
	fmt.Printf("Reshard moves: %s\n", strings.Join(parts, ", "))
	fmt.Printf("Updated shard layout written to %s\n", c.configPath)
	return nil
}

func (c *client) writeShardsToConfig(mapping map[int]int) error {
	cfg, err := loadConfig(c.configPath)
	if err != nil {
		return err
	}

	clusterAccounts := make(map[int][]int)
	for acct, cluster := range mapping {
		clusterAccounts[cluster] = append(clusterAccounts[cluster], acct)
	}

	newShards := make(map[string][]int, len(clusterAccounts))
	for clusterID, accounts := range clusterAccounts {
		sort.Ints(accounts)
		newShards[strconv.Itoa(clusterID)] = accounts
	}
	cfg.Shards = newShards

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.configPath, data, 0o644)
}
