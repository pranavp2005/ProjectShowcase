package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"pranavpateriya.com/distributed-sys/common"
	"pranavpateriya.com/distributed-sys/persistence"
	"pranavpateriya.com/distributed-sys/server/utils"
)

var GlobalConfig = struct {
	NumCluster     int
	FVal           int
	AccountIDs     map[int]struct{}
	InitialBalance int
}{
	NumCluster:     3,
	FVal:           1,
	InitialBalance: 10,
}

type nodeConfig struct {
	Nodes  map[string]nodeConfigEntry `json:"nodes"`
	Shards map[int][]int              `json:"shards"`

	NumCluster int `json:"num_cluster"`
	FVal       int `json:"f_val"`

	InitialBalance int `json:"initial_balance"`
}

type nodeConfigEntry struct {
	Address string   `json:"address"`
	Cluster int      `json:"cluster"`
	Role    *int     `json:"role,omitempty"` // optional: 1 for leader, 0 for backup
	Peers   []string `json:"peers,omitempty"`
}

// setupLogger initializes slog for the given nodeID.
// If logToFile is true, it logs to both stdout and persistence/log/node<id>.log.
// Otherwise it logs only to stdout.
func setupLogger(nodeID int) {
	logDir := "persistence/log"
	var writer io.Writer = os.Stdout

	//config log controls:

	level := slog.LevelError
	// level := slog.LevelWarn
	// level := slog.LevelDebug
	// level := slog.LevelInfo

	logToFile := false

	if logToFile {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			slog.Error("failed to create log directory", slog.String("dir", logDir), slog.Any("error", err))
		} else {
			logPath := fmt.Sprintf("%s/node%d.log", logDir, nodeID)
			logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err == nil {
				writer = io.MultiWriter(os.Stdout, logFile)
			} else {
				slog.Error("failed to open log file, falling back to stdout", slog.String("path", logPath), slog.Any("error", err))
			}
		}
	}

	handler := slog.NewTextHandler(writer, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler).With("node", nodeID))
}

// TODO: complete this
func newNode(nodeID int, clusterID int, nodeRole common.NodeRole, nodeAddr string, clusterPeers map[int][]string, heartbeatTimer, twoPCTimer time.Duration, leaders map[int]clusterLeader, shards map[int][]int) *Node {
	storePath := fmt.Sprintf("persistence/state/cluster%d/node%d.db", clusterID, nodeID)
	store, err := persistence.NewBoltStore(storePath, 0600)
	if err != nil {
		slog.Error("failed to initialize persistence", slog.String("path", storePath), slog.Any("error", err))
		os.Exit(1)
	}

	clusterAddressesCopy := make(map[int][]string, len(clusterPeers))
	for cid, addrs := range clusterPeers {
		clusterAddressesCopy[cid] = append([]string(nil), addrs...)
	}

	uniquePeers := make(map[string]struct{})
	for _, peers := range clusterAddressesCopy {
		for _, addr := range peers {
			if addr == nodeAddr {
				continue
			}
			uniquePeers[addr] = struct{}{}
		}
	}

	rpcClients := make(map[string]*rpc.Client, len(uniquePeers))
	for addr := range uniquePeers {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			slog.Warn("failed to dial peer during init", "peer", addr, "error", err)
			continue
		}
		rpcClients[addr] = client
	}

	initialBallot := Ballot{
		SequenceNumber: 0,
		NodeID:         0,
	}
	// If config marks this node as leader, start with a ballot higher than backups.
	if nodeRole == common.RoleLeader {
		initialBallot = Ballot{
			SequenceNumber: 1,
			NodeID:         nodeID,
		}
	}

	return &Node{
		//External config dependent
		NodeID:     nodeID,
		ClusterID:  clusterID,
		ConfigRole: nodeRole,

		NodeAddress: nodeAddr,
		Status:      common.StatusActive,

		PaxosModule: &PaxosModule{
			Role:               nodeRole, // by default every node is a backup
			HeartbeatTimer:     heartbeatTimer,
			ActiveBallotNumber: initialBallot,
			ReplicationLog: NodeLog{
				LogEntries:         make([]*LogEntry, 0),
				LastExecuted:       -1,
				ClientRequestIndex: make(map[string]int),
			},
			txnIndex: utils.NewTxnIndex(),
			Bank:     store,
		},

		TwoPCModule: &TwoPCModule{
			CoordinatorTimer: twoPCTimer,
			WalService:       store,
		},

		ClusterLeaders:   leaders,
		ClusterAddresses: clusterAddressesCopy,
		AccountIDCluster: buildAccountClusterMap(shards),
		RPCClients:       rpcClients,
		//Function based init
	}
}

func buildAccountClusterMap(shards map[int][]int) map[int]int {
	if len(shards) == 0 {
		slog.Error("no shards present")
		return nil
	}

	clusterMap := make(map[int]int)
	for clusterID, accounts := range shards {
		for _, acct := range accounts {
			clusterMap[acct] = clusterID
		}
	}
	return clusterMap
}

func main() {
	flags := parseFlags()
	nodeID := flags.id

	setupLogger(nodeID)

	configPath := flags.configPath

	conf, err := loadConfig(configPath)
	if err != nil {
		slog.Error("failed to load config", slog.String("path", configPath), slog.Any("error", err))
		os.Exit(1)
	}

	myConfig, ok := conf.Nodes[fmt.Sprintf("%d", nodeID)]
	if !ok {
		slog.Error("node id not found in config", slog.Int("id", nodeID), slog.String("path", configPath))
		os.Exit(1)
	}

	applyGlobalConfig(conf, myConfig.Cluster)

	role := common.RoleBackup
	if myConfig.Role != nil {
		role = common.NodeRole(*myConfig.Role)
	}

	addr := myConfig.Address

	clusterIDs := make(map[int]struct{})
	for _, node := range conf.Nodes {
		clusterIDs[node.Cluster] = struct{}{}
	}

	clusterAddresses := make(map[int][]string, len(clusterIDs))
	for cid := range clusterIDs {
		clusterAddresses[cid] = conf.clusterPeerAddresses(cid)
	}

	roleVal := int(role)
	override := nodeOverride{
		id:      nodeID,
		role:    &roleVal,
		address: addr,
	}

	leaders := conf.clusterLeaders(override)

	node := newNode(nodeID, myConfig.Cluster, role, addr, clusterAddresses, flags.paxosLiveness, flags.twoPCLiveness, leaders, conf.Shards)

	node.startLivenessLoops()
	startRPCs(node)
	select {} // keep process alive
}

type parsedFlags struct {
	id            int
	configPath    string
	paxosLiveness time.Duration
	twoPCLiveness time.Duration
}

func parseFlags() parsedFlags {
	id := flag.Int("id", 1, "Node ID")
	configPath := flag.String("config", "config/node_config.json", "Path to node config")

	paxosLiveness := flag.Int("paxos-liveness", 900, "timeout to trigger leader election in ms")
	twoPCLiveness := flag.Int("2pc-liveness", 900, "timeout to trigger Abort in 2PC in ms")

	flag.Parse()

	return parsedFlags{
		id:            *id,
		configPath:    *configPath,
		paxosLiveness: time.Duration(*paxosLiveness) * time.Millisecond,
		twoPCLiveness: time.Duration(*twoPCLiveness) * time.Millisecond,
	}
}

func startRPCs(n *Node) {
	if err := rpc.RegisterName("Node", n); err != nil {
		slog.Error("rpc registration failed", slog.Any("error", err))
		os.Exit(1)
	}

	l, err := net.Listen("tcp", n.NodeAddress)
	if err != nil {
		slog.Error("listen failed", slog.String("address", n.NodeAddress), slog.Any("error", err))
		os.Exit(1)
	}
	// Handle incoming RPC connections in a dedicated goroutine so the caller
	// returns immediately and the listener can keep serving new peers without
	// blocking on any single connection.
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				slog.Error("accept failed", slog.Any("error", err))
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

func loadConfig(path string) (nodeConfig, error) {
	var cfg nodeConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c nodeConfig) clusterPeerAddresses(clusterID int) []string {
	addressSet := make(map[string]struct{})
	for _, node := range c.Nodes {
		if node.Cluster != clusterID {
			continue
		}
		if len(node.Peers) > 0 {
			for _, peer := range node.Peers {
				addressSet[peer] = struct{}{}
			}
			continue
		}
		addressSet[node.Address] = struct{}{}
	}

	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}
	sort.Strings(addresses)
	return addresses
}

func applyGlobalConfig(cfg nodeConfig, clusterID int) {
	if cfg.NumCluster > 0 {
		GlobalConfig.NumCluster = cfg.NumCluster
	}
	if cfg.FVal > 0 {
		GlobalConfig.FVal = cfg.FVal
	}

	if cfg.InitialBalance > 0 {
		GlobalConfig.InitialBalance = cfg.InitialBalance
	}

	accounts := cfg.Shards[clusterID]
	if len(accounts) == 2 && accounts[0] <= accounts[1] && accounts[1]-accounts[0] > 1 {
		expanded := make([]int, accounts[1]-accounts[0]+1)
		for i := range expanded {
			expanded[i] = accounts[0] + i
		}
		accounts = expanded
	}

	accountSet := make(map[int]struct{}, len(accounts))
	for _, acct := range accounts {
		accountSet[acct] = struct{}{}
	}
	GlobalConfig.AccountIDs = accountSet
}

type nodeOverride struct {
	id      int
	role    *int
	address string
}

func (c nodeConfig) clusterLeaders(override nodeOverride) map[int]clusterLeader {
	type selection struct {
		leader   clusterLeader
		explicit bool
	}

	out := make(map[int]clusterLeader)
	choices := make(map[int]selection)

	for idStr, node := range c.Nodes {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		role := node.Role
		addr := node.Address
		if override.id == id {
			if override.role != nil {
				role = override.role
			}
			if override.address != "" {
				addr = override.address
			}
		}

		explicit := role != nil && common.NodeRole(*role) == common.RoleLeader
		candidate := clusterLeader{
			NodeID:  id,
			Address: addr,
		}

		current, ok := choices[node.Cluster]
		switch {
		case !ok:
			choices[node.Cluster] = selection{leader: candidate, explicit: explicit}
		case explicit && !current.explicit:
			choices[node.Cluster] = selection{leader: candidate, explicit: explicit}
		case explicit == current.explicit && candidate.NodeID < current.leader.NodeID:
			choices[node.Cluster] = selection{leader: candidate, explicit: explicit}
		}
	}

	for clusterID, sel := range choices {
		out[clusterID] = sel.leader
	}
	return out
}

// ============= Configurable system size =============
func NetworkSize() int {
	return 2*GlobalConfig.FVal + 1
}

func QuorumSize() int {
	return GlobalConfig.FVal + 1
}
