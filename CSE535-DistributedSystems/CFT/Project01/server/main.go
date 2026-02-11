package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"

	"pranavpateriya.com/multipaxos/common"
	"pranavpateriya.com/multipaxos/server/persistence/statemachine"
)

func parseFlags() (int, string, []string, time.Duration) {
	// role := flag.String("role", "backup", "leader or backup")
	id := flag.Int("id", 1, "node id 1..5 (tie-breaker in ballots)")
	port := flag.Int("port", 8001, "listen port")
	peers := flag.String("peers", "localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005", "comma-separated peers (include self)")
	liveness := flag.Int("liveness", 900, "per-RPC dial timeout in ms")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)
	peerList := strings.Split(*peers, ",")
	return *id, addr, peerList, time.Duration(*liveness) * time.Millisecond
	// return *role, *id, addr, peerList, time.Duration(*timeoutMs) * time.Millisecond
}

func main() {
	id, addr, peers, livenessTimeout := parseFlags()

	node, err := newNode(id, addr, peers)
	if err != nil {
		slog.Error("failed to initialise node", slog.Any("error", err))
		os.Exit(1)
	}

	// Start RPC services on the same default rpc.Server
	startConsensusRPC(node)
	startElectionRPC(node)
	startTransactionRPC(node)
	startObserverRPC(node)
	slog.Info("node started", slog.Int("id", node.NodeID), slog.String("addr", node.Address), slog.Any("peers", node.Peers), slog.String("role", string(node.Role)))

	// Strategy 1 timers on *all* nodes
	// Tune these for your RTTs:
	const (
		backoffTP   = 300 * time.Millisecond
		heartbeatIV = 300 * time.Millisecond
	)
	// Start liveness timer only after first client request (see TransactionRPC.Submit)
	node.t = livenessTimeout
	node.tp = backoffTP
	node.hbInterval = heartbeatIV
	node.SystemStartup = true
	// Leaders: begin heartbeats immediately (keeps timers fresh during idle)
	if false && node.Role == common.RoleLeader {
		// Choose an initial ballot >= promised
		node.mu.Lock()
		if node.Promised.Less(&Ballot{N: 1, NodeID: node.NodeID}) {
			node.Promised = Ballot{N: 1, NodeID: node.NodeID}
		}
		node.SelfBallot = node.Promised
		node.mu.Unlock()

		go node.runHeartbeats()
	}

	// Keep serving
	select {}
}

func newNode(id int, addr string, peers []string) (*Node, error) {
	status := common.StatusActive
	role := common.RoleBackup
	bank, err := statemachine.NewBankStore(id)
	if err != nil {
		return nil, fmt.Errorf("init bank store: %w", err)
	}

	n := &Node{
		Status:         status,
		Role:           role,
		NodeID:         id,
		Address:        addr,
		PortNumber:     0, // derived in addr
		SequenceNumber: 0, //initially the sequence number is 0
		Peers:          peers,
		Promised:       GetInitBallot(),
		Log:            NodeLog{ExecutedIndex: -1, ClientRequestIndex: make(map[string]int)},
		SystemStartup:  true,
		Bank:           bank,
		RPCTimeout:     time.Duration(1000) * time.Millisecond,
		NewViewHistory: make([]NewViewRecord, 0),
	}
	return n, nil
}

func (currNode *Node) quorumSize() int { return 3 }

// TODO: check if we can use this in election RPCs as well
func dialOnce(addr string, timeout time.Duration) (*rpc.Client, error) {
	d := net.Dialer{Timeout: timeout}
	c, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(c), nil
}
