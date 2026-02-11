package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"pranavpateriya.com/bft/server/persistence/bank"
	server "pranavpateriya.com/bft/server/rpc"
)

// Config holds all node configuration
type Config struct {
	NodeID           int
	Port             int
	Address          string
	Peers            []string
	ClientEndpoint   string
	Timeout          time.Duration
	PrivateKey       ed25519.PrivateKey
	NodePublicKeys   map[int]ed25519.PublicKey
	ClientPublicKeys map[string]ed25519.PublicKey
}

func main() {
	config, err := loadConfiguration()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}
	logCloser, err := setupNodeLogger(config.NodeID)
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logCloser.Close()

	node := createNode(config)
	if node == nil {
		log.Fatalf("node initialization failed")
	}

	if err := startRPCServer(node, config.Address); err != nil {
		log.Fatalf("start RPC server: %v", err)
	}
}

// loadConfiguration parses flags and loads all required configuration
func loadConfiguration() (*Config, error) {
	config, err := parseFlags()
	if err != nil {
		return nil, fmt.Errorf("parse flags: %w", err)
	}

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	if err := loadKeys(config); err != nil {
		return nil, fmt.Errorf("load keys: %w", err)
	}

	return config, nil
}

// parseFlags parses command-line arguments
func parseFlags() (*Config, error) {
	rawArgs := preprocessArgs(os.Args[1:])

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	nodeID := fs.Int("id", 0, "node identifier (required)")
	port := fs.Int("port", 0, "RPC listen port (required)")
	liveness := fs.Int("liveness", 0, "liveness timeout in milliseconds")
	peerStr := fs.String("peers", "", "comma separated peer addresses (required)")
	clientAddr := fs.String("client", "", "client address (required)")

	if err := fs.Parse(rawArgs); err != nil {
		return nil, err
	}

	timeout := time.Duration(*liveness) * time.Millisecond
	if timeout <= 0 {
		timeout = 800 * time.Millisecond
	}

	return &Config{
		NodeID:         *nodeID,
		Port:           *port,
		Address:        fmt.Sprintf(":%d", *port),
		Peers:          parsePeers(*peerStr),
		ClientEndpoint: strings.Trim(strings.TrimSpace(*clientAddr), ","),
		Timeout:        timeout,
	}, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if config.NodeID <= 0 {
		return fmt.Errorf("invalid node id %d", config.NodeID)
	}
	if config.Port <= 0 {
		return fmt.Errorf("invalid port %d", config.Port)
	}
	if len(config.Peers) == 0 {
		return fmt.Errorf("no valid peers provided")
	}
	if config.ClientEndpoint == "" {
		return fmt.Errorf("client address must be provided")
	}
	return nil
}

// loadKeys loads all cryptographic keys
func loadKeys(config *Config) error {
	privKeyPath := filepath.Join("keys", "nodes", fmt.Sprintf("node%d_private.pem", config.NodeID))
	privKey, err := loadEd25519PrivateKey(privKeyPath)
	if err != nil {
		return fmt.Errorf("load private key: %w", err)
	}
	config.PrivateKey = privKey

	nodePublicKeys, err := loadNodePublicKeys(filepath.Join("keys", "nodes"))
	if err != nil {
		return fmt.Errorf("load node public keys: %w", err)
	}
	config.NodePublicKeys = nodePublicKeys

	clientPublicKeys, err := loadClientPublicKeys(filepath.Join("keys", "clients"))
	if err != nil {
		return fmt.Errorf("load client public keys: %w", err)
	}
	config.ClientPublicKeys = clientPublicKeys

	return nil
}

// createNode constructs a Node instance from configuration
func createNode(config *Config) *server.Node {
	bank, err := bank.NewBankStore(config.NodeID)
	if err != nil {
		slog.Error("bank store not initialized", "error", err)
		return nil
	}

	node := &server.Node{
		NodeID:               config.NodeID,
		PortNumber:           config.Port,
		Address:              config.Address,
		Peers:                config.Peers,
		ClientPort:           config.ClientEndpoint,
		NodePublicKeyMaps:    config.NodePublicKeys,
		ClientPublicKeyMap:   config.ClientPublicKeys,
		Status:               server.NodeActive,
		Role:                 server.RoleReplica,
		CurrentView:          1,
		ViewChangeInProgress: false,
		LastStableCheckpoint: 0,
		HigestSequenceNumber: -1, //start off with -1, leader increase sequence number when it gets a client request
		PrivateKey:           config.PrivateKey,
		Log: server.NodeLog{
			LogEntries:         make([]*server.LogEntry, 0),
			ClientRequestIndex: make(map[string]int),
			ExecutedIndex:      -1,
		},
		RPCTimeout:         config.Timeout,
		Bank:               bank,
		CheckpointInterval: server.DefaultHighWatermark,
		ViewChangeTimeout:  5 * time.Second,
	}
	node.InitLivenessState()
	return node
}

// startRPCServer initializes and starts the RPC server
func startRPCServer(node *server.Node, address string) error {
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Node", node); err != nil {
		return fmt.Errorf("register RPCs: %w", err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer listener.Close()

	slog.Info("node started",
		"id", node.NodeID,
		"addr", listener.Addr().String(),
		"client", node.ClientPort,
	)

	return serveRPCConnections(rpcServer, listener)
}

// serveRPCConnections accepts and handles incoming RPC connections
func serveRPCConnections(rpcServer *rpc.Server, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("accept connection failed", "error", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}

// preprocessArgs normalizes command-line arguments
func preprocessArgs(args []string) []string {
	processed := make([]string, 0, len(args))
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--client:"):
			processed = append(processed, "-client="+strings.TrimPrefix(arg, "--client:"))
		case strings.HasPrefix(arg, "--client="):
			processed = append(processed, "-client="+strings.TrimPrefix(arg, "--client="))
		default:
			processed = append(processed, arg)
		}
	}
	return processed
}

// parsePeers splits and cleans a comma-separated peer list
func parsePeers(peers string) []string {
	if peers == "" {
		return nil
	}
	raw := strings.Split(peers, ",")
	result := make([]string, 0, len(raw))
	for _, entry := range raw {
		if trimmed := strings.TrimSpace(entry); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// loadNodePublicKeys loads all node public keys from directory
func loadNodePublicKeys(dir string) (map[int]ed25519.PublicKey, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	keys := make(map[int]ed25519.PublicKey)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "node") || !strings.HasSuffix(name, "_public.pem") {
			continue
		}

		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "node"), "_public.pem")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("parse node id from %s: %w", name, err)
		}
		pub, err := loadEd25519PublicKey(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("load public key for node %d: %w", id, err)
		}
		keys[id] = pub
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("no node public keys found in %s", dir)
	}
	return keys, nil
}

// loadClientPublicKeys loads all client public keys from directory
func loadClientPublicKeys(dir string) (map[string]ed25519.PublicKey, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	keys := make(map[string]ed25519.PublicKey)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, "_public.pem") {
			continue
		}

		pub, err := loadEd25519PublicKey(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("load client key from %s: %w", name, err)
		}
		clientID := strings.TrimSuffix(name, "_public.pem")
		canonicalID := canonicalClientID(clientID)
		if canonicalID == "" {
			return nil, fmt.Errorf("invalid client identifier derived from %s", name)
		}
		keys[canonicalID] = pub
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("no client public keys found in %s", dir)
	}
	return keys, nil
}

func setupNodeLogger(nodeID int) (io.Closer, error) {
	logDir := filepath.Join("server", "persistence", "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("ensure log dir: %w", err)
	}

	logPath := filepath.Join(logDir, fmt.Sprintf("node%d.log", nodeID))
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	handler := slog.NewTextHandler(io.MultiWriter(os.Stdout, file), &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			// Remove timestamp
			// if attr.Key == slog.TimeKey {
			// 	return slog.Attr{}
			// }
			return attr
		},
	})

	// Add node ID to all logs
	logger := slog.New(handler).With("node", nodeID)
	slog.SetDefault(logger)

	slog.Info("logger initialized", "path", logPath)
	return file, nil
}

func canonicalClientID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return ""
	}
	upper := strings.ToUpper(trimmed)
	return strings.TrimPrefix(upper, "CLIENT")
}

// loadEd25519PrivateKey loads a private key from PEM file
func loadEd25519PrivateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM data in %s", path)
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("unexpected PEM type %s in %s", block.Type, path)
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s does not contain an Ed25519 private key", path)
	}
	return priv, nil
}

// loadEd25519PublicKey loads a public key from PEM file
func loadEd25519PublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM data in %s", path)
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("unexpected PEM type %s in %s", block.Type, path)
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%s does not contain an Ed25519 public key", path)
	}
	return pub, nil
}
