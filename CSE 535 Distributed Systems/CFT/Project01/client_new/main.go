package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RPC Types these are duplicated here moving them to a differnt package would be too much work better to duplicate
type Transaction struct {
	From   string
	To     string
	Amount int64
}

type TransactionID struct {
	Timestamp     time.Time
	ClientID      string
	TransactionId string
}

type TransactionReq struct {
	Transaction   Transaction
	TransactionID TransactionID
}

type TransactionRPCReq struct {
	TransactionReq TransactionReq
}

type Ballot struct {
	N      int
	NodeID int
}

type TransactionRPCReply struct {
	RequestResult bool
	Message       string
	Ballot        Ballot
	LeaderID      int
}

type LeaderFailureArgs struct {
	Reason string
}

type LeaderFailureReply struct {
	Stopped bool
	Message string
}

type LeaderResumeArgs struct {
	Reason string
}

type LeaderResumeReply struct {
	Resumed bool
	Message string
}

type PrintStatusArgs struct {
	Sequence int
}

type PrintStatusReply struct {
	Status  string
	Message string
}

type PrintViewArgs struct{}

type PrintViewReply struct {
	Views   []string
	Message string
}

// Client structures
type Node struct {
	ID      string
	Address string
	Active  bool
}

type TransactionEntry struct {
	From   string
	To     string
	Amount int64
	UserID string // For sequencing per-user transactions
	Key    string // Stable identifier for retries
}

type TransactionSet struct {
	SetNumber    int
	Transactions []TransactionEntry
	LiveNodes    []string
}

type Client struct {
	nodes            map[string]*Node
	currentLeader    string
	clientID         string
	txCounter        int
	requestTimeout   time.Duration
	maxRetries       int
	pendingTxs       []TransactionEntry
	mu               sync.Mutex
	transactionIDMap map[string]int // Track transaction IDs per user
	txIDCache        map[string]TransactionID
}

func NewClient() *Client {
	nodes := map[string]*Node{
		"n1": {ID: "n1", Address: "localhost:8001", Active: true},
		"n2": {ID: "n2", Address: "localhost:8002", Active: true},
		"n3": {ID: "n3", Address: "localhost:8003", Active: true},
		"n4": {ID: "n4", Address: "localhost:8004", Active: true},
		"n5": {ID: "n5", Address: "localhost:8005", Active: true},
	}

	return &Client{
		nodes:            nodes,
		currentLeader:    "n1", // Start with n1 as assumed leader
		clientID:         fmt.Sprintf("client-%d", time.Now().Unix()),
		txCounter:        0,
		requestTimeout:   2 * time.Second,
		maxRetries:       10,
		pendingTxs:       make([]TransactionEntry, 0),
		transactionIDMap: make(map[string]int),
		txIDCache:        make(map[string]TransactionID),
	}
}

func parseCSV(filename string) ([]TransactionSet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var sets []TransactionSet
	var currentSet *TransactionSet

	// Regex to parse transactions like (A, J, 3)
	txRegex := regexp.MustCompile(`\(([A-Z]),\s*([A-Z]),\s*(\d+)\)`)

	for i, record := range records {
		if i == 0 { // Skip header
			continue
		}

		setNumStr := strings.TrimSpace(record[0])
		txStr := strings.TrimSpace(record[1])
		liveNodesStr := strings.TrimSpace(record[2])

		// New set
		if setNumStr != "" {
			setNum, err := strconv.Atoi(setNumStr)
			if err != nil {
				return nil, fmt.Errorf("invalid set number: %s", setNumStr)
			}

			if currentSet != nil {
				sets = append(sets, *currentSet)
			}

			currentSet = &TransactionSet{
				SetNumber:    setNum,
				Transactions: make([]TransactionEntry, 0),
			}

			// Parse live nodes
			if liveNodesStr != "" {
				liveNodesStr = strings.Trim(liveNodesStr, "[]")
				nodes := strings.Split(liveNodesStr, ",")
				for _, node := range nodes {
					currentSet.LiveNodes = append(currentSet.LiveNodes, strings.TrimSpace(node))
				}
			}
		}

		// Parse transaction
		if txStr != "" && currentSet != nil {
			// Check for LF (Leader Failure)
			if strings.ToUpper(txStr) == "LF" {
				currentSet.Transactions = append(currentSet.Transactions, TransactionEntry{
					From: "LF",
				})
			} else {
				matches := txRegex.FindStringSubmatch(txStr)
				if len(matches) == 4 {
					amount, _ := strconv.ParseInt(matches[3], 10, 64)
					entry := TransactionEntry{
						From:   matches[1],
						To:     matches[2],
						Amount: amount,
						UserID: matches[1], // From account is the user
					}
					if currentSet != nil {
						entry.Key = fmt.Sprintf("set%d-tx%d", currentSet.SetNumber, len(currentSet.Transactions))
					}
					currentSet.Transactions = append(currentSet.Transactions, entry)
				}
			}
		}
	}

	if currentSet != nil {
		sets = append(sets, *currentSet)
	}

	return sets, nil
}

func (c *Client) updateNodeConfig(liveNodes []string) {
	liveSet := make(map[string]struct{}, len(liveNodes))
	for _, nodeID := range liveNodes {
		liveSet[strings.TrimSpace(nodeID)] = struct{}{}
	}

	var toResume, toStop []string

	c.mu.Lock()
	for nodeID, node := range c.nodes {
		_, shouldBeActive := liveSet[nodeID]
		switch {
		case shouldBeActive && !node.Active:
			toResume = append(toResume, nodeID)
		case !shouldBeActive && node.Active:
			toStop = append(toStop, nodeID)
		}
	}
	c.mu.Unlock()

	for _, nodeID := range toStop {
		if err := c.stopNode(nodeID); err != nil {
			log.Printf("Failed to stop %s: %v", nodeID, err)
		}
	}
	for _, nodeID := range toResume {
		if err := c.resumeNode(nodeID); err != nil {
			log.Printf("Failed to resume %s: %v", nodeID, err)
		}
	}
}

func (c *Client) stopNode(nodeID string) error {
	node := c.nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	client, err := rpc.Dial("tcp", node.Address)
	if err != nil {
		return err
	}
	defer client.Close()

	args := &LeaderFailureArgs{Reason: "Client requested stop"}
	reply := &LeaderFailureReply{}

	err = client.Call("ElectionRPC.BackupFailure", args, reply)
	if err != nil {
		return err
	}

	log.Printf("Node %s stopped: %s", nodeID, reply.Message)
	c.mu.Lock()
	node.Active = false
	c.mu.Unlock()
	return nil
}

func (c *Client) resumeNode(nodeID string) error {
	node := c.nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	client, err := rpc.Dial("tcp", node.Address)
	if err != nil {
		return err
	}
	defer client.Close()

	args := &LeaderResumeArgs{Reason: "Client requested resume"}
	reply := &LeaderResumeReply{}

	err = client.Call("ElectionRPC.BackupResume", args, reply)
	if err != nil {
		return err
	}

	log.Printf("Node %s resumed: %s", nodeID, reply.Message)
	c.mu.Lock()
	node.Active = true
	c.mu.Unlock()
	return nil
}

func (c *Client) triggerLeaderFailure() error {
	log.Println("Triggering leader failure...")

	// Broadcast to all active nodes
	for nodeID, node := range c.nodes {
		if !node.Active {
			continue
		}

		client, err := rpc.Dial("tcp", node.Address)
		if err != nil {
			continue
		}

		args := &LeaderFailureArgs{Reason: "Client triggered leader failure"}
		reply := &LeaderFailureReply{}

		err = client.Call("ElectionRPC.LeaderFailure", args, reply)
		client.Close()

		if err == nil && reply.Stopped {
			log.Printf("Leader failure triggered on %s", nodeID)
			c.mu.Lock()
			node.Active = false
			c.currentLeader = "" // Reset leader
			c.mu.Unlock()
			time.Sleep(2 * time.Second) // Wait for leader election
			return nil
		}
	}

	return fmt.Errorf("failed to trigger leader failure")
}

func (c *Client) printLog(nodeID string) error {
	logPath := fmt.Sprintf("./server/persistence/logs/log%s.log", nodeID[1:]) // Extract number from "n1" -> "1"

	data, err := os.ReadFile(logPath)
	if err != nil {
		return fmt.Errorf("failed to read log file for %s: %v", nodeID, err)
	}

	fmt.Printf("\n=== Log for Node %s ===\n", nodeID)
	fmt.Println(string(data))
	fmt.Println("=== End of Log ===\n")

	return nil
}

func (c *Client) printDB(nodeID string) error {
	dbPath := fmt.Sprintf("./server/persistence/statemachine/balances%s.csv", nodeID[1:]) // Extract number from "n1" -> "1"

	file, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database file for %s: %v", nodeID, err)
	}
	defer file.Close()

	fmt.Printf("\n=== Database for Node %s ===\n", nodeID)
	fmt.Println("Account | Balance")
	fmt.Println("--------|--------")

	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read database: %v", err)
		}

		if len(record) >= 2 {
			fmt.Printf("%-7s | %s\n", record[0], record[1])
		}
	}

	fmt.Println("=== End of Database ===\n")
	return nil
}

func (c *Client) printStatus(nodeID string, sequence int) error {
	node := c.nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	client, err := rpc.Dial("tcp", node.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", nodeID, err)
	}
	defer client.Close()

	args := &PrintStatusArgs{Sequence: sequence}
	reply := &PrintStatusReply{}

	err = client.Call("ObserverRPC.PrintStatus", args, reply)
	if err != nil {
		return fmt.Errorf("RPC call failed: %v", err)
	}

	fmt.Printf("Node %s - Sequence %d: %s", nodeID, sequence, reply.Status)
	if reply.Message != "" {
		fmt.Printf(" (%s)", reply.Message)
	}
	fmt.Println()

	return nil
}

func (c *Client) printStatusAll(sequence int) {
	fmt.Printf("\n=== Status for Sequence %d ===\n", sequence)
	fmt.Println("Node | Status")
	fmt.Println("-----|-------")

	for _, nodeID := range []string{"n1", "n2", "n3", "n4", "n5"} {
		node := c.nodes[nodeID]
		if !node.Active {
			fmt.Printf("%-4s | Inactive\n", nodeID)
			continue
		}

		err := c.printStatus(nodeID, sequence)
		if err != nil {
			fmt.Printf("%-4s | Error: %v\n", nodeID, err)
		}
	}

	fmt.Println("=== End of Status ===\n")
}

func (c *Client) printView(nodeID string) error {
	node := c.nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	client, err := rpc.Dial("tcp", node.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", nodeID, err)
	}
	defer client.Close()

	args := &PrintViewArgs{}
	reply := &PrintViewReply{}

	err = client.Call("ObserverRPC.PrintView", args, reply)
	if err != nil {
		return fmt.Errorf("RPC call failed: %v", err)
	}

	fmt.Printf("\n=== Views from Node %s ===\n", nodeID)
	if len(reply.Views) == 0 {
		fmt.Println("No view changes recorded")
	} else {
		for i, view := range reply.Views {
			fmt.Printf("View %d: %s\n", i+1, view)
		}
	}
	if reply.Message != "" {
		fmt.Printf("\nMessage: %s\n", reply.Message)
	}
	fmt.Println("=== End of Views ===\n")

	return nil
}

func (c *Client) printViewAll() {
	fmt.Println("\n=== All New-View Messages ===")

	// Try to get views from first active node
	for _, nodeID := range []string{"n1", "n2", "n3", "n4", "n5"} {
		node := c.nodes[nodeID]
		if node.Active {
			err := c.printView(nodeID)
			if err == nil {
				return
			}
		}
	}

	fmt.Println("Could not retrieve views from any node")
	fmt.Println("=== End of Views ===\n")
}

func (c *Client) submitTransaction(tx TransactionEntry) (bool, error) {
	c.mu.Lock()
	c.txCounter++
	var txID TransactionID
	if tx.Key != "" {
		if cached, ok := c.txIDCache[tx.Key]; ok {
			txID = cached
		} else {
			idx := c.transactionIDMap[tx.UserID]
			txID = TransactionID{
				Timestamp:     time.Now(),
				ClientID:      c.clientID,
				TransactionId: fmt.Sprintf("%s-%d", tx.UserID, idx),
			}
			c.transactionIDMap[tx.UserID] = idx + 1
			c.txIDCache[tx.Key] = txID
		}
	} else {
		idx := c.transactionIDMap[tx.UserID]
		txID = TransactionID{
			Timestamp:     time.Now(),
			ClientID:      c.clientID,
			TransactionId: fmt.Sprintf("%s-%d", tx.UserID, idx),
		}
		c.transactionIDMap[tx.UserID] = idx + 1
	}
	c.mu.Unlock()

	req := &TransactionRPCReq{
		TransactionReq: TransactionReq{
			Transaction: Transaction{
				From:   tx.From,
				To:     tx.To,
				Amount: tx.Amount,
			},
			TransactionID: txID,
		},
	}

	// Try current leader first
	if c.currentLeader != "" {
		success, err := c.sendToNode(c.currentLeader, req)
		if success {
			return true, nil
		}
		log.Printf("Failed to send to leader %s: %v", c.currentLeader, err)
	}

	// Broadcast to all active nodes
	log.Println("Broadcasting to all active nodes...")
	return c.broadcastTransaction(req)
}

func (c *Client) sendToNode(nodeID string, req *TransactionRPCReq) (bool, error) {
	node := c.nodes[nodeID]
	if node == nil || !node.Active {
		return false, fmt.Errorf("node %s not available", nodeID)
	}

	client, err := rpc.Dial("tcp", node.Address)
	if err != nil {
		return false, err
	}
	defer client.Close()

	reply := &TransactionRPCReply{}
	doneChan := make(chan error, 1)

	go func() {
		doneChan <- client.Call("TransactionRPC.Submit", req, reply)
	}()

	select {
	case err := <-doneChan:
		if err != nil {
			return false, err
		}
		if reply.RequestResult {
			return true, nil
		}
		return false, fmt.Errorf("transaction rejected: %s", reply.Message)
	case <-time.After(c.requestTimeout):
		return false, fmt.Errorf("timeout")
	}
}

func (c *Client) broadcastTransaction(req *TransactionRPCReq) (bool, error) {
	var wg sync.WaitGroup
	resultChan := make(chan string, len(c.nodes))

	for nodeID, node := range c.nodes {
		if !node.Active {
			continue
		}

		wg.Add(1)
		go func(nID string, n *Node) {
			defer wg.Done()

			success, err := c.sendToNode(nID, req)
			if success {
				resultChan <- nID
			} else {
				log.Printf("Node %s failed: %v", nID, err)
			}
		}(nodeID, node)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Wait for first successful response
	for leaderID := range resultChan {
		c.mu.Lock()
		c.currentLeader = leaderID
		c.mu.Unlock()
		log.Printf("Updated leader to %s", leaderID)
		return true, nil
	}

	return false, fmt.Errorf("all nodes failed")
}

func (c *Client) executeTransactionsSequentially(transactions []TransactionEntry) []TransactionEntry {
	failed := make([]TransactionEntry, 0)

	// Group transactions by user
	userTxs := make(map[string][]TransactionEntry)
	lfIndices := make([]int, 0) // Track LF positions

	for i, tx := range transactions {
		if tx.From == "LF" {
			lfIndices = append(lfIndices, i)
		} else {
			userTxs[tx.UserID] = append(userTxs[tx.UserID], tx)
		}
	}

	// If there are LF transactions, handle them sequentially
	if len(lfIndices) > 0 {
		// Find all transactions before first LF
		firstLFIdx := lfIndices[0]
		beforeLF := transactions[:firstLFIdx]

		// Execute transactions before LF
		failedBeforeLF := c.executeParallelByUser(beforeLF)

		// Execute LF
		err := c.triggerLeaderFailure()
		if err != nil {
			log.Printf("Leader failure failed: %v", err)
		}
		time.Sleep(3 * time.Second)

		// Execute remaining transactions
		afterLF := transactions[firstLFIdx+1:]
		failedAfterLF := c.executeParallelByUser(afterLF)

		failed = append(failed, failedBeforeLF...)
		failed = append(failed, failedAfterLF...)
	} else {
		// No LF, execute all in parallel by user
		failed = c.executeParallelByUser(transactions)
	}

	return failed
}

func (c *Client) executeParallelByUser(transactions []TransactionEntry) []TransactionEntry {
	// Group by user
	userTxs := make(map[string][]TransactionEntry)
	for _, tx := range transactions {
		if tx.From != "LF" {
			userTxs[tx.UserID] = append(userTxs[tx.UserID], tx)
		}
	}

	var wg sync.WaitGroup
	failedChan := make(chan TransactionEntry, len(transactions))

	// Execute each user's transactions sequentially in parallel goroutines
	for userID, txs := range userTxs {
		wg.Add(1)
		go func(uid string, userTransactions []TransactionEntry) {
			defer wg.Done()

			for i, tx := range userTransactions {
				success := false
				var err error

				for retry := 0; retry < c.maxRetries; retry++ {
					success, err = c.submitTransaction(tx)
					if success {
						log.Printf("[+] Transaction %s -> %s (%d) succeeded", tx.From, tx.To, tx.Amount)
						break
					}
					log.Printf("Retry %d/%d for %s -> %s: %v", retry+1, c.maxRetries, tx.From, tx.To, err)
					time.Sleep(500 * time.Millisecond)
				}

				if !success {
					log.Printf("[-] Transaction %s -> %s (%d) failed after %d retries", tx.From, tx.To, tx.Amount, c.maxRetries)
					failedChan <- tx
				}

				// Add 100ms delay between transactions for the same user
				if i < len(userTransactions)-1 {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(userID, txs)
	}

	wg.Wait()
	close(failedChan)

	failed := make([]TransactionEntry, 0)
	for tx := range failedChan {
		failed = append(failed, tx)
	}

	return failed
}

func (c *Client) processSet(set TransactionSet, previousFailed []TransactionEntry) []TransactionEntry {
	log.Printf("\n=== Processing Set %d ===", set.SetNumber)
	log.Printf("Live nodes: %v", set.LiveNodes)

	// Update node configuration
	c.updateNodeConfig(set.LiveNodes)
	time.Sleep(1 * time.Second) // Allow time for nodes to update

	// Combine previous failed transactions with current set
	allTransactions := append(previousFailed, set.Transactions...)

	if len(previousFailed) > 0 {
		log.Printf("Retrying %d failed transactions from previous set", len(previousFailed))
	}

	// Execute transactions
	failed := c.executeTransactionsSequentially(allTransactions)

	log.Printf("Set %d completed. Failed: %d", set.SetNumber, len(failed))
	return failed
}

func (c *Client) handleUserInput() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nEnter command (continue/PrintDb/PrintLogs/PrintStatus): ")
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run client.go <csv_file>")
	}

	filename := os.Args[1]

	sets, err := parseCSV(filename)
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}

	log.Printf("Loaded %d transaction sets", len(sets))

	client := NewClient()
	var previousFailed []TransactionEntry

	for _, set := range sets {
		previousFailed = client.processSet(set, previousFailed)

		// Wait for user input
		for {
			cmd := client.handleUserInput()
			cmdParts := strings.Fields(cmd)

			if len(cmdParts) == 0 {
				continue
			}

			switch strings.ToLower(cmdParts[0]) {
			case "continue":
				goto nextSet

			case "printdb":
				if len(cmdParts) < 2 {
					fmt.Println("Usage: PrintDb <nodeID>")
					fmt.Println("Example: PrintDb n1")
					continue
				}
				nodeID := cmdParts[1]
				if err := client.printDB(nodeID); err != nil {
					fmt.Printf("Error: %v\n", err)
				}

			case "printlog":
				if len(cmdParts) < 2 {
					fmt.Println("Usage: PrintLog <nodeID>")
					fmt.Println("Example: PrintLog n1")
					continue
				}
				nodeID := cmdParts[1]
				if err := client.printLog(nodeID); err != nil {
					fmt.Printf("Error: %v\n", err)
				}

			case "printstatus":
				if len(cmdParts) < 2 {
					fmt.Println("Usage: PrintStatus <sequence_number>")
					fmt.Println("Example: PrintStatus 5")
					continue
				}
				sequence, err := strconv.Atoi(cmdParts[1])
				if err != nil {
					fmt.Println("Invalid sequence number")
					continue
				}
				client.printStatusAll(sequence)

			case "printviewAll":
				client.printViewAll()

			case "printview":
				if len(cmdParts) < 2 {
					fmt.Println("Usage: PrintView <NodeID>")
					fmt.Println("Example: PrintView 1")
					continue
				}
				sequence, err := strconv.Atoi(cmdParts[1])
				if err != nil {
					fmt.Println("Invalid sequence number")
					continue
				}
				client.printStatusAll(sequence)

			default:
				fmt.Println("Unknown command. Available commands:")
				fmt.Println("  continue")
				fmt.Println("  PrintDb <nodeID>       - e.g., PrintDb n1")
				fmt.Println("  PrintLog <nodeID>      - e.g., PrintLog n2")
				fmt.Println("  PrintStatus <seq>      - e.g., PrintStatus 5")
				fmt.Println("  PrintViewAll              - shows all view changes")
				fmt.Println("  PrintView <nodeID>     - shows view changes")

			}
		}
	nextSet:
	}

	// Handle any remaining failed transactions
	if len(previousFailed) > 0 {
		log.Printf("\n=== Retrying %d failed transactions ===", len(previousFailed))
		finalFailed := client.executeTransactionsSequentially(previousFailed)
		if len(finalFailed) > 0 {
			log.Printf("⚠ %d transactions failed permanently", len(finalFailed))
		}
	}

	log.Println("\n=== All sets processed ===")
}
