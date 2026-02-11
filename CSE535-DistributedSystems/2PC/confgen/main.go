package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type NodeConfig struct {
	Address string   `json:"address"`
	Cluster int      `json:"cluster"`
	Role    int      `json:"role"`
	Peers   []string `json:"peers"`
}

type ShardRange struct {
	Start int `json:"0"`
	End   int `json:"1"`
}

type Config struct {
	NumCluster     int                    `json:"num_cluster"`
	FVal           int                    `json:"f_val"`
	InitialBalance int                    `json:"initial_balance"`
	Nodes          map[string]NodeConfig  `json:"nodes"`
	Shards         map[string][]int       `json:"shards"`
}

func main() {
	var numCluster, fVal int

	fmt.Print("Enter number of clusters (num_cluster): ")
	_, err := fmt.Scan(&numCluster)
	if err != nil {
		fmt.Println("Error reading num_cluster:", err)
		return
	}

	fmt.Print("Enter f value (f_val): ")
	_, err = fmt.Scan(&fVal)
	if err != nil {
		fmt.Println("Error reading f_val:", err)
		return
	}

	// Validate input
	if numCluster <= 0 {
		fmt.Println("num_cluster must be positive")
		return
	}
	if fVal < 0 {
		fmt.Println("f_val must be non-negative")
		return
	}

	config := generateConfig(numCluster, fVal)

	// Marshal to JSON with indentation
	jsonData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	// Write to file
	err = os.WriteFile("config.json", jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

	fmt.Println("\nConfiguration file 'config.json' generated successfully!")
	fmt.Printf("Clusters: %d\n", numCluster)
	fmt.Printf("Nodes per cluster: %d (2*f_val + 1 = 2*%d + 1)\n", 2*fVal+1, fVal)
	fmt.Printf("Total nodes: %d\n", numCluster*(2*fVal+1))
	fmt.Printf("Accounts per shard: %d\n", 9000/numCluster)
}

func generateConfig(numCluster, fVal int) Config {
	config := Config{
		NumCluster:     numCluster,
		FVal:           fVal,
		InitialBalance: 10,
		Nodes:          make(map[string]NodeConfig),
		Shards:         make(map[string][]int),
	}

	nodesPerCluster := 2*fVal + 1
	accountsPerShard := 9000 / numCluster
	nodeID := 1
	basePort := 8001

	// Generate nodes for each cluster
	for clusterID := 1; clusterID <= numCluster; clusterID++ {
		// Collect peer addresses for this cluster
		peers := make([]string, nodesPerCluster)
		for i := 0; i < nodesPerCluster; i++ {
			port := basePort + (nodeID - 1) + i
			peers[i] = fmt.Sprintf("127.0.0.1:%d", port)
		}

		// Create nodes for this cluster
		for i := 0; i < nodesPerCluster; i++ {
			port := basePort + (nodeID - 1)
			role := 0
			if i == 0 {
				role = 1 // First node in cluster is leader
			}

			node := NodeConfig{
				Address: fmt.Sprintf("127.0.0.1:%d", port),
				Cluster: clusterID,
				Role:    role,
				Peers:   peers,
			}

			config.Nodes[fmt.Sprintf("%d", nodeID)] = node
			nodeID++
		}

		// Generate shard ranges
		startAccount := (clusterID-1)*accountsPerShard + 1
		accounts := make([]int, accountsPerShard)
		for i := 0; i < accountsPerShard; i++ {
			accounts[i] = startAccount + i
		}
		config.Shards[fmt.Sprintf("%d", clusterID)] = accounts
	}

	return config
}
