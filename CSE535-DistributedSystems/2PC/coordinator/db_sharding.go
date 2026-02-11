package main

import (
	"sort"
	"strconv"

	"pranavpateriya.com/distributed-sys/common"
)

// =========== TYPES ===========
var currShardMapping map[int][]int

// =========== FUNCTIONS ===========

// set the global shard mapping
func setCurrShardMapping(shard [][]int) {
	currShardMapping = make(map[int][]int, len(shard))
	for clusterID, accounts := range shard {
		currShardMapping[clusterID] = append([]int(nil), accounts...)
	}
}

// RangeSharding distributes account IDs from 1..end across numClusters shards.
// produces balanced shards, even if the number of accounts is not divisible bu numClusters
func RangeSharding(end int, numClusters int) [][]int {
	if numClusters <= 0 {
		return nil
	}

	shardMappings := make([][]int, numClusters)

	if end <= 0 {
		// return empty shards when there are no accounts
		for i := range shardMappings {
			shardMappings[i] = []int{}
		}
		return shardMappings
	}

	base := end / numClusters
	remainder := end % numClusters
	next := 1

	for i := range numClusters {
		size := base
		if i < remainder {
			size++
		}

		shard := make([]int, size)
		for j := 0; j < size; j++ {
			shard[j] = next
			next++
		}

		shardMappings[i] = shard
	}

	return shardMappings
}

// HyperGraphPartition takes an array of transactions as input
func HyperGraphPartition(pastTransactions []common.TransactionVal, numClusters int) ([][]int, map[int][]string) {
	newSharding := make([][]int, numClusters)

	return newSharding, make(map[int][]string)
}

// Reshard computes a new shard assignment based on recent transactions.
// Inputs:
//   - currentShards: clusterID -> []accountIDs
//   - txs: recent transactions (workload history)
//   - numClusters: total number of clusters
//
// Outputs:
//   - newShards: index = clusterID, value = []accountIDs for that cluster
//   - movement: accountID -> []string{"oldClusterID", "newClusterID"} (only for moved accounts)
//
// NOTE: This function is pure logic; it does not talk to any DB.
func Reshard(
	currShar map[int][]int,
	txs []common.TransactionVal,
	numClusters int,
	) (newShards [][]int, movement map[int][]string) {

	// -----------------------------
	// 1. Discover accounts and max ID
	// -----------------------------

	// Collect all accounts and find max account ID
	accountSet := make(map[int]struct{})
	maxAccountID := 0

	for _, accounts := range currShar {
		for _, acc := range accounts {
			accountSet[acc] = struct{}{}
			if acc > maxAccountID {
				maxAccountID = acc
			}
		}
	}

	// Flatten accountSet to a slice
	allAccounts := make([]int, 0, len(accountSet))
	for acc := range accountSet {
		allAccounts = append(allAccounts, acc)
	}

	// -----------------------------
	// 2. Build graph (adjacency + degree)
	// -----------------------------

	// adjacency[a][b] = weight of edge between a and b
	adjacency := make(map[int]map[int]int)
	// degree[a] = Σ_b adjacency[a][b]
	degree := make(map[int]int)

	for _, tx := range txs {
		u := tx.From
		v := tx.To

		// Ignore self-transfers for the graph (they don't affect cross-shard cost)
		if u == v {
			continue
		}

		// Ensure both endpoints exist in our account universe; if not, skip.
		if _, ok := accountSet[u]; !ok {
			continue
		}
		if _, ok := accountSet[v]; !ok {
			continue
		}

		if adjacency[u] == nil {
			adjacency[u] = make(map[int]int)
		}
		if adjacency[v] == nil {
			adjacency[v] = make(map[int]int)
		}

		// Increase weight by 1 (you could use Amount or other metrics if desired)
		adjacency[u][v]++
		adjacency[v][u]++

		degree[u]++
		degree[v]++
	}

	// -----------------------------
	// 3. Build initial clusterOf and clusterSizes
	// -----------------------------

	// clusterOf[accountID] = clusterID
	clusterOf := make([]int, maxAccountID+1)
	// Initialize with -1 to detect missing assignments
	for i := range clusterOf {
		clusterOf[i] = -1
	}

	// originalClusterOf will be used later to compute movement
	originalClusterOf := make([]int, maxAccountID+1)
	for i := range originalClusterOf {
		originalClusterOf[i] = -1
	}

	// clusterSizes[clusterID] = number of accounts
	clusterSizes := make([]int, numClusters)

	for clusterID, accounts := range currShar {
		for _, acc := range accounts {
			if acc >= 0 && acc < len(clusterOf) {
				clusterOf[acc] = clusterID
				originalClusterOf[acc] = clusterID
				clusterSizes[clusterID]++
			}
		}
	}

	// -----------------------------
	// 4. Balance constraints
	// -----------------------------

	totalAccounts := len(allAccounts)
	if totalAccounts == 0 {
		// No accounts, trivial case
		return make([][]int, numClusters), make(map[int][]string)
	}

	targetPerCluster := totalAccounts / numClusters
	// Slack: how much deviation from perfect balance we allow
	slack := 5
	minPerCluster := targetPerCluster - slack
	if minPerCluster < 0 {
		minPerCluster = 0
	}
	maxPerCluster := targetPerCluster + slack

	// -----------------------------
	// 5. Order accounts by hotness (degree)
	// -----------------------------

	sort.Slice(allAccounts, func(i, j int) bool {
		di := degree[allAccounts[i]]
		dj := degree[allAccounts[j]]
		// higher degree first
		if di != dj {
			return di > dj
		}
		// tie-breaker: smaller account ID first
		return allAccounts[i] < allAccounts[j]
	})

	// -----------------------------
	// 6. Helper to compute affinity
	// -----------------------------

	computeAffinity := func(acc int, clusterID int) int {
		neighbors := adjacency[acc]
		if neighbors == nil {
			return 0
		}
		sum := 0
		for nb, w := range neighbors {
			if nb >= 0 && nb < len(clusterOf) && clusterOf[nb] == clusterID {
				sum += w
			}
		}
		return sum
	}

	// -----------------------------
	// 7. Local improvement loop
	// -----------------------------

	maxPasses := 5
	for pass := 0; pass < maxPasses; pass++ {
		movedInThisPass := false

		for _, acc := range allAccounts {
			curC := clusterOf[acc]
			if curC < 0 {
				// Should not happen if currentShards covers all accounts
				continue
			}

			// If this account never appears in any tx, moving it doesn't change cost
			if degree[acc] == 0 {
				continue
			}

			curAffinity := computeAffinity(acc, curC)
			bestC := curC
			bestAffinity := curAffinity

			// Try all possible clusters to find best destination
			for c := 0; c < numClusters; c++ {
				if c == curC {
					continue
				}
				if clusterSizes[c] >= maxPerCluster {
					continue
				}

				aff := computeAffinity(acc, c)
				if aff > bestAffinity {
					bestAffinity = aff
					bestC = c
				}
			}

			if bestC != curC {
				gain := bestAffinity - curAffinity
				// Move only if gain is positive and we don't violate balance on the source cluster
				if gain > 0 && clusterSizes[curC]-1 >= minPerCluster {
					clusterOf[acc] = bestC
					clusterSizes[curC]--
					clusterSizes[bestC]++
					movedInThisPass = true
				}
			}
		}

		if !movedInThisPass {
			// No further local improvements
			break
		}
	}

	// -----------------------------
	// 8. Build newShards from clusterOf
	// -----------------------------

	newShards = make([][]int, numClusters)
	for _, acc := range allAccounts {
		c := clusterOf[acc]
		if c >= 0 && c < numClusters {
			newShards[c] = append(newShards[c], acc)
		}
	}

	// -----------------------------
	// 9. Build movement map (only for moved accounts)
	// -----------------------------

	movement = make(map[int][]string)

	for _, acc := range allAccounts {
		oldC := originalClusterOf[acc]
		newC := clusterOf[acc]
		if oldC == -1 {
			// Wasn't in the original mapping; you can either skip or handle separately
			continue
		}
		if oldC != newC {
			// Represent as []string{"oldCluster", "newCluster"}
			movement[acc] = []string{
				strconv.Itoa(oldC),
				strconv.Itoa(newC),
			}
		}
	}

	return newShards, movement
}
