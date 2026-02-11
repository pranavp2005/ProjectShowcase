package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

type nodeConfig struct {
	Nodes map[string]nodeConfigEntry `json:"nodes"`
	// shard id -> account ids that belong to that shard.
	Shards map[int][]int `json:"shards"`

	NumCluster     int `json:"num_cluster"`
	FVal           int `json:"f_val"`
	InitialBalance int `json:"initial_balance"`
}

type nodeConfigEntry struct {
	Cluster int `json:"cluster"`
}

type options struct {
	totalTxns     int
	rwPercent     float64
	crossPercent  float64
	skewness      float64
	seed          int64
	configPath    string
	outputPath    string
	minAmount     int
	maxAmount     int
	setNumber     string
}

type accountPicker struct {
	accounts []int
	rng      *rand.Rand
	zipf     *rand.Zipf
	skewed   bool
}

func main() {
	opts := parseFlags()

	cfg, err := loadConfig(opts.configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := validateOpts(opts); err != nil {
		fmt.Fprintf(os.Stderr, "invalid options: %v\n", err)
		os.Exit(1)
	}

	liveNodes := buildLiveNodes(cfg.Nodes)
	clusterAccounts := buildClusterAccounts(cfg.Shards)

	rng := rand.New(rand.NewSource(opts.seed))

	txns, err := generateTransactions(rng, opts, clusterAccounts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate transactions: %v\n", err)
		os.Exit(1)
	}

	if err := writeOutput(opts.outputPath, txns, liveNodes, opts.setNumber); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write output: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "generated %d transactions (rw=%d, ro=%d, cross=%d, intra=%d)\n",
		len(txns),
		countRW(txns),
		countRO(txns),
		countCross(txns, clusterAccounts),
		countIntra(txns, clusterAccounts),
	)
}

func parseFlags() options {
	var opts options
	flag.IntVar(&opts.totalTxns, "total", 200, "total number of transactions to generate")
	flag.Float64Var(&opts.rwPercent, "rwPercent", 80, "percentage of transactions that are read-write (0-100)")
	flag.Float64Var(&opts.crossPercent, "crossPercent", 10, "percentage of read-write txns that are cross-shard (0-100)")
	flag.Float64Var(&opts.skewness, "skew", 0, "skewness factor 0-1 (0 behaves as uniform; higher means hotter keys)")
	flag.Int64Var(&opts.seed, "seed", time.Now().UnixNano(), "random seed for reproducibility")
	flag.StringVar(&opts.configPath, "config", "config/node_config.json", "path to node config")
	flag.StringVar(&opts.outputPath, "out", "", "output file path (default stdout)")
	flag.IntVar(&opts.minAmount, "minAmount", 1, "minimum transfer amount for read-write transactions")
	flag.IntVar(&opts.maxAmount, "maxAmount", 10, "maximum transfer amount for read-write transactions")
	flag.StringVar(&opts.setNumber, "set", "1", "set number to use in the CSV header")
	flag.Parse()
	return opts
}

func validateOpts(opts options) error {
	if opts.totalTxns <= 0 {
		return errors.New("total transactions must be positive")
	}
	if opts.rwPercent < 0 || opts.rwPercent > 100 {
		return errors.New("rwPercent must be between 0 and 100")
	}
	if opts.crossPercent < 0 || opts.crossPercent > 100 {
		return errors.New("crossPercent must be between 0 and 100")
	}
	if opts.skewness < 0 || opts.skewness > 1 {
		return errors.New("skew must be between 0 and 1")
	}
	if opts.minAmount <= 0 || opts.maxAmount < opts.minAmount {
		return errors.New("amount bounds must be positive and maxAmount >= minAmount")
	}
	return nil
}

func loadConfig(path string) (nodeConfig, error) {
	var cfg nodeConfig
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func buildLiveNodes(nodes map[string]nodeConfigEntry) []string {
	ids := make([]int, 0, len(nodes))
	for idStr := range nodes {
		var id int
		fmt.Sscanf(idStr, "%d", &id)
		ids = append(ids, id)
	}
	sort.Ints(ids)

	live := make([]string, 0, len(ids))
	for _, id := range ids {
		live = append(live, fmt.Sprintf("n%d", id))
	}
	return live
}

func buildClusterAccounts(shards map[int][]int) map[int][]int {
	clusterAccounts := make(map[int][]int, len(shards))
	for shardID, accounts := range shards {
		accCopy := append([]int(nil), accounts...)
		sort.Ints(accCopy)
		clusterAccounts[shardID] = accCopy
	}
	return clusterAccounts
}

func generateTransactions(rng *rand.Rand, opts options, clusterAccounts map[int][]int) ([]string, error) {
	cluster1 := clusterAccounts[1]
	cluster2 := clusterAccounts[2]
	allAccounts := flattenAccounts(clusterAccounts)

	if len(cluster1) == 0 {
		return nil, errors.New("cluster 1 has no accounts configured")
	}
	if len(cluster2) == 0 && opts.crossPercent > 0 {
		return nil, errors.New("cluster 2 has no accounts configured but crossPercent > 0")
	}

	generalPicker, err := newAccountPicker(allAccounts, opts.skewness, rng)
	if err != nil {
		return nil, err
	}
	cluster1Picker, err := newAccountPicker(cluster1, opts.skewness, rng)
	if err != nil {
		return nil, err
	}

	var cluster2Picker *accountPicker
	if len(cluster2) > 0 {
		cluster2Picker, err = newAccountPicker(cluster2, opts.skewness, rng)
		if err != nil {
			return nil, err
		}
	}

	numRW := int(math.Round(float64(opts.totalTxns) * opts.rwPercent / 100.0))
	if numRW > opts.totalTxns {
		numRW = opts.totalTxns
	}
	numCross := int(math.Round(float64(numRW) * opts.crossPercent / 100.0))
	if numCross > numRW {
		numCross = numRW
	}
	numIntra := numRW - numCross
	numRO := opts.totalTxns - numRW

	txns := make([]string, 0, opts.totalTxns)

	for i := 0; i < numIntra; i++ {
		from := cluster1Picker.pick()
		to := cluster1Picker.pickDifferent(from)
		amt := opts.minAmount + rng.Intn(opts.maxAmount-opts.minAmount+1)
		txns = append(txns, fmt.Sprintf("(%d, %d, %d)", from, to, amt))
	}

	for i := 0; i < numCross; i++ {
		from := cluster1Picker.pick()
		to := cluster2Picker.pick()
		amt := opts.minAmount + rng.Intn(opts.maxAmount-opts.minAmount+1)
		txns = append(txns, fmt.Sprintf("(%d, %d, %d)", from, to, amt))
	}

	for i := 0; i < numRO; i++ {
		account := generalPicker.pick()
		txns = append(txns, fmt.Sprintf("(%d,)", account))
	}

	return txns, nil
}

func newAccountPicker(accounts []int, skew float64, rng *rand.Rand) (*accountPicker, error) {
	if len(accounts) == 0 {
		return nil, errors.New("no accounts available for selection")
	}

	p := &accountPicker{
		accounts: append([]int(nil), accounts...),
		rng:      rng,
	}

	if skew > 0 {
		// Map skew 0-1 to Zipf's s parameter roughly in [1, 3] for increasing hotness.
		s := 1.0 + 2.0*skew
		// v=1 gives standard Zipf-like behavior.
		p.zipf = rand.NewZipf(rng, s, 1, uint64(len(p.accounts)-1))
		p.skewed = true
	}

	return p, nil
}

func (p *accountPicker) pick() int {
	if len(p.accounts) == 1 {
		return p.accounts[0]
	}

	if p.skewed && p.zipf != nil {
		idx := int(p.zipf.Uint64())
		if idx >= len(p.accounts) {
			idx = len(p.accounts) - 1
		}
		return p.accounts[idx]
	}

	return p.accounts[p.rng.Intn(len(p.accounts))]
}

func (p *accountPicker) pickDifferent(exclude int) int {
	if len(p.accounts) == 1 {
		return p.accounts[0]
	}

	for {
		val := p.pick()
		if val != exclude {
			return val
		}
	}
}

func flattenAccounts(clusterAccounts map[int][]int) []int {
	all := make([]int, 0)
	for _, accounts := range clusterAccounts {
		all = append(all, accounts...)
	}
	sort.Ints(all)
	return all
}

func writeOutput(path string, txns []string, liveNodes []string, setNumber string) error {
	var out *os.File
	if path == "" {
		out = os.Stdout
	} else {
		var err error
		out, err = os.Create(path)
		if err != nil {
			return err
		}
		defer out.Close()
	}

	writer := csv.NewWriter(out)
	defer writer.Flush()

	if err := writer.Write([]string{"Set Number", "Transactions", "Live Nodes"}); err != nil {
		return err
	}

	live := fmt.Sprintf("[%s]", strings.Join(liveNodes, ", "))
	for i, txn := range txns {
		row := []string{"", txn, ""}
		if i == 0 {
			row[0] = setNumber
			row[2] = live
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Error()
}

// Utility counts for quick sanity summaries printed to stderr.
func countRW(txns []string) int {
	total := 0
	for _, t := range txns {
		if strings.Count(t, ",") == 2 {
			total++
		}
	}
	return total
}

func countRO(txns []string) int {
	total := 0
	for _, t := range txns {
		if strings.Count(t, ",") == 1 {
			total++
		}
	}
	return total
}

func countCross(txns []string, clusters map[int][]int) int {
	clusterLookup := make(map[int]int)
	for cid, accounts := range clusters {
		for _, a := range accounts {
			clusterLookup[a] = cid
		}
	}

	total := 0
	for _, t := range txns {
		if strings.Count(t, ",") != 2 {
			continue
		}
		var from, to, amt int
		if _, err := fmt.Sscanf(t, "(%d, %d, %d)", &from, &to, &amt); err != nil {
			continue
		}
		if clusterLookup[from] != 0 && clusterLookup[to] != 0 && clusterLookup[from] != clusterLookup[to] {
			total++
		}
	}
	return total
}

func countIntra(txns []string, clusters map[int][]int) int {
	clusterLookup := make(map[int]int)
	for cid, accounts := range clusters {
		for _, a := range accounts {
			clusterLookup[a] = cid
		}
	}

	total := 0
	for _, t := range txns {
		if strings.Count(t, ",") != 2 {
			continue
		}
		var from, to, amt int
		if _, err := fmt.Sscanf(t, "(%d, %d, %d)", &from, &to, &amt); err != nil {
			continue
		}
		if clusterLookup[from] != 0 && clusterLookup[from] == clusterLookup[to] {
			total++
		}
	}
	return total
}
