// filename: main.go
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"

	clientrpc "pranavpateriya.com/bft/client/rpc"
)

type options struct {
	csvPath          string
	listenAddr       string
	nodeHost         string
	dialTimeout      time.Duration
	broadcastTimeout time.Duration
	readTimeout      time.Duration
	faultTolerance   int
}

func (o options) controllerConfig() ControllerConfig {
	return ControllerConfig{
		NodeAddrs:        buildNodeAddresses(o.nodeHost),
		FaultTolerance:   o.faultTolerance,
		DialTimeout:      o.dialTimeout,
		BroadcastTimeout: o.broadcastTimeout,
		ReadTimeout:      o.readTimeout,
	}
}

func main() {
	opts := parseFlags()

	sets, err := parseTestCases(opts.csvPath)
	if err != nil {
		log.Fatalf("parse csv: %v", err)
	}
	if len(sets) == 0 {
		log.Fatalf("no test sets found in %s", opts.csvPath)
	}

	execSrv := clientrpc.NewExecResult()
	stopExec, err := startResultServer(opts.listenAddr, execSrv)
	if err != nil {
		log.Fatalf("start exec RPC: %v", err)
	}
	defer stopExec()

	controller, err := NewController(opts.controllerConfig(), execSrv)
	if err != nil {
		log.Fatalf("init controller: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Loaded %d test sets from %s. Replicas should dial client at %s.\n", len(sets), opts.csvPath, opts.listenAddr)
	printUsage()

	for _, set := range sets {
		if err := controller.PrepareSet(set); err != nil {
			log.Fatalf("prepare set %d: %v", set.Number, err)
		}

		if err := waitForSetStart(reader, controller, set); err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Exiting per user request.")
				return
			}
			log.Fatalf("aborted before set %d: %v", set.Number, err)
		}

		if err := controller.ExecuteSet(set); err != nil {
			log.Fatalf("execute set %d: %v", set.Number, err)
		}

		if err := controller.PostSetMenu(reader, set); err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Exiting per user request.")
				return
			}
			log.Fatalf("post-set %d: %v", set.Number, err)
		}
	}

	fmt.Println("All sets completed successfully.")
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.csvPath, "csv", "CSE535-F25-Project-2-Testcases.csv", "path to the test case CSV file")
	flag.StringVar(&opts.listenAddr, "listen", ":9000", "address replicas use to submit execution results")
	flag.StringVar(&opts.nodeHost, "host", "127.0.0.1", "host used to reach replica RPC endpoints (ports follow n=8000+n)")
	flag.DurationVar(&opts.dialTimeout, "dial-timeout", 2*time.Second, "maximum time to dial a replica RPC endpoint")
	flag.DurationVar(&opts.broadcastTimeout, "broadcast-timeout", defaultCommitTimeout, "timeout to wait for a SubmitNormal leader response before broadcasting")
	flag.DurationVar(&opts.readTimeout, "read-timeout", 3*time.Second, "timeout for fast read quorum")
	flag.IntVar(&opts.faultTolerance, "f", 2, "fault tolerance parameter f (system assumes 3f+1 replicas)")
	flag.Parse()
	return opts
}

func waitForSetStart(reader *bufio.Reader, controller *Controller, set TestSet) error {
	fmt.Printf("\n--- Set %d ---\n", set.Number)
	fmt.Printf("Transactions: %d | Live nodes: %v | Byzantine nodes: %v\n", len(set.Transactions), set.LiveNodes, set.ByzantineNodes)
	fmt.Println("Press Enter to start, type 'run' to proceed, or 'quit' to exit. Type 'help' for available commands.")
	fmt.Println("Inspection commands before execution: log <node|all>, db <node|all>, status <seq>, view <node|all>.")

	for {
		fmt.Printf("[set %d pre] > ", set.Number)
		line, err := readLine(reader)
		if err != nil {
			return err
		}
		cmdRaw := strings.TrimSpace(line)
		cmd := strings.ToLower(cmdRaw)
		switch cmd {
		case "", "run", "start":
			return nil
		case "quit", "exit":
			return io.EOF
		case "help":
			printUsage()
		default:
			if handled := handlePreSetCommand(cmdRaw, controller); handled {
				continue
			}
			fmt.Println("Unrecognized input. Type 'help' to see available commands.")
		}
	}
}

func handlePreSetCommand(cmd string, controller *Controller) bool {
	if controller == nil {
		return false
	}
	fields := strings.Fields(strings.ToLower(cmd))
	if len(fields) == 0 {
		return false
	}

	switch fields[0] {
	case "log", "db", "status", "view":
		if err := controller.handlePostCommand(cmd); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		return true
	}
	return false
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if errors.Is(err, io.EOF) && len(line) > 0 {
		return line, nil
	}
	return line, err
}

func startResultServer(addr string, exec *clientrpc.ExecResult) (func(), error) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("ExecResult", exec); err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				slog.Error("exec RPC accept failed", "error", err)
				continue
			}
			go srv.ServeConn(conn)
		}
	}()

	return func() {
		_ = ln.Close()
	}, nil
}

func buildNodeAddresses(host string) map[int]string {
	addrs := make(map[int]string)
	for id := 1; id <= 7; id++ {
		addrs[id] = fmt.Sprintf("%s:%d", host, 8000+id)
	}
	return addrs
}

func printUsage() {
	fmt.Println("Client commands:")
	fmt.Println("  <enter>/run/start  - begin executing the current test set")
	fmt.Println("  help               - print this command list")
	fmt.Println("  quit/exit          - terminate execution")
	fmt.Println("Post-set menu commands (log/db/status/view also available before starting a set):")
	fmt.Println("  log <node|all>     - print replica logs")
	fmt.Println("  db <node|all>      - print replica balances")
	fmt.Println("  status <seq>       - show per-node log status for a sequence number")
	fmt.Println("  view <node|all>    - display view-change history")
	fmt.Println("  continue/next      - move to the next set")
	fmt.Println("  quit/exit          - terminate execution")
}
