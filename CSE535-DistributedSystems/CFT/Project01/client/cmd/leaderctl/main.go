package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

// Local copies of the RPC argument/response shapes. The server defines these
// in its main package, so we mirror the fields here to keep gob happy.
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

func main() {
	addr := flag.String("addr", "localhost:8002", "host:port of the ElectionRPC server")
	failureReason := flag.String("failure-reason", "integration-test", "reason string to send with LeaderFailure")
	resumeReason := flag.String("resume-reason", "integration-test", "reason string to send with LeaderResume")
	waitAfterFailure := flag.Duration("wait", 1200*time.Millisecond, "pause between failure and resume RPCs")
	flag.Parse()

	if err := exerciseLeaderRPCs(*addr, *failureReason, *resumeReason, *waitAfterFailure); err != nil {
		log.Fatalf("leader RPC test failed: %v", err)
	}

	// Call PrintRole RPC 3 times with 5s wait between.
	for i := 1; i <= 3; i++ {
		log.Printf("=== PrintRoles run %d ===", i)
		if err := printRoles(); err != nil {
			log.Printf("[WARN] printRoles failed on run %d: %v", i, err)
		}
		if i < 3 {
			time.Sleep(5 * time.Second)
		}
	}
}

func exerciseLeaderRPCs(addr, failReason, resumeReason string, pause time.Duration) error {
	// log.Printf("dialing %s for LeaderFailure", addr)
	// failReply := LeaderFailureReply{}
	// if err := callOnce(addr, "ElectionRPC.LeaderFailure", &LeaderFailureArgs{Reason: failReason}, &failReply); err != nil {
	// 	return fmt.Errorf("LeaderFailure RPC error: %w", err)
	// }
	// log.Printf("LeaderFailure response: stopped=%v msg=%q", failReply.Stopped, failReply.Message)

	// if pause > 0 {
	// 	time.Sleep(pause)
	// }

	// log.Printf("dialing %s for LeaderResume", addr)
	// resumeReply := LeaderResumeReply{}
	// if err := callOnce(addr, "ElectionRPC.LeaderResume", &LeaderResumeArgs{Reason: resumeReason}, &resumeReply); err != nil {
	// 	return fmt.Errorf("LeaderResume RPC error: %w", err)
	// }
	// log.Printf("LeaderResume response: resumed=%v msg=%q", resumeReply.Resumed, resumeReply.Message)
	return nil
}

func callOnce(addr, method string, args, reply interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}

func printRoles() error {
	// Each node listens on localhost:8001 ... localhost:8005
	for port := 8001; port <= 8005; port++ {
		addr := fmt.Sprintf("localhost:%d", port)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			log.Printf("[WARN] could not connect to %s: %v", addr, err)
			continue
		}
		client := rpc.NewClient(conn)

		// We assume PrintRole takes no args and returns a struct like:
		// type PrintRoleReply struct { Role string; NodeID int }
		var reply struct {
			Role   string
			NodeID int
			Msg    string
		}
		if err := client.Call("ElectionRPC.PrintRole", struct{}{}, &reply); err != nil {
			log.Printf("[WARN] PrintRole RPC failed for %s: %v", addr, err)
		} else {
			log.Printf("[INFO] Node %d at %s has role: %s (%s)", reply.NodeID, addr, reply.Role, reply.Msg)
		}

		client.Close()
		conn.Close()
	}

	return nil
}
