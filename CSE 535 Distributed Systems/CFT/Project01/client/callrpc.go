package main

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	common "pranavpateriya.com/multipaxos/common"
	"pranavpateriya.com/multipaxos/config"
)

const clientID = "cli"

type transactionRPCReq struct {
	TransactionReq common.TransactionReq
}

type transactionRPCReply struct {
	RequestResult bool
	Message       string
	Ballot        ballot
	LeaderID      int
}

type ballot struct {
	N      int
	NodeID int
}

// SubmitTransaction dials addr and invokes TransactionRPC.Submit with the provided transaction.
func SubmitTransaction(ctx context.Context, addr string, tx common.Transaction) (*common.SubmitTxReply, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	now := time.Now()
	req := &transactionRPCReq{
		TransactionReq: common.TransactionReq{
			Transaction: tx,
			TransactionID: common.TransactionID{
				Timestamp:     now,
				ClientID:      clientID,
				TransactionId: fmt.Sprintf("%s-%d", clientID, now.UnixNano()),
			},
		},
	}

	reply := new(transactionRPCReply)
	call := client.Go("TransactionRPC.Submit", req, reply, nil)

	select {
	case done := <-call.Done:
		if done.Error != nil {
			return nil, fmt.Errorf("submit tx to %s: %w", addr, done.Error)
		}
	case <-ctx.Done():
		client.Close()
		select {
		case <-call.Done:
		default:
		}
		return nil, ctx.Err()
	}

	return &common.SubmitTxReply{
		Success:    reply.RequestResult,
		Message:    reply.Message,
		LeaderHint: resolveLeaderHint(reply.LeaderID),
	}, nil
}

// SubmitTransaction dials addr and invokes TransactionRPC.Submit with the provided transaction.
func SubmitTransactionRepeat(ctx context.Context, addr, tID string, tx common.TransactionReq) (*common.SubmitTxReply, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	now := time.Now()
	req := &transactionRPCReq{
		TransactionReq: common.TransactionReq{
			Transaction: tx.Transaction,
			TransactionID: common.TransactionID{
				Timestamp:     now,
				ClientID:      clientID,
				TransactionId: tID,
			},
		},
	}

	reply := new(transactionRPCReply)
	call := client.Go("TransactionRPC.Submit", req, reply, nil)

	select {
	case done := <-call.Done:
		if done.Error != nil {
			return nil, fmt.Errorf("submit tx to %s: %w", addr, done.Error)
		}
	case <-ctx.Done():
		client.Close()
		select {
		case <-call.Done:
		default:
		}
		return nil, ctx.Err()
	}

	return &common.SubmitTxReply{
		Success:    reply.RequestResult,
		Message:    reply.Message,
		LeaderHint: resolveLeaderHint(reply.LeaderID),
	}, nil
}
func resolveLeaderHint(nodeID int) string {
	if nodeID == 0 {
		return ""
	}
	key := fmt.Sprintf("n%d", nodeID)
	if addr, ok := config.ServerMap[key]; ok {
		return addr
	}
	if strings.Contains(key, ":") {
		return key
	}
	if _, err := strconv.Atoi(key); err == nil {
		return key
	}
	return ""
}

type failureArgs struct {
	Reason string
}

type failureReply struct {
	Stopped bool
	Message string
}

func SendLeaderFailure(ctx context.Context, addr, reason string) (*failureReply, error) {
	return sendFailure(ctx, addr, "ElectionRPC.LeaderFailure", reason)
}

func SendBackupFailure(ctx context.Context, addr, reason string) (*failureReply, error) {
	return sendFailure(ctx, addr, "ElectionRPC.BackupFailure", reason)
}

type resumeReply struct {
	Resumed bool
	Message string
}

func SendLeaderResume(ctx context.Context, addr, reason string) (*resumeReply, error) {
	return sendResume(ctx, addr, "ElectionRPC.LeaderResume", reason)
}

func SendBackupResume(ctx context.Context, addr, reason string) (*resumeReply, error) {
	return sendResume(ctx, addr, "ElectionRPC.BackupResume", reason)
}

func sendFailure(ctx context.Context, addr, method, reason string) (*failureReply, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	args := &failureArgs{Reason: reason}
	reply := new(failureReply)
	call := client.Go(method, args, reply, nil)

	select {
	case done := <-call.Done:
		if done.Error != nil {
			return nil, fmt.Errorf("%s call to %s: %w", method, addr, done.Error)
		}
		return reply, nil
	case <-ctx.Done():
		client.Close()
		select {
		case <-call.Done:
		default:
		}
		return nil, ctx.Err()
	}
}

func sendResume(ctx context.Context, addr, method, reason string) (*resumeReply, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	args := &failureArgs{Reason: reason}
	reply := new(resumeReply)
	call := client.Go(method, args, reply, nil)

	select {
	case done := <-call.Done:
		if done.Error != nil {
			return nil, fmt.Errorf("%s call to %s: %w", method, addr, done.Error)
		}
		return reply, nil
	case <-ctx.Done():
		client.Close()
		select {
		case <-call.Done:
		default:
		}
		return nil, ctx.Err()
	}
}

func callObserver(ctx context.Context, addr, method string, args, reply interface{}) error {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	call := client.Go(method, args, reply, nil)

	select {
	case done := <-call.Done:
		if done.Error != nil {
			return fmt.Errorf("%s call to %s: %w", method, addr, done.Error)
		}
		return nil
	case <-ctx.Done():
		client.Close()
		select {
		case <-call.Done:
		default:
		}
		return ctx.Err()
	}
}

func FetchLog(ctx context.Context, addr string) (*common.PrintLogReply, error) {
	reply := new(common.PrintLogReply)
	if err := callObserver(ctx, addr, "ObserverRPC.PrintLog", &common.PrintLogArgs{}, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func FetchDB(ctx context.Context, addr string) (*common.PrintDBReply, error) {
	reply := new(common.PrintDBReply)
	if err := callObserver(ctx, addr, "ObserverRPC.PrintDB", &common.PrintDBArgs{}, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func FetchStatus(ctx context.Context, addr string, sequence int) (*common.PrintStatusReply, error) {
	reply := new(common.PrintStatusReply)
	args := &common.PrintStatusArgs{Sequence: sequence}
	if err := callObserver(ctx, addr, "ObserverRPC.PrintStatus", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func FetchViews(ctx context.Context, addr string) (*common.PrintViewReply, error) {
	reply := new(common.PrintViewReply)
	if err := callObserver(ctx, addr, "ObserverRPC.PrintView", &common.PrintViewArgs{}, reply); err != nil {
		return nil, err
	}
	return reply, nil
}
