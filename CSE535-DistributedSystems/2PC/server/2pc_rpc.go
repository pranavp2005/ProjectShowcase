package main

import (
	"log/slog"

	"pranavpateriya.com/distributed-sys/common"
)

// ======== TYPES ==========
// <PREPARE , t, m>
type PrepareTpcReq struct {
	T      common.ClientReq
	Digest []byte
}

type PrepareTpcResp struct {
	Ack     bool //true means c
	Message string
}

// Prepare_2PC is called on the leader of the participant cluster
func (n *Node) Prepare_2PC(req *PrepareTpcReq, resp *PrepareTpcResp) error {
	if n.Status == common.StatusStopped {
		slog.Info("2PC participant prepare rejected: node stopped")
		resp.Ack = false
		resp.Message = "node-stopped"
		return nil
	}

	if n.PaxosModule.Role != common.RoleLeader {
		slog.Info("2PC participant prepare rejected: not leader")
		resp.Ack = false
		resp.Message = "not-leader"
		return nil
	}

	to := req.T.TranscationVal.To

	if !tryAddIfAbsent(to) {
		slog.Info("2PC participant prepare: account locked, aborting", "account", to)
		_, _ = n.runPaxosForTxn(-1, TwoPCAborted, req.T)
		resp.Ack = false
		resp.Message = "account locked"
		return nil
	}

	slog.Info("2PC participant prepare: locked account, starting paxos", "account", to)
	if _, err := n.runPaxosForTxn(-1, TwoPCPrepared, req.T); err != nil {
		slog.Warn("2PC participant prepare paxos failed", "error", err)
		_, _ = n.runPaxosForTxn(-1, TwoPCAborted, req.T)
		resp.Ack = false
		resp.Message = "prepare paxos failed"
		return nil
	}

	resp.Ack = true
	resp.Message = "prepared"
	return nil
}

type CommitTPCReq struct {
	T common.ClientReq
}

type CommitTPCResp struct {
	Ack     bool
	Message string
}

// Commit_2PC is invoked by the coordinator leader to tell participant to commit.
func (n *Node) Commit_2PC(req *CommitTPCReq, resp *CommitTPCResp) error {
	if n.Status != common.StatusActive || n.PaxosModule.Role != common.RoleLeader {
		slog.Info("2PC participant commit rejected", "reason", "not active leader")
		resp.Ack = false
		resp.Message = "not-leader"
		return nil
	}
	slog.Info("2PC participant commit consensus")
	if err := n.startConsensusForCommit(req.T); err != nil {
		slog.Warn("2PC participant commit consensus failed", "error", err)
		resp.Ack = false
		resp.Message = err.Error()
		return nil
	}
	resp.Ack = true
	resp.Message = "committed"
	return nil
}

// Abort_2PC is invoked by coordinator to abort participant.
type AbortTPCReq struct {
	T common.ClientReq
}

type AbortTPCResp struct {
	Ack     bool
	Message string
}

func (n *Node) Abort_2PC(req *AbortTPCReq, resp *AbortTPCResp) error {
	if n.Status != common.StatusActive || n.PaxosModule.Role != common.RoleLeader {
		slog.Info("2PC participant abort rejected", "reason", "not active leader")
		resp.Ack = false
		resp.Message = "not-leader"
		return nil
	}
	slog.Info("2PC participant abort consensus")
	if err := n.startConsensusForAbort(req.T); err != nil {
		slog.Warn("2PC participant abort consensus failed", "error", err)
		resp.Ack = false
		resp.Message = err.Error()
		return nil
	}
	resp.Ack = true
	resp.Message = "aborted"
	return nil
}

type FinalizeTPCReq struct {
	T      common.ClientReq
	Commit bool
}

type FinalizeTPCResp struct {
	Ack     bool
	Message string
}

// Finalize_2PC executes the second phase (commit/abort) on participant leaders.
func (n *Node) Finalize_2PC(req *FinalizeTPCReq, resp *FinalizeTPCResp) error {
	if n.Status == common.StatusStopped {
		slog.Info("participant finalize rejected: stopped")
		resp.Ack = false
		resp.Message = "node-stopped"
		return nil
	}

	if n.PaxosModule.Role != common.RoleLeader {
		slog.Info("participant finalize rejected: not leader")
		resp.Ack = false
		resp.Message = "not-leader"
		return nil
	}

	part := TwoPCAborted
	if req.Commit {
		part = TwoPCCommitted
	}

	slog.Info("participant finalize start", "commit", req.Commit, "from", req.T.TranscationVal.From, "to", req.T.TranscationVal.To)
	if _, err := n.runPaxosForTxn(-1, part, req.T); err != nil {
		slog.Warn("participant finalize paxos failed", "error", err)
		resp.Ack = false
		resp.Message = "finalize paxos failed"
		return nil
	}

	resp.Ack = true
	resp.Message = "ok"
	slog.Info("participant finalize done", "commit", req.Commit)
	return nil
}

func (n *Node) startConsensusForAbort(txn common.ClientReq) error {
	_, err := n.runConsensusWithSeq(txn, TwoPCAborted, -1)
	return err
}

func (n *Node) startConsensusForCommit(txn common.ClientReq) error {
	_, err := n.runConsensusWithSeq(txn, TwoPCCommitted, -1)
	return err
}
