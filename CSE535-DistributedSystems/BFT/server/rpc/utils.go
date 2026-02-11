package server

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"pranavpateriya.com/bft/common"
)

// getLeader is static, we hard code m to 7
func (n *Node) getLeader() int {
	return leaderForView(n.CurrentView)
}

func leaderForView(view int) int {
	m := getTotalNodes()
	if m <= 0 {
		return 0
	}
	if view <= 0 {
		view = 1
	}
	v := view % m
	if v == 0 {
		v = m
	}
	return v
}

func (n *Node) getPublicKey(i int) ed25519.PublicKey {
	return n.NodePublicKeyMaps[i]
}

func (n *Node) crashActive() bool {
	return n != nil && n.CrashAttackFlag
}

func (n *Node) crashLeader(view int) bool {
	if !n.crashActive() {
		return false
	}
	return leaderForView(view) == n.NodeID
}

func (n *Node) crashReplica(view int) bool {
	if !n.crashActive() {
		return false
	}
	return leaderForView(view) != n.NodeID
}

func getEmptyLogEntry() *LogEntry {
	return &LogEntry{
		IsNoOp:     false,
		ViewNumber: 0,
		Txn: common.ClientReq{
			TransactionVal: common.TransactionVal{},
			TransactionID:  common.TransactionID{},
		},
		Status: StatusNone,
		Reply:  common.NodeResponse{},
	}
}

func clonePrepareSigned(src PrepareSigned) PrepareSigned {
	return PrepareSigned{
		PrePrepareResponse: src.PrePrepareResponse,
		PRSignature:        append([]byte(nil), src.PRSignature...),
	}
}

func clonePrepareCerts(src []PrepareSigned) []PrepareSigned {
	if len(src) == 0 {
		return nil
	}
	dst := make([]PrepareSigned, len(src))
	for i, cert := range src {
		dst[i] = clonePrepareSigned(cert)
	}
	return dst
}

func clonePrePrepareSigned(src PrePrepareSigned) PrePrepareSigned {
	return PrePrepareSigned{
		PrePrepare: PrePrepare{
			ViewNumber:  src.PrePrepare.ViewNumber,
			SequenceNum: src.PrePrepare.SequenceNum,
			Digest:      src.PrePrepare.Digest,
		},
		PPSignature: append([]byte(nil), src.PPSignature...),
	}
}

func clonePrepareProof(src PrepareProof) PrepareProof {
	msg := PrePrepareMessage{
		PrePrepareSig: clonePrePrepareSigned(src.PrePrepareMessage.PrePrepareSig),
		Transaction:   src.PrePrepareMessage.Transaction,
	}
	return PrepareProof{
		PrePrepareMessage: msg,
		Prepares:          clonePrepareCerts(src.Prepares),
	}
}

func cloneCommitSigned(src CommitSigned) CommitSigned {
	dst := CommitSigned{
		Commit: Commit{
			Ack:            src.Commit.Ack,
			ViewNumber:     src.Commit.ViewNumber,
			SequenceNumber: src.Commit.SequenceNumber,
			NodeID:         src.Commit.NodeID,
		},
		CommitSignature: append([]byte(nil), src.CommitSignature...),
	}
	if len(src.Commit.Digest) != 0 {
		dst.Commit.Digest = append([]byte(nil), src.Commit.Digest...)
	}
	return dst
}

// TODO: for identifying gap
func isEmptyLogEntry(logEntry *LogEntry) bool {
	if logEntry == nil {
		return true
	}

	// No-op entries are not considered empty
	if logEntry.IsNoOp {
		return false
	}

	// Check if all important fields are zero/default
	return logEntry.Status == StatusNone &&
		logEntry.ViewNumber == 0 &&
		logEntry.Digest == [32]byte{} &&
		logEntry.Txn.TransactionID.UniqueID == "" &&
		logEntry.Txn.TransactionID.ClientID == ""
}

func NewPublicKeyMaps(configRoot string) (PublicKeyMaps, error) {
	entries, err := os.ReadDir(configRoot)
	if err != nil {
		return PublicKeyMaps{}, fmt.Errorf("read config dir %q: %w", configRoot, err)
	}

	result := PublicKeyMaps{
		NodeKeys:   make(map[int]ed25519.PublicKey),
		ClientKeys: make(map[string]ed25519.PublicKey),
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		switch {
		case strings.HasPrefix(name, "node"):
			idStr := strings.TrimPrefix(name, "node")
			if idStr == "" {
				return PublicKeyMaps{}, fmt.Errorf("invalid node directory name %q", name)
			}

			key, err := loadEd25519PublicKey(filepath.Join(configRoot, name, "public.pem"))
			if err != nil {
				return PublicKeyMaps{}, fmt.Errorf("load public key for node %s: %w", idStr, err)
			}

			id, err := strconv.Atoi(idStr)
			if err != nil {
				return PublicKeyMaps{}, fmt.Errorf("parse node id from %q: %w", name, err)
			}
			result.NodeKeys[id] = key

		case strings.HasPrefix(name, "client"):
			clientID := strings.TrimPrefix(name, "client")
			if clientID == "" {
				clientID = "default"
			}

			key, err := loadEd25519PublicKey(filepath.Join(configRoot, name, "public.pem"))
			if err != nil {
				return PublicKeyMaps{}, fmt.Errorf("load public key for client %s: %w", clientID, err)
			}
			result.ClientKeys[clientID] = key
		}
	}

	return result, nil
}

func loadEd25519PrivateKey(path string) (ed25519.PrivateKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("%s: no PEM data found", path)
	}
	if block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s: unexpected PEM type %q", path, block.Type)
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 private key", path)
	}
	return priv, nil
}

func loadEd25519PublicKey(path string) (ed25519.PublicKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("%s: no PEM data found", path)
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("%s: unexpected PEM type %q", path, block.Type)
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 public key", path)
	}
	return pub, nil
}

func getTotalNodes() int {
	return 7
}

func getTotalClients() int {
	return 10
}

func getDigestOfState(state map[string]int) [32]byte {
	if len(state) == 0 {
		return sha256.Sum256([]byte{})
	}

	// Collect and sort keys for deterministic ordering
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	hasher := sha256.New()
	var lenBuf [4]byte
	var valBuf [8]byte

	for _, k := range keys {
		// Write key length
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(k)))
		hasher.Write(lenBuf[:])

		// Write key
		hasher.Write([]byte(k))

		// Write value (positive integers only)
		binary.LittleEndian.PutUint64(valBuf[:], uint64(state[k]))
		hasher.Write(valBuf[:])
	}

	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}
