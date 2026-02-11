package server

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"

	"pranavpateriya.com/bft/common"
)

// func BuildPrePrepareMessage(view, seq int, tx common.ClientReq, priv ed25519.PrivateKey, pub ed25519.PublicKey) (server.PrePrepareMessage, error) {
// 	d, err := DigestOfTransaction(tx)
// 	if err != nil {
// 		return server.PrePrepareMessage{}, err
// 	}
// 	pp := server.PrePrepare{ViewNumber: view, SequenceNum: seq, Digest: d}
// 	return server.PrePrepareMessage{
// 		PrePrepareSig: server.PrePrepareSig{
// 			PrePrepare:  pp,
// 			PPSignature: SignPrePrepare(pp, priv),
// 		},
// 		Transaction: tx,
// 	}, nil
// }


func DigestOfTransaction(tx common.ClientReq) (Digest) {
	// Canonical bytes to hash — here JSON of a *concrete* struct (no maps/any).
	b, err := json.Marshal(tx)
	if err != nil {
		slog.Error("error in generating digest of transaction", "error", err)
		return Digest{}
	}
	return sha256.Sum256(b)
}

func SignStruct(v any, priv ed25519.PrivateKey) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid ed25519 private key length")
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	signature := ed25519.Sign(priv, payload)
	return signature, nil
}

func verifyStruct(v any, sig []byte, pub ed25519.PublicKey) (bool, error) {
	if len(pub) != ed25519.PublicKeySize {
		return false, errors.New("invalid ed25519 public key length")
	}
	if len(sig) != ed25519.SignatureSize {
		return false, errors.New("invalid ed25519 signature length")
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	return ed25519.Verify(pub, payload, sig), nil
}
