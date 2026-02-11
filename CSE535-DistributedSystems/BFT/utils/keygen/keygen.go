package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/gob"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ---- Helpers ----

func Short(b []byte) string {
	if len(b) < 8 {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%x…%x", b[:4], b[len(b)-4:])
}

// ---- Save / Load (PEM, stdlib) ----

func SaveEd25519PrivateKeyPEM(path string, priv ed25519.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(priv) // PKCS#8
	if err != nil {
		return err
	}
	block := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	// 0600 so only current user can read/write
	return os.WriteFile(path, pem.EncodeToMemory(block), 0o600)
}

func SaveEd25519PublicKeyPEM(path string, pub ed25519.PublicKey) error {
	der, err := x509.MarshalPKIXPublicKey(pub) // SubjectPublicKeyInfo (PKIX)
	if err != nil {
		return err
	}
	block := &pem.Block{Type: "PUBLIC KEY", Bytes: der}
	// 0644 is fine for public keys
	return os.WriteFile(path, pem.EncodeToMemory(block), 0o644)
}

func LoadEd25519PrivateKeyPEM(path string) (ed25519.PrivateKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("no PKCS#8 PRIVATE KEY block found")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	sk, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("not an Ed25519 private key")
	}
	return sk, nil
}

func LoadEd25519PublicKeyPEM(path string) (ed25519.PublicKey, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "PUBLIC KEY" {
		return nil, errors.New("no PUBLIC KEY block found")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pk, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("not an Ed25519 public key")
	}
	return pk, nil
}

// ---- Generate or Load convenience ----

func EnsureKeypair(privPath, pubPath string) (ed25519.PublicKey, ed25519.PrivateKey, error) {
	// If both exist, load
	if fileExists(privPath) && fileExists(pubPath) {
		priv, err := LoadEd25519PrivateKeyPEM(privPath)
		if err != nil {
			return nil, nil, err
		}
		pub, err := LoadEd25519PublicKeyPEM(pubPath)
		if err != nil {
			return nil, nil, err
		}
		return pub, priv, nil
	}

	// Else generate and save
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(privPath), 0o755); err != nil && !os.IsExist(err) {
		return nil, nil, err
	}
	if err := SaveEd25519PrivateKeyPEM(privPath, priv); err != nil {
		return nil, nil, err
	}
	if err := SaveEd25519PublicKeyPEM(pubPath, pub); err != nil {
		return nil, nil, err
	}
	return pub, priv, nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// ---- Generate Multiple Keypairs ----

func GenerateNodeKeypairs(baseDir string, count int) error {
	fmt.Printf("\n=== Generating %d Node Keypairs ===\n", count)
	
	for i := 1; i <= count; i++ {
		privPath := filepath.Join(baseDir, fmt.Sprintf("node%d_private.pem", i))
		pubPath := filepath.Join(baseDir, fmt.Sprintf("node%d_public.pem", i))
		
		pub, priv, err := EnsureKeypair(privPath, pubPath)
		if err != nil {
			return fmt.Errorf("failed to generate node %d keypair: %v", i, err)
		}
		
		fmt.Printf("Node %d: Private=%s, Public=%s\n", i, Short(priv), Short(pub))
	}
	
	return nil
}

func GenerateClientKeypairs(baseDir string) error {
	fmt.Printf("\n=== Generating 10 Client Keypairs ===\n")
	
	clientIDs := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	
	for _, id := range clientIDs {
		privPath := filepath.Join(baseDir, fmt.Sprintf("client%s_private.pem", id))
		pubPath := filepath.Join(baseDir, fmt.Sprintf("client%s_public.pem", id))
		
		pub, priv, err := EnsureKeypair(privPath, pubPath)
		if err != nil {
			return fmt.Errorf("failed to generate client %s keypair: %v", id, err)
		}
		
		fmt.Printf("Client %s: Private=%s, Public=%s\n", id, Short(priv), Short(pub))
	}
	
	return nil
}

// ---- Demo ----

func main() {
	const keysDir = "keys/nodes"
	
	// Generate 7 node keypairs
	if err := GenerateNodeKeypairs(keysDir, 7); err != nil {
		panic(err)
	}
	
	// Generate 10 client keypairs
	if err := GenerateClientKeypairs(keysDir); err != nil {
		panic(err)
	}
	
	fmt.Println("\n=== All keypairs generated successfully! ===")
	fmt.Println("\nDirectory structure:")
	fmt.Println("keys/nodes/")
	fmt.Println("  ├── node1_private.pem / node1_public.pem")
	fmt.Println("  ├── node2_private.pem / node2_public.pem")
	fmt.Println("  ├── ...")
	fmt.Println("  ├── node7_private.pem / node7_public.pem")
	fmt.Println("  ├── clientA_private.pem / clientA_public.pem")
	fmt.Println("  ├── clientB_private.pem / clientB_public.pem")
	fmt.Println("  ├── ...")
	fmt.Println("  └── clientJ_private.pem / clientJ_public.pem")
	
	// Example: Test with node1's keys
	fmt.Println("\n=== Testing Signature with Node 1 ===")
	privPath := filepath.Join(keysDir, "node1_private.pem")
	pubPath := filepath.Join(keysDir, "node1_public.pem")
	
	pub, priv, err := EnsureKeypair(privPath, pubPath)
	if err != nil {
		panic(err)
	}
	
	msg := struct {
		Name   string
		Course string
	}{
		Name:   "Pranav",
		Course: "CSE535",
	}
	
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	_ = encoder.Encode(msg)
	data := buf.Bytes()
	
	sig := ed25519.Sign(priv, data)
	ok := ed25519.Verify(pub, data, sig)
	fmt.Printf("Message signed and verified: %v\n", ok)
	fmt.Printf("Signature: %s\n", Short(sig))
}
