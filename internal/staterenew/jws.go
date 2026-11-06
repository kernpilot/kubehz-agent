package staterenew

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SignatureHeader carries the signed-request JWS (build contract 4.3).
const SignatureHeader = "Kubehz-State-Signature"

// Signer signs renewal requests with a client key and its certificate.
type Signer struct {
	Key  crypto.Signer
	Cert *x509.Certificate
}

// sigPayload is the JWS payload of build contract 4.3. bodySha256 is the
// lowercase hex SHA-256 of the exact request body bytes.
type sigPayload struct {
	Method     string `json:"method"`
	Path       string `json:"path"`
	BodySHA256 string `json:"bodySha256"`
	IAT        int64  `json:"iat"`
	JTI        string `json:"jti"`
	ClusterID  string `json:"clusterId"`
}

type sigHeader struct {
	Alg string   `json:"alg"`
	X5C []string `json:"x5c"`
}

var b64 = base64.RawURLEncoding

// Sign returns a compact JWS over the request: header {alg, x5c},
// payload {method, path, bodySha256, iat, jti, clusterId}. alg is ES256
// for a P-256 key and EdDSA for an Ed25519 key.
func (s Signer) Sign(method, path string, body []byte, clusterID string, now time.Time) (string, error) {
	if s.Key == nil || s.Cert == nil {
		return "", errors.New("no signing key or certificate")
	}
	var alg string
	switch k := s.Key.(type) {
	case *ecdsa.PrivateKey:
		if k.Curve != elliptic.P256() {
			return "", errors.New("ECDSA key is not P-256")
		}
		alg = "ES256"
	case ed25519.PrivateKey:
		alg = "EdDSA"
	default:
		return "", fmt.Errorf("key type %T cannot sign (want ECDSA P-256 or Ed25519)", s.Key)
	}
	jti := make([]byte, 16)
	if _, err := rand.Read(jti); err != nil {
		return "", fmt.Errorf("make request id: %w", err)
	}
	sum := sha256.Sum256(body)
	hb, err := json.Marshal(sigHeader{Alg: alg, X5C: []string{base64.StdEncoding.EncodeToString(s.Cert.Raw)}})
	if err != nil {
		return "", err
	}
	pb, err := json.Marshal(sigPayload{
		Method:     method,
		Path:       path,
		BodySHA256: hex.EncodeToString(sum[:]),
		IAT:        now.Unix(),
		JTI:        hex.EncodeToString(jti),
		ClusterID:  clusterID,
	})
	if err != nil {
		return "", err
	}
	input := b64.EncodeToString(hb) + "." + b64.EncodeToString(pb)
	var sig []byte
	switch k := s.Key.(type) {
	case *ecdsa.PrivateKey:
		digest := sha256.Sum256([]byte(input))
		r, ss, err := ecdsa.Sign(rand.Reader, k, digest[:])
		if err != nil {
			return "", fmt.Errorf("sign request: %w", err)
		}
		// JWS ES256 is the raw R||S, each 32 bytes big-endian (RFC 7518 3.4).
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		ss.FillBytes(sig[32:])
	case ed25519.PrivateKey:
		sig = ed25519.Sign(k, []byte(input))
	}
	return input + "." + b64.EncodeToString(sig), nil
}
