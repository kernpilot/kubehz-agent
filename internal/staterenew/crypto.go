package staterenew

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"time"
)

// newKey makes a P-256 client key. ES256 signs the renewal requests with
// it, and k3s uses it for the datastore connection.
func newKey() (*ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("make client key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("encode client key: %w", err)
	}
	return key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// newCSR makes a certificate signing request for key. The api takes only
// the public key from it and sets the subject, the SAN and the usage
// itself, so the subject here is a label for people only.
func newCSR(key crypto.Signer, commonName string) ([]byte, error) {
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: commonName},
	}, key)
	if err != nil {
		return nil, fmt.Errorf("make certificate signing request: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), nil
}

// parseKey reads a PEM private key (PKCS#8 or SEC 1).
func parseKey(b []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("no PEM block in the client key")
	}
	switch block.Type {
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse client key: %w", err)
		}
		s, ok := k.(crypto.Signer)
		if !ok {
			return nil, errors.New("client key is not a signing key")
		}
		switch s.(type) {
		case *ecdsa.PrivateKey, ed25519.PrivateKey:
			return s, nil
		default:
			return nil, fmt.Errorf("client key type %T is not supported (want ECDSA P-256 or Ed25519)", s)
		}
	case "EC PRIVATE KEY":
		k, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse client key: %w", err)
		}
		return k, nil
	default:
		return nil, fmt.Errorf("client key has PEM type %q (want PRIVATE KEY)", block.Type)
	}
}

// parseCert reads the first PEM certificate of b.
func parseCert(b []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(b)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("no PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

// checkClientCert checks that cert is a client certificate for key and is
// valid now. The api sets the fields; this catches a wrong answer before
// k3s uses it.
func checkClientCert(cert *x509.Certificate, key crypto.Signer, now time.Time) error {
	pub, ok := key.Public().(interface{ Equal(crypto.PublicKey) bool })
	if !ok || !pub.Equal(cert.PublicKey) {
		return errors.New("the certificate is not for this machine's key")
	}
	if now.Before(cert.NotBefore.Add(-5*time.Minute)) || now.After(cert.NotAfter) {
		return fmt.Errorf("the certificate is not valid now (valid %s to %s)", cert.NotBefore.UTC().Format(time.RFC3339), cert.NotAfter.UTC().Format(time.RFC3339))
	}
	clientAuth := len(cert.ExtKeyUsage) == 0
	for _, u := range cert.ExtKeyUsage {
		if u == x509.ExtKeyUsageClientAuth {
			clientAuth = true
		}
	}
	if !clientAuth {
		return errors.New("the certificate does not allow client authentication")
	}
	return nil
}

// checkCABundle checks that b holds at least one PEM CA certificate.
func checkCABundle(b []byte) error {
	found := false
	for rest := b; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return fmt.Errorf("serving CA bundle has a PEM block of type %q", block.Type)
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("parse serving CA: %w", err)
		}
		if !c.IsCA {
			return errors.New("serving CA bundle has a certificate that is not a CA")
		}
		found = true
	}
	if !found {
		return errors.New("serving CA bundle has no certificate")
	}
	return nil
}
