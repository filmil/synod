// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Identity represents the cryptographic identity of an agent.
type Identity struct {
	Certificate *x509.Certificate
	PrivateKey  crypto.PrivateKey
}

// Generate creates a new self-signed X.509 certificate and ECDSA private key.
func Generate(shortName string) (*Identity, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: shortName,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	return &Identity{
		Certificate: cert,
		PrivateKey:  priv,
	}, nil
}

// AgentID derives a unique agent ID from the certificate's public key.
func (i *Identity) AgentID() string {
	pubBytes, err := x509.MarshalPKIXPublicKey(i.Certificate.PublicKey)
	if err != nil {
		// This should not happen with a valid certificate
		return ""
	}
	hash := sha256.Sum256(pubBytes)
	return hex.EncodeToString(hash[:])
}

// Sign creates a signature for the given data using the private key.
func (i *Identity) Sign(data []byte) ([]byte, error) {
	hash := sha256.Sum256(data)
	switch priv := i.PrivateKey.(type) {
	case *ecdsa.PrivateKey:
		r, s, err := ecdsa.Sign(rand.Reader, priv, hash[:])
		if err != nil {
			return nil, err
		}
		return asn1.Marshal(struct{ R, S *big.Int }{r, s})
	default:
		return nil, fmt.Errorf("unsupported private key type: %T", i.PrivateKey)
	}
}

// Verify checks a signature against the given data and certificate.
func Verify(cert *x509.Certificate, data []byte, signature []byte) error {
	hash := sha256.Sum256(data)
	switch pub := cert.PublicKey.(type) {
	case *ecdsa.PublicKey:
		var sig struct{ R, S *big.Int }
		if _, err := asn1.Unmarshal(signature, &sig); err != nil {
			return err
		}
		if ecdsa.Verify(pub, hash[:], sig.R, sig.S) {
			return nil
		}
		return fmt.Errorf("invalid signature")
	default:
		return fmt.Errorf("unsupported public key type: %T", cert.PublicKey)
	}
}

// MarshalPrivateKey exports the private key in PEM format, optionally encrypted.
func MarshalPrivateKey(priv crypto.PrivateKey, passphrase string) ([]byte, error) {
	var block *pem.Block
	switch k := priv.(type) {
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, err
		}
		block = &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil, fmt.Errorf("unsupported private key type: %T", priv)
	}

	if passphrase != "" {
		var err error
		block, err = x509.EncryptPEMBlock(rand.Reader, block.Type, block.Bytes, []byte(passphrase), x509.PEMCipherAES256)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt private key: %w", err)
		}
	}

	return pem.EncodeToMemory(block), nil
}

// UnmarshalPrivateKey imports a private key from PEM format.
func UnmarshalPrivateKey(data []byte, passphrase string) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	der := block.Bytes
	if x509.IsEncryptedPEMBlock(block) {
		var err error
		der, err = x509.DecryptPEMBlock(block, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt private key: %w", err)
		}
	}

	if block.Type == "EC PRIVATE KEY" {
		return x509.ParseECPrivateKey(der)
	}
	return x509.ParsePKCS8PrivateKey(der)
}

// MarshalCertificate exports the certificate in PEM format.
func MarshalCertificate(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

// UnmarshalCertificate imports a certificate from PEM format.
func UnmarshalCertificate(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}
	return x509.ParseCertificate(block.Bytes)
}

// SignMessage signs a protobuf message. It clears the 'auth' field before signing.
func (i *Identity) SignMessage(msg proto.Message) (signature []byte, certificate []byte, err error) {
	m := msg.ProtoReflect()
	descriptor := m.Descriptor()
	authField := descriptor.Fields().ByName("auth")

	var originalAuth protoreflect.Value
	var hasAuth bool
	if authField != nil {
		hasAuth = m.Has(authField)
		if hasAuth {
			originalAuth = m.Get(authField)
			m.Clear(authField)
		}
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal message for signing: %w", err)
	}

	// Restore original auth if it existed
	if hasAuth {
		m.Set(authField, originalAuth)
	}

	sig, err := i.Sign(data)
	if err != nil {
		return nil, nil, err
	}

	return sig, i.Certificate.Raw, nil
}

// VerifyMessage verifies a protobuf message's signature. It clears the 'auth' field before verifying.
func VerifyMessage(cert *x509.Certificate, msg proto.Message, signature []byte) error {
	m := msg.ProtoReflect()
	descriptor := m.Descriptor()
	authField := descriptor.Fields().ByName("auth")

	if authField == nil {
		return fmt.Errorf("message does not have an 'auth' field")
	}

	var originalAuth protoreflect.Value
	hasAuth := m.Has(authField)
	if hasAuth {
		originalAuth = m.Get(authField)
		m.Clear(authField)
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message for verification: %w", err)
	}

	if hasAuth {
		m.Set(authField, originalAuth)
	}

	return Verify(cert, data, signature)
}
