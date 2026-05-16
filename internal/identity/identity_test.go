// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"crypto/pem"
	"testing"
)

func TestUnmarshalCertificate(t *testing.T) {
	// 1. Success case: Generate a valid identity and round-trip its certificate.
	t.Run("Success", func(t *testing.T) {
		id, err := Generate("test-agent")
		if err != nil {
			t.Fatalf("Failed to generate identity: %v", err)
		}

		pemData := MarshalCertificate(id.Certificate)
		unmarshaled, err := UnmarshalCertificate(pemData)
		if err != nil {
			t.Fatalf("UnmarshalCertificate failed: %v", err)
		}

		if !bytes.Equal(id.Certificate.Raw, unmarshaled.Raw) {
			t.Errorf("Unmarshaled certificate RAW bytes do not match original")
		}
		if id.Certificate.Subject.CommonName != unmarshaled.Subject.CommonName {
			t.Errorf("Expected Subject %q, got %q", id.Certificate.Subject.CommonName, unmarshaled.Subject.CommonName)
		}
	})

	// 2. Failure: Invalid PEM block.
	t.Run("InvalidPEM", func(t *testing.T) {
		invalidPEM := []byte("this is not a PEM block")
		cert, err := UnmarshalCertificate(invalidPEM)
		if err == nil {
			t.Errorf("Expected error for invalid PEM data, got nil")
		}
		if cert != nil {
			t.Errorf("Expected nil certificate for invalid PEM data")
		}
	})

	// 3. Failure: Empty input.
	t.Run("EmptyInput", func(t *testing.T) {
		cert, err := UnmarshalCertificate([]byte{})
		if err == nil {
			t.Errorf("Expected error for empty input, got nil")
		}
		if cert != nil {
			t.Errorf("Expected nil certificate for empty input")
		}
	})

	// 4. Failure: Valid PEM but wrong type.
	t.Run("WrongPEMType", func(t *testing.T) {
		wrongBlock := &pem.Block{
			Type:  "NOT A CERTIFICATE",
			Bytes: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		}
		wrongData := pem.EncodeToMemory(wrongBlock)
		_, err := UnmarshalCertificate(wrongData)
		// UnmarshalCertificate doesn't explicitly check the Type field,
		// it just calls x509.ParseCertificate(block.Bytes).
		// So it will likely fail during parsing.
		if err == nil {
			t.Errorf("Expected error for wrong PEM type / invalid DER, got nil")
		}
	})

	// 5. Failure: Valid PEM "CERTIFICATE" but invalid DER bytes.
	t.Run("InvalidDER", func(t *testing.T) {
		invalidDERBlock := &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: []byte("definitely not a valid X.509 DER certificate"),
		}
		invalidData := pem.EncodeToMemory(invalidDERBlock)
		cert, err := UnmarshalCertificate(invalidData)
		if err == nil {
			t.Errorf("Expected error for invalid DER bytes, got nil")
		}
		if cert != nil {
			t.Errorf("Expected nil certificate for invalid DER bytes")
		}
	})
}
