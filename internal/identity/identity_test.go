// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"testing"
)

func TestGenerate(t *testing.T) {
	shortName := "test-agent"
	ident, err := Generate(shortName)
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	if ident == nil {
		t.Fatal("Generate() returned nil identity")
	}

	if ident.Certificate == nil {
		t.Error("Generate() returned identity with nil certificate")
	} else if ident.Certificate.Subject.CommonName != shortName {
		t.Errorf("Generate() certificate common name = %q, want %q", ident.Certificate.Subject.CommonName, shortName)
	}

	if ident.PrivateKey == nil {
		t.Error("Generate() returned identity with nil private key")
	}
}

func TestMarshalUnmarshalCertificate(t *testing.T) {
	shortName := "test-agent-cert"
	ident, err := Generate(shortName)
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	pemData := MarshalCertificate(ident.Certificate)
	if len(pemData) == 0 {
		t.Fatal("MarshalCertificate() returned empty byte slice")
	}

	cert, err := UnmarshalCertificate(pemData)
	if err != nil {
		t.Fatalf("UnmarshalCertificate() failed: %v", err)
	}

	if cert == nil {
		t.Fatal("UnmarshalCertificate() returned nil certificate")
	}

	if cert.Subject.CommonName != shortName {
		t.Errorf("UnmarshalCertificate() returned certificate with CommonName %q, want %q", cert.Subject.CommonName, shortName)
	}
}

func TestUnmarshalCertificate_Errors(t *testing.T) {
	tests := []struct {
		name    string
		pemData []byte
	}{
		{
			name:    "empty input",
			pemData: []byte(""),
		},
		{
			name:    "invalid PEM data",
			pemData: []byte("NOT A PEM"),
		},
		{
			name:    "corrupted certificate bytes",
			pemData: []byte("-----BEGIN CERTIFICATE-----\nYXNkZg==\n-----END CERTIFICATE-----"),
		},
		{
			name:    "incorrect PEM block type",
			pemData: []byte("-----BEGIN PRIVATE KEY-----\nYXNkZg==\n-----END PRIVATE KEY-----"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalCertificate(tt.pemData)
			if err == nil {
				t.Error("UnmarshalCertificate() expected error, got nil")
			}
		})
	}
}
