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

func TestUnmarshalPrivateKey(t *testing.T) {
	shortName := "test-agent"
	ident, err := Generate(shortName)
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	t.Run("unencrypted", func(t *testing.T) {
		pemBytes, err := MarshalPrivateKey(ident.PrivateKey, "")
		if err != nil {
			t.Fatalf("MarshalPrivateKey() failed: %v", err)
		}

		key, err := UnmarshalPrivateKey(pemBytes, "")
		if err != nil {
			t.Fatalf("UnmarshalPrivateKey() failed: %v", err)
		}
		if key == nil {
			t.Error("UnmarshalPrivateKey() returned nil key")
		}
	})

	t.Run("encrypted", func(t *testing.T) {
		passphrase := "secret123"
		pemBytes, err := MarshalPrivateKey(ident.PrivateKey, passphrase)
		if err != nil {
			t.Fatalf("MarshalPrivateKey() failed: %v", err)
		}

		key, err := UnmarshalPrivateKey(pemBytes, passphrase)
		if err != nil {
			t.Fatalf("UnmarshalPrivateKey() failed: %v", err)
		}
		if key == nil {
			t.Error("UnmarshalPrivateKey() returned nil key")
		}
	})

	t.Run("encrypted with wrong passphrase", func(t *testing.T) {
		passphrase := "secret123"
		pemBytes, err := MarshalPrivateKey(ident.PrivateKey, passphrase)
		if err != nil {
			t.Fatalf("MarshalPrivateKey() failed: %v", err)
		}

		_, err = UnmarshalPrivateKey(pemBytes, "wrongpassword")
		if err == nil {
			t.Error("UnmarshalPrivateKey() succeeded with wrong passphrase, expected error")
		}
	})

	t.Run("invalid PEM", func(t *testing.T) {
		_, err := UnmarshalPrivateKey([]byte("not a real pem"), "")
		if err == nil {
			t.Error("UnmarshalPrivateKey() succeeded with invalid PEM, expected error")
		}
	})
}
