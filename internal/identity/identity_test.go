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
