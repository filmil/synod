package names

import (
	"testing"
)

func TestGenerate(t *testing.T) {
	if len(loadedNames) == 0 {
		t.Fatal("loadedNames is empty")
	}
	name := Generate()
	if name == "" || name == "Anonymous" {
		t.Errorf("Generate() returned %q, expected a name from the list", name)
	}
}
