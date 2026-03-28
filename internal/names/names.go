package names

import (
	"math/rand"
	"time"
)

var loadedNames []string

func init() {
	rand.Seed(time.Now().UnixNano())
}

// GenerateForIndex returns a random name from the pre-loaded list.
// It prefers names starting with 'A' for index 0, 'B' for index 1, etc.
func GenerateForIndex(index int) string {
	if len(loadedNames) == 0 {
		return "Anonymous"
	}

	targetLetter := byte('A' + (index % 26))
	var candidates []string
	for _, name := range loadedNames {
		if len(name) > 0 && name[0] == targetLetter {
			candidates = append(candidates, name)
		}
	}

	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))]
	}

	return loadedNames[rand.Intn(len(loadedNames))]
}

// Generate returns a purely random name from the pre-loaded list.
func Generate() string {
	if len(loadedNames) == 0 {
		return "Anonymous"
	}
	return loadedNames[rand.Intn(len(loadedNames))]
}
