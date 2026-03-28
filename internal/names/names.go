package names

import (
	"math/rand"
	"time"
)

var loadedNames []string

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Generate returns a random name from the pre-loaded list.
// If the list is empty, it returns a fallback string.
func Generate() string {
	if len(loadedNames) == 0 {
		return "Anonymous"
	}
	return loadedNames[rand.Intn(len(loadedNames))]
}
