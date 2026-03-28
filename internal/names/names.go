package names

import (
	"bufio"
	"math/rand"
	"os"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/golang/glog"
)

var loadedNames []string

func init() {
	rand.Seed(time.Now().UnixNano())
	
	path, err := runfiles.Rlocation("_main/internal/names/names.txt")
	if err != nil {
		path, err = runfiles.Rlocation("synod/internal/names/names.txt")
	}
	
	if err != nil {
		glog.Errorf("Failed to resolve names.txt runfile: %v", err)
		return
	}

	file, err := os.Open(path)
	if err != nil {
		glog.Errorf("Failed to open names.txt: %v", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		loadedNames = append(loadedNames, scanner.Text())
	}
}

// Generate returns a random name from the pre-loaded list.
// If the list is empty, it returns a fallback string.
func Generate() string {
	if len(loadedNames) == 0 {
		return "Anonymous"
	}
	return loadedNames[rand.Intn(len(loadedNames))]
}
