// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	// First get the latest commit hash
	cmd := exec.Command("git", "log", "-n", "1", "--format=%H")
	out, err := cmd.Output()
	if err != nil {
		fmt.Printf("Failed to get git commit hash: %v\n", err)
		os.Exit(1)
	}
	hash := strings.TrimSpace(string(out))

	// Get pseudo-version info from proxy.golang.org
	url := fmt.Sprintf("https://proxy.golang.org/github.com/filmil/synod/@v/%s.info", hash)
	fmt.Printf("Fetching proxy info from %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error fetching from proxy: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to fetch proxy info, status: %s\n", resp.Status)
		os.Exit(1)
	}

	var info struct {
		Version string `json:"Version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		fmt.Printf("Failed to decode proxy info: %v\n", err)
		os.Exit(1)
	}

	// Wait a bit to ensure it propagated
	time.Sleep(2 * time.Second)

	// Now ping pkg.go.dev with the specific version
	fetchURL := fmt.Sprintf("https://pkg.go.dev/fetch/github.com/filmil/synod@%s", info.Version)
	fmt.Printf("Fetching %s\n", fetchURL)
	fetchResp, err := http.Post(fetchURL, "text/plain", bytes.NewReader(nil))
	if err != nil {
		fmt.Printf("Error pinging pkg.go.dev: %v\n", err)
		os.Exit(1)
	}
	defer fetchResp.Body.Close()
	fmt.Printf("pkg.go.dev response status: %s\n", fetchResp.Status)
}
