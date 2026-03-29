// Package main is an executable that generates the names list used by the names package.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <input_txt> <output_go>\n", os.Args[0])
		os.Exit(1)
	}

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	in, err := os.Open(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open input: %v\n", err)
		os.Exit(1)
	}
	defer in.Close()

	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	fmt.Fprintln(out, "package names")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "func init() {")
	fmt.Fprintln(out, "\tloadedNames = []string{")

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		name := strings.TrimSpace(scanner.Text())
		if name != "" {
			fmt.Fprintf(out, "\t\t%q,\n", name)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(out, "\t}")
	fmt.Fprintln(out, "}")
}
