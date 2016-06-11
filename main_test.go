package main

import (
	"fmt"
)

func generateHDFSFiles(n int) (output []string) {
	for i := 0; i < n; i++ {
		output = append(output, fmt.Sprintf("part-%d", i))
	}
	return output
}
