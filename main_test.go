package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func generateHDFSFiles(n int) (output []string) {
	for i := 0; i < n; i++ {
		output = append(output, fmt.Sprintf("part-%d", i))
	}
	return output
}

func mergeSlices(input [][]string) (output []string) {
	for _, slice := range input {
		output = append(output, slice...)
	}
	return output
}

func TestChunk(t *testing.T) {
	const (
		testTimes   = 1000
		maxN        = 10000
		maxChunkNum = 10000
	)
	for i := 0; i < testTimes; i++ {
		lst := generateHDFSFiles(rand.Intn(maxN))
		chunkNum := rand.Intn(maxChunkNum)
		slices := chunk(lst, chunkNum)
		merged := mergeSlices(slices)
		if len(merged) != len(lst) {
			t.Errorf("chunks merged length doesn't match, got %d and expect %d", len(merged), len(lst))
		}
		sort.Sort(sort.StringSlice(merged))
		sort.Sort(sort.StringSlice(lst))
		if !reflect.DeepEqual(lst, merged) {
			t.Errorf("chunks merged doesn't equal original list")
		}
	}
}
