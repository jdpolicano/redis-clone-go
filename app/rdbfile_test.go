package main

import (
	"testing"
)

func TestRDBFile(t *testing.T) {
	rdbParser, fileErr := NewRDBFileParser("../dump.rdb")
	if fileErr != nil {
		t.Fatal(fileErr)
	}
	_, parseErr := rdbParser.Parse()
	if parseErr != nil {
		t.Fatal(parseErr)
	}
}
