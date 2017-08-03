package redblacktree

import (
	"testing"
	"strconv"
)

func TestTree_InvalidFromJSON(t *testing.T) {
	tree := NewWithStringComparator()
	errData := `{
			1: 1,
			"test", "this"
		}`

	err := tree.FromJSON([]byte(errData))
	if err == nil {
		t.Error("should return error")
	}
}

func TestTree_FromJSON(t *testing.T) {
	tree := NewWithStringComparator()
	data := `{
			"5": "e",
			"6": "f",
			"7": "g",
			"4": "h"
		}`

	err := tree.FromJSON([]byte(data))
	// │   ┌── 7
	// └── 6
	//     │   ┌── 5
	//     └── 4

	if err != nil {
		t.Error("making tree from json error ", err)
	}

	count := 3
	for it := tree.Begin(); it.Present(); it.Next() {
		count++
		key, _ := strconv.Atoi(it.Key().(string))

		switch key {
		case count:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
	}

	if actualValue, expectedValue := tree.Size(), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
}
