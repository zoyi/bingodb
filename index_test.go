// +build !race

package bingodb

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

var bingo *Bingo

func prepare() {
	bingo = newBingo()
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", "test.yml"))

	bingo = NewBingoFromConfigFile(absPath)

	table := bingo.tables["onlines"]
	{
		var s = `{
			"channelId": "1",
			"id": "test@gmail.com",
			"lastSeen": 123,
			"expiresAt": 200
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
	{
		var s = `{
			"channelId": "1",
			"id": "test@gmail.com",
			"lastSeen": 123,
			"expiresAt": 201
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}

	{
		var s = `{
			"channelId": "1",
			"id": "aaa@gmail.com",
			"lastSeen": 123,
			"expiresAt": 202
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}

	{
		var s = `{
			"channelId": "1",
			"id": "terry@zoyi.co",
			"lastSeen": 144,
			"expiresAt": 210
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}

	{
		var s = `{
			"channelId": "1",
			"id": "red@zoyi.co",
			"lastSeen": 100,
			"expiresAt": 200
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
}

func TestScanForPrimaryIndex(t *testing.T) {
	prepare()

	table := bingo.tables["onlines"]
	primaryIndex := table.primaryIndex

	result, next := primaryIndex.Scan("1", nil, 10)
	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}

	result, next = primaryIndex.Scan("1", nil, 2)
	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next, "terry@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.Scan("1", "a", 10)
	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.Scan("1", "rz", 10)
	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRScanForPrimaryIndex(t *testing.T) {
	prepare()

	table := bingo.tables["onlines"]
	primaryIndex := table.primaryIndex

	result, next := primaryIndex.RScan("1", nil, 10)
	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}

	result, next = primaryIndex.RScan("1", nil, 2)
	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next, "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.RScan("1", "az", 3)
	if actualValue, expectedValue := len(result), 1; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue := next; next != nil {
		t.Errorf("Value different. Got %v expected %v", actualValue, nil)
	}

	result, next = primaryIndex.RScan("1", "rz", 10)

	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestFetchSubIndex(t *testing.T) {
	prepare()

	table := bingo.tables["onlines"]

	result, next := table.Index("guest").Scan("1", nil, 10)

	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	result, next = table.Index("guest").Scan("1", SubSortKey{sort: int64(100), primaryHash: "1"}, 10)

	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	result, next = table.Index("guest").Scan("1", SubSortKey{sort: int64(123), primaryHash: "1"}, 10)

	if actualValue, expectedValue := len(result), 3; actualValue != expectedValue {
		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	//
	//result, next = table.Index("guest").Fetch("1",
	//	SubSortKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.schema)},
	//	SubSortKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.schema)},
	//	2)
	//
	//if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
	//	t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	//}
	//if actualValue, expectedValue := next.Document.data["id"], "test@gmail.com"; actualValue != expectedValue {
	//	t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	//}
	//
	//result, next = table.Index("guest").Fetch("1",
	//	SubSortKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.schema)},
	//	SubSortKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.schema)},
	//	1)
	//
	//if actualValue, expectedValue := len(result), 1; actualValue != expectedValue {
	//	t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
	//}
	//if actualValue, expectedValue := result[0].data["id"], "red@zoyi.co"; actualValue != expectedValue {
	//	t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	//}
}

//func TestRFetchSubIndex(t *testing.T) {
//	prepare()
//
//	table := config.tables["onlines"]
//
//	var result [](*Document)
//	var next SubSortKey
//	var emptySubSortTreeKey SubSortKey
//
//	result, next = table.Index("guest").RFetch("1", emptySubSortTreeKey, emptySubSortTreeKey, 10)
//
//	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
//		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
//	}
//	if !next.Empty() {
//		t.Errorf("Value different. Got %v expected empty", next)
//	}
//
//	result, next = table.Index("guest").RFetch("1",
//		SubSortKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.schema)},
//		SubSortKey{Key: int64(300), Document: ParseDoc(Data{"channelId": "1"}, table.schema)},
//		10)
//
//	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
//		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
//	}
//	if !next.Empty() {
//		t.Errorf("Value different. Got %v expected empty", next)
//	}
//
//	result, next = table.Index("guest").RFetch("1",
//		SubSortKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.schema)},
//		SubSortKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.schema)},
//		2)
//
//	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
//		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue, expectedValue := next.Document.data["id"], "red@zoyi.co"; actualValue != expectedValue {
//		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
//	}
//
//	result, next = table.Index("guest").RFetch("1",
//		SubSortKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.schema)},
//		SubSortKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.schema)},
//		1)
//
//	if actualValue, expectedValue := len(result), 1; actualValue != expectedValue {
//		t.Errorf("size different. Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue, expectedValue := result[0].data["id"], "test@gmail.com"; actualValue != expectedValue {
//		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
//	}
//	if actualValue, expectedValue := next.Document.data["id"], "aaa@gmail.com"; actualValue != expectedValue {
//		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
//	}
//}
