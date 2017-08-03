package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var config *Bingo

func prepare() {
	config, _ = Load("test_config.yml")

	// Make empty trees with insertion and deletion.
	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)

	var s = `{
			"channelId": "1",
			"id": "dummy",
			"expiresAt": 0
		}`

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var data Data
	dec.Decode(&data)
	table.Put(&data)

	table.Delete("1", "dummy")
}

func initData() {
	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
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
		table.Put(&data)
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
		table.Put(&data)
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
		table.Put(&data)
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
		table.Put(&data)
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
		table.Put(&data)
	}
}

func TestFetchPrimaryIndex(t *testing.T) {
	prepare()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
	primaryIndex := table.PrimaryIndex

	var result [](*Document)
	var next interface{}

	result, next = primaryIndex.Fetch("1", nil, "z", 10)
	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}

	initData()

	result, next = primaryIndex.Fetch("1", nil, "z", 10)
	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}

	result, next = primaryIndex.Fetch("1", nil, nil, 2)
	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next, "terry@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.Fetch("1", "a", "z", 10)
	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.Fetch("1", "r", nil, 10)

	if actualValue, expectedValue := len(result), 3; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.Fetch("1", "z", "a", 10)

	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}
}

func TestRFetchPrimaryIndex(t *testing.T) {
	prepare()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
	primaryIndex := table.PrimaryIndex

	var result [](*Document)
	var next interface{}

	result, next = primaryIndex.RFetch("1", nil, "t", 10)
	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}

	initData()

	result, next = primaryIndex.RFetch("1", nil, "t", 10)
	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}

	result, next = primaryIndex.RFetch("1", nil, nil, 2)
	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next, "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.RFetch("1", "a", "z", 3)
	if actualValue, expectedValue := len(result), 3; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next, "aaa@gmail.com"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.RFetch("1", "r", nil, 10)

	if actualValue, expectedValue := len(result), 3; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = primaryIndex.RFetch("1", "z", "a", 10)

	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if next != nil {
		t.Errorf("Value different. Got %v expected nil", next)
	}
}

func TestFetchSubIndex(t *testing.T) {
	prepare()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)

	var result [](*Document)
	var next SubSortTreeKey
	var emptySubSortTreeKey SubSortTreeKey

	result, next = table.Index("guest").Fetch("1", emptySubSortTreeKey, emptySubSortTreeKey, 10)

	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	initData()

	result, next = table.Index("guest").Fetch("1", emptySubSortTreeKey, emptySubSortTreeKey, 10)

	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	result, next = table.Index("guest").Fetch("1",
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		SubSortTreeKey{Key: int64(300), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		10)

	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	result, next = table.Index("guest").Fetch("1",
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		SubSortTreeKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.Schema)},
		2)

	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next.Document.data["id"], "test@gmail.com"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = table.Index("guest").Fetch("1",
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		SubSortTreeKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.Schema)},
		1)

	if actualValue, expectedValue := len(result), 1; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := result[0].data["id"], "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = table.Index("guest").Fetch("1",
		SubSortTreeKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.Schema)},
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		1)

	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}
}

func TestRFetchSubIndex(t *testing.T) {
	prepare()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)

	var result [](*Document)
	var next SubSortTreeKey
	var emptySubSortTreeKey SubSortTreeKey

	result, next = table.Index("guest").RFetch("1", emptySubSortTreeKey, emptySubSortTreeKey, 10)

	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	initData()

	result, next = table.Index("guest").RFetch("1", emptySubSortTreeKey, emptySubSortTreeKey, 10)

	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	result, next = table.Index("guest").RFetch("1",
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		SubSortTreeKey{Key: int64(300), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		10)

	if actualValue, expectedValue := len(result), 4; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}

	result, next = table.Index("guest").RFetch("1",
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		SubSortTreeKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.Schema)},
		2)

	if actualValue, expectedValue := len(result), 2; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next.Document.data["id"], "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = table.Index("guest").RFetch("1",
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		SubSortTreeKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.Schema)},
		1)

	if actualValue, expectedValue := len(result), 1; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := result[0].data["id"], "test@gmail.com"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := next.Document.data["id"], "aaa@gmail.com"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	result, next = table.Index("guest").RFetch("1",
		SubSortTreeKey{Key: int64(123), Document: ParseDoc(Data{"channelId": "1", "id": "z"}, table.Schema)},
		SubSortTreeKey{Key: int64(100), Document: ParseDoc(Data{"channelId": "1"}, table.Schema)},
		2)

	if actualValue, expectedValue := len(result), 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
	if !next.Empty() {
		t.Errorf("Value different. Got %v expected empty", next)
	}
}

func TestGetPrimaryIndex(t *testing.T) {
	prepare()
	initData()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
	primaryIndex := table.PrimaryIndex

	doc, ok := primaryIndex.Get("1", "red@zoyi.co")
	if !ok {
		t.Error("should not return failed")
	}
	if actualValue, expectedValue := doc.data["id"], "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := doc.data["lastSeen"], int64(100); actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := doc.data["expiresAt"], int64(200); actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	_, ok = primaryIndex.Get("1", "red@zoyi.com")
	if ok {
		t.Error("should not return success")
	}

	_, ok = primaryIndex.Get("2", "red@zoyi.co")
	if ok {
		t.Error("should not return success")
	}

}

func TestGetSubIndex(t *testing.T) {
	prepare()
	initData()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
	subIndex := table.Index("guest")

	doc, ok := subIndex.Get("1", "100")
	if !ok {
		t.Error("should not return failed")
	}
	if actualValue, expectedValue := doc.data["id"], "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := doc.data["lastSeen"], int64(100); actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := doc.data["expiresAt"], int64(200); actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	_, ok = subIndex.Get("1", "12321")
	if ok {
		t.Error("should not return success")
	}

	_, ok = subIndex.Get("2", "123")
	if ok {
		t.Error("should not return success")
	}

}

func TestDeletePrimaryIndex(t *testing.T) {
	prepare()
	initData()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
	primaryIndex := table.PrimaryIndex

	doc, ok := primaryIndex.delete("1", "red@zoyi.co")
	if !ok {
		t.Error("should not return failed")
	}
	if actualValue, expectedValue := doc.data["id"], "red@zoyi.co"; actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := doc.data["lastSeen"], int64(100); actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue, expectedValue := doc.data["expiresAt"], int64(200); actualValue != expectedValue {
		t.Errorf("Value different. Got %v expected %v", actualValue, expectedValue)
	}

	_, ok = primaryIndex.delete("1", "red@zoyi.co")
	if ok {
		t.Error("should not return success")
	}

	_, ok = primaryIndex.delete("1", "test@zoyi.co")
	if ok {
		t.Error("should not return success")
	}

	_, ok = primaryIndex.delete("2", "red@zoyi.co")
	if ok {
		t.Error("should not return success")
	}
}

func TestDeleteSubIndex(t *testing.T) {
	prepare()
	initData()

	tableData, _ := config.Tables.Load("onlines")
	table := tableData.(*Table)
	subIndex := table.Index("guest")
	document := ParseDoc(Data{"channelId": "1", "id": "red@zoyi.co", "lastSeen": int64(100)}, table.Schema)

	subIndex.delete(document)
	doc, ok := subIndex.Get("1", "100")
	if ok {
		fmt.Print(doc)
		t.Error("should not return success")
	}
}
