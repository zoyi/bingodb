package model

import (
	"encoding/json"
	"strings"
	"testing"
)

var config *Bingo

func prepare() {
	config = Load("test_config.yml")

	table := config.Tables["onlines"]

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

	primaryIndex := config.Tables["onlines"].PrimaryIndex

	var result [](*Document)
	var next interface{}

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
}

func TestRFetchPrimaryIndex(t *testing.T) {
	prepare()

	primaryIndex := config.Tables["onlines"].PrimaryIndex

	var result [](*Document)
	var next interface{}

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
}

func TestFetchSubIndex(t *testing.T) {
	prepare()

	table := config.Tables["onlines"]

	var result [](*Document)
	var next SubSortTreeKey
	var emptySubSortTreeKey SubSortTreeKey

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
}

func TestRFetchSubIndex(t *testing.T) {
	prepare()

	table := config.Tables["onlines"]

	var result [](*Document)
	var next SubSortTreeKey
	var emptySubSortTreeKey SubSortTreeKey

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
}
