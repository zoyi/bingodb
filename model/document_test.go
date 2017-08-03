package model

import (
	"encoding/json"
	"testing"
)

var bingo *Bingo

func documentPrepare() {
	bingo, _ = Load("config.yml")
	InitDefaultSeedData(bingo)
}

func TestParseDoc(t *testing.T) {
	documentPrepare()

	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*Table)

	doc := ParseDoc(Data{"channelId": "1", "id": "doc_test"}, table.Schema)
	channelId := doc.Get("channelId")
	id := doc.Get("id")

	if channelId != "1" {
		t.Error("incorrect field value")
	}

	if id != "doc_test" {
		t.Error("inccorect field value")
	}
}

func TestDocument_ToJSON(t *testing.T) {
	documentPrepare()

	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*Table)

	doc := ParseDoc(Data{"channelId": "1", "id": "doc_test"}, table.Schema)

	var data Data
	jsonData, err := doc.ToJSON()
	if err != nil {
		t.Error("should not return error")
	}

	jsonError := json.Unmarshal(jsonData, &data)
	if jsonError != nil {
		t.Error("should not return error")
	}

	if data["channelId"] != "1" {
		t.Error("data contains incorrect value")
	}

	if data["id"] != "doc_test" {
		t.Error("data contains incorrect value")
	}
}

func TestDocument_GetData(t *testing.T) {
	documentPrepare()

	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*Table)

	doc := ParseDoc(Data{"channelId": "1", "id": "doc_test"}, table.Schema)
	data := doc.GetData()

	if data["channelId"] != "1" {
		t.Error("doc contains incorrect value")
	}

	if data["id"] != "doc_test" {
		t.Error("doc contains incorrect value")
	}
}

func TestDocument_GetExpiresAt(t *testing.T) {
	documentPrepare()

	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*Table)

	doc := ParseDoc(Data{"channelId": "1", "id": "doc_test", "expiresAt": 1234}, table.Schema)
	exp, ok := doc.GetExpiresAt()
	if !ok {
		t.Error("should not have error with valid expires at")
	}

	if exp != 1234 {
		t.Error("should have value with 1234")
	}

	doc = ParseDoc(Data{"channelId": "1", "id": "doc_test"}, table.Schema)
	_, ok = doc.GetExpiresAt()
	if ok {
		t.Error("empty expires at field should return error")
	}

	//doc = ParseDoc(Data{"channelId": "1", "id": "doc_test", "expiresAt":"invalid"}, table.Schema)
	//exp, ok = doc.GetExpiresAt()
	//if ok {
	//	t.Error("invalid expiresAt field value should return error")
	//}
}

func TestDocument_PrimaryKeyValue(t *testing.T) {
	documentPrepare()

	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*Table)

	doc := ParseDoc(Data{"channelId": "1", "id": "doc_test", "expiresAt": 1234}, table.Schema)

	hashKeyValue, sortKeyValue := doc.PrimaryKeyValue()
	if hashKeyValue != "1" {
		t.Error("incorrect hash key value")
	}

	if sortKeyValue != "doc_test" {
		t.Error("incorrect sort key value")
	}
}
