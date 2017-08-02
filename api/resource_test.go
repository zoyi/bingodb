package api

import (
	"encoding/json"
	"fmt"
	"github.com/zoyi/bingodb/mock"
	"github.com/zoyi/bingodb/model"
	"strings"
	"testing"
)

var response *Resource

func resourcePrepare() {
	bingo, _ := model.Load("config.yml")
	mock.InitDefaultSeedData(bingo)
	resource = &Resource{
		Db:          bingo,
		AccessToken: "",
	}
}

func TestResource_Fetch(t *testing.T) {
	resourcePrepare()
	tableData, _ := resource.Db.Tables.Load("onlines")
	table, _ := tableData.(*model.Table)
	fmt.Printf("tree :%v\n", resource.Db.Keeper.String())
	indexName := new(string)
	*indexName = "guest"

	hashKey := table.PrimaryIndex.HashKey.Parse("1")
	startKey := table.PrimaryIndex.SortKey.Parse("test")

	params := GetParams{
		TableName:    "onlines",
		SubIndexName: nil,
		HashKey:      hashKey,
		StartKey:     startKey,
		EndKey:       nil,
		Limit:        20,
		Order:        "DESC",
	}

	docs, ok := resource.Fetch(table, params)
	if !ok {
		t.Error("should return ok")
	}

	if len(docs) != 2 {
		t.Error("should return length 2: ", len(docs))
	}
}

func TestResource_FetchFromPrimary(t *testing.T) {
	resourcePrepare()
	tableData, _ := resource.Db.Tables.Load("onlines")
	table, _ := tableData.(*model.Table)

	params := GetParams{
		TableName:    "onlines",
		SubIndexName: nil,
		HashKey:      table.PrimaryIndex.HashKey.Parse("1"),
		StartKey:     table.PrimaryIndex.SortKey.Parse("ddd"),
		EndKey:       nil,
		Limit:        20,
		Order:        "DESC",
	}

	docs, ok := resource.FetchFromPrimary(table, params)
	if !ok {
		t.Error("should return ok")
	}

	if len(docs) != 3 {
		t.Error("should return length 3, return", len(docs))
	}

	params = GetParams{
		TableName:    "onlines",
		SubIndexName: nil,
		HashKey:      table.PrimaryIndex.HashKey.Parse("1"),
		StartKey:     table.PrimaryIndex.SortKey.Parse("ddd"),
		EndKey:       nil,
		Limit:        20,
		Order:        "ASC",
	}

	docs, ok = resource.FetchFromPrimary(table, params)
	if !ok {
		t.Error("should return ok")
	}

	if len(docs) != 3 {
		t.Error("should return length 3, return", len(docs))
	}

}

func TestResource_FetchFromSubIndices(t *testing.T) {
	resourcePrepare()
	tableData, _ := resource.Db.Tables.Load("onlines")
	table, _ := tableData.(*model.Table)

	indexName := new(string)
	*indexName = "guest"

	subIndexData, _ := table.SubIndices.Load("guest")
	subIndex, _ := subIndexData.(*model.SubIndex)

	params := GetParams{
		TableName:    "onlines",
		SubIndexName: indexName,
		HashKey:      subIndex.HashKey.Parse("1"),
		StartKey:     subIndex.SortKey.Parse("120"),
		EndKey:       subIndex.SortKey.Parse("140"),
		Limit:        20,
		Order:        "DESC",
	}

	docs, ok := resource.FetchFromSubIndices(table, params)
	if !ok {
		t.Error("should return ok")
	}

	if len(docs) != 3 {
		t.Error("should return length 3: %d", len(docs))
	}

	params = GetParams{
		TableName:    "onlines",
		SubIndexName: nil,
		HashKey:      "1",
		StartKey:     "120",
		EndKey:       "140",
		Limit:        20,
		Order:        "DESC",
	}

	docs, ok = resource.FetchFromSubIndices(table, params)
	if ok {
		t.Error("should return not ok")
	}

	params = GetParams{
		TableName:    "onlines",
		SubIndexName: indexName,
		HashKey:      subIndex.HashKey.Parse("1"),
		StartKey:     subIndex.SortKey.Parse("120"),
		EndKey:       subIndex.SortKey.Parse("140"),
		Limit:        1,
		Order:        "ASC",
	}

	docs, ok = resource.FetchFromSubIndices(table, params)
	if !ok {
		t.Error("should return ok")
	}

	if len(docs) != 1 {
		t.Error("should return length 1: %d", len(docs))
	}

	invalidName := new(string)
	*invalidName = "invalid"

	params = GetParams{
		TableName:    "onlines",
		SubIndexName: invalidName,
		HashKey:      subIndex.HashKey.Parse("1"),
		StartKey:     subIndex.SortKey.Parse("120"),
		EndKey:       subIndex.SortKey.Parse("140"),
		Limit:        1,
		Order:        "ASC",
	}

	docs, ok = resource.FetchFromSubIndices(table, params)
	if ok {
		t.Error("should return not ok")
	}
}

func TestResource_Get(t *testing.T) {
	resourcePrepare()

	jsonData := resource.Get("onlines", "1", "test")
	if len(jsonData) == 0 {
		t.Error("should return non-empty json value")
	}

	jsonData = resource.Get("onlines", "2", "test")
	if len(jsonData) != 2 {
		t.Error("should return empty json value: ", jsonData)
	}

	jsonData = resource.Get("onlines", 2, "test")
	if len(jsonData) != 2 {
		t.Error("should return empty json value: ", jsonData)
	}
}

func TestResource_GetMultiple(t *testing.T) {
	resourcePrepare()

	params := &GetParams{
		TableName:    "onlines",
		SubIndexName: nil,
		HashKey:      "1",
		StartKey:     "test",
		EndKey:       nil,
		Limit:        20,
		Order:        "DESC",
	}

	jsonData := resource.GetMultiple(params)
	var response []model.Data
	json.Unmarshal(jsonData, &response)

	if len(response) != 2 {
		t.Error("should return 2 but ", len(response))
	}

	params = &GetParams{
		TableName:    "onlines",
		SubIndexName: nil,
		HashKey:      "1",
		StartKey:     "ddd",
		EndKey:       nil,
		Limit:        20,
		Order:        "DESC",
	}

	jsonData = resource.GetMultiple(params)
	json.Unmarshal(jsonData, &response)

	if len(response) != 3 {
		t.Error("should return 3 but ", len(response))
	}

	invalidName := new(string)
	*invalidName = "invalid"

	params = &GetParams{
		TableName:    "onlines",
		SubIndexName: invalidName,
		HashKey:      "1",
		StartKey:     "ddd",
		EndKey:       nil,
		Limit:        20,
		Order:        "DESC",
	}

	jsonData = resource.GetMultiple(params)
	if len(jsonData) != 2 {
		t.Error("should return empty json value: ", jsonData)
	}

	params = &GetParams{
		TableName:    "online",
		SubIndexName: nil,
		HashKey:      "1",
		StartKey:     "ddd",
		EndKey:       nil,
		Limit:        20,
		Order:        "DESC",
	}

	jsonData = resource.GetMultiple(params)
	if len(jsonData) != 2 {
		t.Error("should return empty json value: ", jsonData)
	}
}

func TestResource_Update(t *testing.T) {
	resourcePrepare()

	var s = `{
			"channelId": "2",
			"id": "hehehe",
			"lastSeen": 150,
			"expiresAt": 201
		}`

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var data model.Data
	dec.Decode(&data)

	jsonData := resource.Update("onlines", &data)
	if len(jsonData) == 2 {
		t.Error("should return valid json data")
	}

	jsonData = resource.Update("invalid", &data)
	if len(jsonData) != 2 {
		t.Error("should return empty json")
	}
}

func TestResource_Delete(t *testing.T) {
	resourcePrepare()

	var s = `{
			"channelId": "1",
			"id": "what",
			"lastSeen": 150,
			"expiresAt": 201
		}`

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var data model.Data
	dec.Decode(&data)

	resource.Delete("onlines", "1", "what")
}
