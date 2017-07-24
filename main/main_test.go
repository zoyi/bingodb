package main

import (
	"encoding/json"
	"fmt"
	"github.com/zoyi/bingodb/model"
	"strings"
	"testing"
)

var bingo *model.Bingo

func prepare() {
	bingo = model.Load("test_config.yml")

	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*model.Table)
	{
		var s = `{
			"channelId": "1",
			"id": "test@gmail.com",
			"lastSeen": 123,
			"expiresAt": 200
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}
	{
		var s = `{
			"channelId": "1",
			"id": "xyz@gmail.com",
			"lastSeen": 123,
			"expiresAt": 201
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
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
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}

	{
		var s = `{
			"channelId": "1",
			"id": "red@gmail.com",
			"lastSeen": 144,
			"expiresAt": 210
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}

	fmt.Println(table.Get("1", "test@gmail.com"))
	fmt.Println(table.Index("guest").Get("1", "123"))
	fmt.Println(table.Index("guest").Get("1", "144"))
	fmt.Println(table.Delete("1", "aaa@gmail.com"))
	fmt.Println(table.Index("guest").Get("1", "123"))

	{
		var s = `{
			"channelId": "1",
			"id": "red@gmail.com",
			"lastSeen": 100,
			"expiresAt": 9999999999999
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}

	fmt.Println(table.Index("guest").Get("1", "123"))
	fmt.Println(table.Index("guest").Get("1", "100"))

	fmt.Println("expire keeper tree")
	fmt.Println(bingo.Keeper.String())
	bingo.Keeper.Expire()
	fmt.Println(bingo.Keeper.String())
	fmt.Println(table.Index("guest").Get("1", "123"))

	fmt.Println("====")

	fmt.Println("expire keeper tree")
	fmt.Println(bingo.Keeper.String())
	bingo.Keeper.Expire()
	fmt.Println(bingo.Keeper.String())
	fmt.Println(table.Index("guest").Get("1", "123"))
}

func TestLoad(t *testing.T) {
	prepare()
}
