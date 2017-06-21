package main

import (
	"encoding/json"
	"fmt"
	"github.com/zoyi/bingodb/model"
	"strings"
	"testing"
)

var config *model.Bingo

func prepare() {
	config = model.Load("test_config.yml")

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

	fmt.Println(config.Tables["onlines"].Get("1", "test@gmail.com"))

	fmt.Println(config.Tables["onlines"].Index("guest").Get("1", "123"))
	fmt.Println(config.Tables["onlines"].Index("guest").Get("1", "144"))

	fmt.Println(config.Tables["onlines"].Delete("1", "aaa@gmail.com"))

	fmt.Println(config.Tables["onlines"].Index("guest").Get("1", "123"))

	{
		var s = `{
			"channelId": "1",
			"id": "red@gmail.com",
			"lastSeen": 100,
			"expiresAt": 200
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}

	fmt.Println(config.Tables["onlines"].Index("guest").Get("1", "123"))
	fmt.Println(config.Tables["onlines"].Index("guest").Get("1", "100"))
}

func TestLoad(t *testing.T) {
	prepare()
}
