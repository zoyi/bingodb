package model

import (
	"encoding/json"
	"strings"
)

func InitDefaultSeedData(bingo *Bingo) {
	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*Table)
	{
		var s = `{
			"channelId": "1",
			"id": "test",
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
			"id": "what",
			"lastSeen": 129,
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
			"id": "ddd",
			"lastSeen": 132,
			"expiresAt": 201
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data Data
		dec.Decode(&data)
		table.Put(&data)
	}
}
