package mock

import (
	"encoding/json"
	"strings"

	"github.com/zoyi/bingodb/model"
)

func InitDefaultSeedData(bingo *model.Bingo) {
	tableData, _ := bingo.Tables.Load("onlines")
	table := tableData.(*model.Table)
	{
		var s = `{
			"channelId": "1",
			"id": "test",
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
			"id": "what",
			"lastSeen": 129,
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
			"id": "ddd",
			"lastSeen": 132,
			"expiresAt": 201
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}
}
