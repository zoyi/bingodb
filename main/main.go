package main

import (
)
import (
	"bingodb/model"
	"fmt"
	"encoding/json"
	"strings"
)

func main() {

	v := model.Load("/Users/red/go/src/bingodb/config.yml")

	fmt.Println(v.Tables["onlines"])

	table := v.Tables["onlines"]

	fmt.Println(v.Tables["onlines"])

	{
		var s = `{
			"channelId": "1",
			"id": "test@gmail.com",
			"lastSeen": 123
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
			"lastSeen": 123
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
			"lastSeen": 123
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
			"lastSeen": 144
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data model.Data
		dec.Decode(&data)
		table.Put(&data)
	}


	fmt.Println(v.Tables["onlines"].Get("1", "test@gmail.com"))

	fmt.Println(v.Tables["onlines"].Index("guest").Get("1", "123"))
	fmt.Println(v.Tables["onlines"].Index("guest").Get("1", "144"))

	fmt.Println(v.Tables["onlines"].Delete("1", "aaa@gmail.com"))

	fmt.Println(v.Tables["onlines"].Index("guest").Get("1", "123"))
}
