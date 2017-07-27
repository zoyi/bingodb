package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/CrowdSurge/banner"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/api"
	"github.com/zoyi/bingodb/model"
	"strings"
)

var (
	addr   = flag.String("addr", ":8080", "TCP address to listen to")
	config = flag.String("file", "config.yml", "import config file")
)

func initSeedData(bingo *model.Bingo) {
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

func DefaultRouter() *fasthttprouter.Router {
	flag.Parse()

	fmt.Printf("* Loaded configration file..\n")
	bingo := model.Load(*config)

	initSeedData(bingo)

	fmt.Printf("* Preparing resources..\n")
	rs := &api.Resource{
		Db:          bingo,
		AccessToken: "",
	}

	fmt.Printf("* Get manager ready to serve..\n")
	m := api.Manager{
		Whitelist: []string{
			"/health_check",
			"/something",
		},
		Resource: rs,
	}

	router := fasthttprouter.New()
	//example
	//router.GET("/", Index)
	//middleware example
	//router.GET("/get", m.Authenticate(Protect))
	router.GET("/m", api.Logging(m.Get))
	router.GET("/d", api.Logging(m.GetMultiples))
	router.POST("/m", api.Logging(m.Update))
	router.DELETE("/m", api.Logging(m.Delete))

	return router
}

func main() {
	banner.Print("bingodb")
	fmt.Printf("									by ZOYI\n")

	router := DefaultRouter()

	fmt.Printf("* Bingo is ready on %s\n", *addr)
	fasthttp.ListenAndServe(*addr, router.Handler)
}
