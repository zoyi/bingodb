package main

import (
	"flag"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/api"
	"github.com/zoyi/bingodb/model"
)

var (
	addr   = flag.String("addr", ":8080", "TCP address to listen to")
	config = flag.String("file", "config.yml", "import config file")
)

// examples
func Index(ctx *fasthttp.RequestCtx) {
	fmt.Fprint(ctx, "Not protected!\n")
}

func Protect(ctx *fasthttp.RequestCtx) {
	fmt.Fprint(ctx, "Not protected!\n")
}

func main() {
	bingo := model.Load(*config)

	flag.Parse()

	rs := &api.Resource{
		BingoDB:     bingo,
		AccessToken: "",
	}

	m := api.Manager{
		Whitelist: []string{
			"/health_check",
			"/something",
		},
		Resource: rs,
	}

	router := fasthttprouter.New()
	//example
	router.GET("/", Index)
	//middleware example
	router.GET("/get", api.Authenticate(Protect))
	router.GET("/m", m.Get)
	router.GET("/d", m.GetMultiples)
	router.POST("/m", m.Update)
	router.DELETE("/m", m.Delete)

	fasthttp.ListenAndServe(*addr, router.Handler)
}
