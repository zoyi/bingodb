package main

import (
	"flag"
	"fmt"
	"github.com/CrowdSurge/banner"
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

func DefaultServer() *fasthttprouter.Router {
	flag.Parse()

	fmt.Printf("* Loaded configration file..\n")
	bingo := model.Load(*config)

	fmt.Printf("* Preparing resources..\n")
	rs := &api.Resource{
		BingoDB:     bingo,
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
	router.GET("/", Index)
	//middleware example
	router.GET("/get", m.Authenticate(Protect))
	router.GET("/m", m.Get)
	router.GET("/d", m.GetMultiples)
	router.POST("/m", m.Update)
	router.DELETE("/m", m.Delete)

	return router
}

func main() {
	banner.Print("bingodb")
	fmt.Printf("									by ZOYI\n")

	router := DefaultServer()

	fmt.Printf("* Ready to serve on %s\n", *addr)
	fasthttp.ListenAndServe(*addr, router.Handler)
}
