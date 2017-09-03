package api

import (
	"flag"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb"
)

type Manager struct {
	*Resource
}

func Test(ctx *fasthttp.RequestCtx) {
	fmt.Fprint(ctx, "Welcome!\n")
}

func MakeRouter(bingo *bingodb.Bingo) *fasthttprouter.Router {
	flag.Parse()

	fmt.Printf("* Preparing resources..\n")
	resource := &Resource{
		bingo: bingo}

	router := fasthttprouter.New()

	router.GET("/tables", Logging(resource.Tables))
	router.GET("/tables/:table", Logging(resource.Get))
	router.GET("/tables/:table/info", Logging(resource.TableInfo))
	router.GET("/tables/:table/scan", Logging(resource.Scan))
	router.GET("/tables/:table/indices/:index", Logging(resource.Get))

	router.PUT("/tables/:table", Logging(resource.Put))

	router.DELETE("/tables/:table", Logging(resource.Remove))

	return router
}

// Logging ... simple logging middleware
func Logging(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		// ctx.Logger().Printf("%s at %s\n", ctx.Method(), ctx.Path())
		h(ctx)
	})
}
