package api

import (
	"flag"
	"fmt"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb"
)

const (
	corsAllowHeaders     = "authorization"
	corsAllowMethods     = "HEAD,GET,POST,PUT,DELETE,OPTIONS,PATCH"
	corsAllowOrigin      = "*"
	corsAllowCredentials = "true"
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

	router.GET("/tables", cors(resource.Tables))
	router.GET("/tables/:table", cors(resource.Get))
	router.GET("/tables/:table/info", cors(resource.TableInfo))
	router.GET("/tables/:table/scan", cors(resource.Scan))
	router.GET("/tables/:table/indices/:index", cors(resource.Get))
	router.GET("/tables/:table/indices/:index/scan", cors(resource.Scan))

	router.PUT("/tables/:table", cors(resource.Put))

	router.DELETE("/tables/:table", cors(resource.Remove))

	return router
}

func cors(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return logging(fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Credentials", corsAllowCredentials)
		ctx.Response.Header.Set("Access-Control-Allow-Headers", corsAllowHeaders)
		ctx.Response.Header.Set("Access-Control-Allow-Methods", corsAllowMethods)
		ctx.Response.Header.Set("Access-Control-Allow-Origin", corsAllowOrigin)
		next(ctx)
	}))
}

func logging(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Logger().Printf("%s at %s\n", ctx.Method(), ctx.Path())
		next(ctx)
	})
}
