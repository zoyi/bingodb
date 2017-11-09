package api

import "github.com/valyala/fasthttp"

type Middleware func(next fasthttp.RequestHandler) fasthttp.RequestHandler

//func (server *BingoServer) setMiddleware(config *BingoConfig) {
//	bingo.middleware = make([]Middleware, 0)
//
//	if config.Logging {
//		bingo.AddMiddleware(logging)
//	}
//}

func (server *BingoServer) AddMiddleware(middleware Middleware) {
	server.middleware = append(server.middleware, middleware)
}

func logging(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Logger().Printf("%s at %s\n", ctx.Method(), ctx.Path())
		next(ctx)
	})
}

func cors(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Credentials", corsAllowCredentials)
		ctx.Response.Header.Set("Access-Control-Allow-Headers", corsAllowHeaders)
		ctx.Response.Header.Set("Access-Control-Allow-Methods", corsAllowMethods)
		ctx.Response.Header.Set("Access-Control-Allow-Origin", corsAllowOrigin)
		next(ctx)
	})
}
