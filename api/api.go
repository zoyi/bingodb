package api

import (
	"github.com/valyala/fasthttp"
)

type Manager struct {
	Whitelist []string
	*Resource
}

func validateAuthToken(ctx *fasthttp.RequestCtx) bool {
	auth := ctx.Request.Header.Peek("Authorization")
	if auth == nil {
		return false
	}
	//validate auth token here
	return true
}

// Logging ... simple logging middleware
func Logging(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Logger().Printf("%s at %s\n", ctx.Method(), ctx.Path())
		h(ctx)
	})
}

// Authenticate ... check if auth token is valid
func Authenticate(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return Logging(fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		// Get the Basic Authentication credentials
		isValid := validateAuthToken(ctx)

		if isValid {
			h(ctx)
			return
		}
		// otherwise error
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)
	}))
}

func (manager *Manager) Get(ctx *fasthttp.RequestCtx) {
	//get request params
	//look up in bingodb
	//table, ok := manager.BingoDB.Tables.Load("somekey")
	//return value
}

func (manager *Manager) GetMultiples(ctx *fasthttp.RequestCtx) {
	//get request params
	//look up in bingodb
	//return values
}

func (manager *Manager) Update(ctx *fasthttp.RequestCtx) {
	//get post data
	//insert into bingodb
	//return result code
}

func (manager *Manager) Delete(ctx *fasthttp.RequestCtx) {
	//get post data
	//delete from bingodb
	//return result code
}
