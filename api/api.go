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
func (manager *Manager) Authenticate(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return Logging(fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		//skip whitelist
		for _, path := range manager.Whitelist {
			if string(ctx.Path()) == path {
				h(ctx)
				return
			}
		}

		// TBD
		isValid := validateAuthToken(ctx)

		if isValid {
			h(ctx)
			return
		}
		// otherwise error
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)
	}))
}

func (m *Manager) Get(ctx *fasthttp.RequestCtx) {
	tableName, hashKey, sortKey, valid := m.Resource.ValidateGetParams(ctx)
	//validate if any of param is not present
	if !valid {
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusBadRequest), fasthttp.StatusBadRequest)
		return
	}

	data := m.Resource.Get(tableName, hashKey, sortKey)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write(data)
}

func (m *Manager) GetMultiples(ctx *fasthttp.RequestCtx) {
	params, valid := m.Resource.ValidateGetListParams(ctx)
	if !valid {
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusBadRequest), fasthttp.StatusBadRequest)
		return
	}

	data := m.Resource.GetMultiple(params)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write(data)
}

func (manager *Manager) Update(ctx *fasthttp.RequestCtx) {
	//get post data
	//json body = { }
	//validate body
	//insert into bingodb
	//return result code
}

func (manager *Manager) Delete(ctx *fasthttp.RequestCtx) {
	//get post data
	//delete from bingodb
	//return result code
}
