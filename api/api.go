package api

import (
	"encoding/json"
	"flag"
	"fmt"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/mock"
	"github.com/zoyi/bingodb/model"
)

type Manager struct {
	//Whitelist []string
	*Resource
}

func DefaultRouter(configFileName string) *fasthttprouter.Router {
	flag.Parse()

	fmt.Printf("* Loaded configration file..\n")
	bingo := model.Load(configFileName)

	//for test purpose
	mock.InitDefaultSeedData(bingo)

	fmt.Printf("* Preparing resources..\n")
	rs := &Resource{
		Db:          bingo,
		AccessToken: "",
	}

	fmt.Printf("* Get manager ready to serve..\n")
	m := Manager{
		//Whitelist: []string{
		//	"/health_check",
		//	"/something",
		//},
		Resource: rs,
	}

	router := fasthttprouter.New()

	router.GET("/get/:tableName", Logging(m.Get))
	router.GET("/gets/:tableName", Logging(m.GetMultiples))
	router.POST("/update/:tableName", Logging(m.Update))
	router.DELETE("/delete/:tableName", Logging(m.Delete))

	return router
}

// Logging ... simple logging middleware
func Logging(h fasthttp.RequestHandler) fasthttp.RequestHandler {
	return fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Logger().Printf("%s at %s\n", ctx.Method(), ctx.Path())
		h(ctx)
	})
}

// Authenticate ... check if auth token is valid
//func (m *Manager) Authenticate(h fasthttp.RequestHandler) fasthttp.RequestHandler {
//	return Logging(fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
//		//skip whitelist
//		for _, path := range m.Whitelist {
//			if string(ctx.Path()) == path {
//				h(ctx)
//				return
//			}
//		}
//
//		// TBD
//		isValid := validateAuthToken(ctx)
//
//		if isValid {
//			h(ctx)
//			return
//		}
//		// otherwise error
//		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusUnauthorized), fasthttp.StatusUnauthorized)
//	}))
//}

func (m *Manager) Get(ctx *fasthttp.RequestCtx) {
	tableName, hashKey, sortKey, valid := m.Resource.ValidateParams(ctx)
	if !valid {
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusBadRequest), fasthttp.StatusBadRequest)
		return
	}

	data := m.Resource.Get(tableName, hashKey, sortKey)

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write(data)

	var response model.Data
	json.Unmarshal(data, &response)
	ctx.Logger().Printf("response: %v\n", response)
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

	var response []model.Data
	json.Unmarshal(data, &response)
	ctx.Logger().Printf("response: %v\n", response)
}

func (m *Manager) Update(ctx *fasthttp.RequestCtx) {
	tableName, body, valid := m.Resource.ValidatePostParams(ctx)
	if !valid {
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusBadRequest), fasthttp.StatusBadRequest)
		return
	}

	data := m.Resource.Update(tableName, &body)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Write(data)

	var response model.Data
	json.Unmarshal(data, &response)
	ctx.Logger().Printf("response: %v\n", response)
}

func (m *Manager) Delete(ctx *fasthttp.RequestCtx) {
	tableName, hashKey, sortKey, valid := m.Resource.ValidateParams(ctx)
	if !valid {
		ctx.Error(fasthttp.StatusMessage(fasthttp.StatusBadRequest), fasthttp.StatusBadRequest)
		return
	}

	m.Resource.Delete(tableName, hashKey, sortKey)

	ctx.SetStatusCode(fasthttp.StatusOK)
}
