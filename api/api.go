package api

import (
	"fmt"
	"github.com/CrowdSurge/banner"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb"
	"log"
)

const (
	corsAllowHeaders     = "authorization"
	corsAllowMethods     = "HEAD,GET,POST,PUT,DELETE,OPTIONS,PATCH"
	corsAllowOrigin      = "*"
	corsAllowCredentials = "true"
)

type BingoServer struct {
	bingo      *bingodb.Bingo
	config     *bingodb.ServerConfig
	router     *fasthttprouter.Router
	middleware []Middleware
}

func NewBingoServer(bingo *bingodb.Bingo, middleware []Middleware) *BingoServer {
	server := &BingoServer{
		bingo:      bingo,
		config:     bingo.ServerConfig,
		router:     fasthttprouter.New(),
		middleware: middleware,
	}
	server.initRouter()

	return server
}

func (server *BingoServer) Run() {
	banner.Print("bingodb")
	fmt.Printf("* bingo is ready on %s\n", server.config.Addr)
	log.Fatal(fasthttp.ListenAndServe(server.config.Addr, server.router.Handler))
}

func (server *BingoServer) initRouter() {
	fmt.Printf("* Preparing resources..\n")
	resource := &Resource{bingo: server.bingo}

	server.router.GET("/tables", server.handler(resource.Tables))
	server.router.GET("/tables/:table", server.handler(resource.Get))
	server.router.GET("/tables/:table/info", server.handler(resource.TableInfo))
	server.router.GET("/tables/:table/scan", server.handler(resource.Scan))
	server.router.GET("/tables/:table/indices/:index", server.handler(resource.Get))
	server.router.GET("/tables/:table/indices/:index/scan", server.handler(resource.Scan))

	server.router.PUT("/tables/:table", server.handler(resource.Put))

	server.router.DELETE("/tables/:table", server.handler(resource.Remove))
}

func (server *BingoServer) handler(handler fasthttp.RequestHandler) fasthttp.RequestHandler {
	if server.config.Logging {
		handler = logging(handler)
	}

	handler = cors(handler)

	if server.middleware != nil {
		for i := range server.middleware {
			handler = server.middleware[i](handler)
		}
	}

	return handler
}
