package api

import (
	"fmt"
	"github.com/CrowdSurge/banner"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/zoyi/bingodb"
	"log"
	"net/http"
	"time"
)

type BingoServer struct {
	bingo  *bingodb.Bingo
	config *bingodb.ServerConfig
	engine *gin.Engine
}

func NewBingoServer(bingo *bingodb.Bingo, middleware ...gin.HandlerFunc) *BingoServer {
	engine := gin.New()

	gin.SetMode(bingo.ServerConfig.Mode)

	engine.NoMethod(notFound())
	engine.NoRoute(notFound())

	engine.Use(cors.Default())
	engine.Use(gin.Recovery())
	engine.Use(errorHandler())

	if bingo.ServerConfig.Logging {
		engine.Use(gin.Logger())
	}

	if middleware != nil {
		engine.Use(middleware...)
	}

	fmt.Printf("* Preparing resources..\n")
	resource := &Resource{bingo: bingo}

	engine.GET("/tables", resource.Tables)
	engine.GET("/tables/:table", resource.Get)
	engine.GET("/tables/:table/info", resource.TableInfo)
	engine.GET("/tables/:table/scan", resource.Scan)
	engine.GET("/tables/:table/indices/:index", resource.Get)
	engine.GET("/tables/:table/indices/:index/scan", resource.Scan)

	engine.PUT("/tables/:table", resource.Put)

	engine.DELETE("/tables/:table", resource.Remove)

	time.Sleep(50000000)

	return &BingoServer{
		bingo:  bingo,
		config: bingo.ServerConfig,
		engine: engine,
	}
}

func (server *BingoServer) Run() {
	banner.Print("bingodb")
	fmt.Printf("* bingo is ready on %s\n", server.config.Addr)
	log.Fatal(http.ListenAndServe(server.config.Addr, server.engine))
}

func errorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 {
			c.JSON(http.StatusUnprocessableEntity, c.Errors)
		}
	}
}

func notFound() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"error": "page not found"})
	}
}
