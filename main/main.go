package main

import (
	"flag"
	"fmt"
	"github.com/CrowdSurge/banner"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/api"
)

var (
	addr   = flag.String("addr", ":8090", "TCP address to listen to")
	config = flag.String("file", "config.yml", "import config file")
)

func main() {
	banner.Print("bingodb")
	fmt.Printf("									by ZOYI\n")

	router := api.DefaultRouter(*config)

	fmt.Printf("* Bingo is ready on %s\n", *addr)

	s := &fasthttp.Server {
		Handler: router.Handler,
		Name: "bingodb",
	}

	s.ListenAndServe(*addr)
}
