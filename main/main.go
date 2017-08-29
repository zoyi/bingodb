package main

import (
	"flag"
	"fmt"
	"github.com/CrowdSurge/banner"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/api"
	"github.com/zoyi/bingodb"
)

var (
	addr   = flag.String("addr", ":8080", "TCP address to listen to")
	config = flag.String("file", "config.yml", "import config file")
)

func main() {
	banner.Print("bingodb")
	fmt.Printf("									by ZOYI\n")

	fmt.Printf("* Loaded configration file..\n")
	bingo := bingodb.Load(*config)

	router := api.MakeRouter(bingo)

	fmt.Printf("* bingo is ready on %s\n", *addr)
	fasthttp.ListenAndServe(*addr, router.Handler)
}
