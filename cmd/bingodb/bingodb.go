package main

import (
	"flag"
	"fmt"
	"github.com/CrowdSurge/banner"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb"
	"github.com/zoyi/bingodb/api"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP address to listen to")
	config := flag.String("config", "config/development.yml", "Config file")
	flag.Parse()

	banner.Print("bingodb")

	fmt.Printf("* Loading configration file... %s %s\n", *config, *addr)
	bingo := bingodb.NewBingoFromConfigFile(*config)
	bingo.SetNewRelicAgent()

	router := api.MakeRouter(bingo)

	fmt.Printf("* bingo is ready on %s\n", *addr)
	fasthttp.ListenAndServe(*addr, router.Handler)
}
