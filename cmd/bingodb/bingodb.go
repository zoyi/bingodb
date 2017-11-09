package main

import (
	"flag"
	"github.com/zoyi/bingodb"
	"github.com/zoyi/bingodb/api"
)

func main() {
	config := flag.String("config", "config/development.yml", "Config file")
	flag.Parse()

	bingo := bingodb.NewBingoFromConfigFile(*config)

	server := api.NewBingoServer(bingo, nil)
	server.Run()
}
