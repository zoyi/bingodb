package model

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Bingo struct {
	Tables   *sync.Map
	Keeper   *Keeper
	Adaptors Adaptors
}

func Load(filename string) *Bingo {
	projectPath := filepath.Join(os.Getenv("GOPATH"), "/src/github.com/zoyi/bingodb/")
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", filename))

	bingo := newBingo()
	err := ParseConfig(bingo, absPath)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("error: %v", err)
	}
	return bingo
}

func newBingo() *Bingo {
	return &Bingo{Tables: new(sync.Map), Keeper: newKeeper()}
}

func (bingo *Bingo) addTable(
	tableName string,
	schema *TableSchema,
	primaryIndex *PrimaryIndex,
	subIndices *sync.Map,
	stream *Stream) {

	bingo.Tables.Store(tableName, &Table{
		Bingo:        bingo,
		Name:         tableName,
		Schema:       schema,
		PrimaryIndex: primaryIndex,
		SubIndices:   subIndices,
		Stream:       stream})
}
