package model

import (
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Bingo struct {
	Tables *sync.Map
	Keeper *Keeper
}

func Load(filename string) (*Bingo, error) {
	projectPath := filepath.Join(os.Getenv("GOPATH"), "/src/github.com/zoyi/bingodb/")
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", filename))

	bingo := newBingo()
	err := ParseConfig(bingo, absPath)
	if err != nil {
		log.Fatalf("error: %v", err)
		return nil, err
	}
	return bingo, nil
}

func newBingo() *Bingo {
	return &Bingo{Tables: new(sync.Map), Keeper: newKeeper()}
}

func (bingo *Bingo) AddTable(
	tableName string,
	schema *TableSchema,
	primaryIndex *PrimaryIndex,
	subIndices *sync.Map) {

	bingo.Tables.Store(tableName, &Table{
		Bingo:        bingo,
		Name:         tableName,
		Schema:       schema,
		PrimaryIndex: primaryIndex,
		SubIndices:   subIndices})
}
