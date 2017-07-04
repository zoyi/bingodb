package model

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type Bingo struct {
	Tables map[string]*Table
	Keeper *Keeper
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
	return &Bingo{Tables: make(map[string]*Table), Keeper: newKeeper()}
}

func (bingo *Bingo) AddTable(
	tableName string,
	schema *TableSchema,
	primaryIndex *PrimaryIndex,
	subIndices map[string]*SubIndex) {

	bingo.Tables[tableName] = &Table{
		Bingo:        bingo,
		Name:         tableName,
		Schema:       schema,
		PrimaryIndex: primaryIndex,
		SubIndices:   subIndices}
}
