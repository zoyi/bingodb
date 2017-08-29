package bingodb

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type Bingo struct {
	tables map[string]*Table
	keeper *Keeper
}

func (bingo *Bingo) TablesArray() []*Table {
	tables := make([]*Table, 0, len(bingo.tables))
	for _, table := range bingo.tables {
		tables = append(tables, table)
	}
	return tables
}

func (bingo *Bingo) Table(name string) (value *Table, ok bool) {
	value, ok = bingo.tables[name]
	return
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
	return &Bingo{tables: make(map[string]*Table), keeper: NewKeeper()}
}

func (bingo *Bingo) setMetrics() {
	for _, source := range bingo.tables {
		if config := source.metricsConfig; config != nil {
			NewMetric(
				source,
				bingo.tables[config.Table],
				config.Ttl,
				config.Interval)
		}
	}
}

