package bingodb

import (
	"fmt"
	"log"
	"sync/atomic"
)

type Bingo struct {
	tables        map[string]*Table
	keeper        *Keeper
	systemMetrics *SystemMetrics
}

const (
	FieldError = "Field '%s' is defined as %s type but value '%v' cannot be parsed"
	SetOrInsertMissing = "set or setOnInsert are required"
	HashKeyMissing = "Hash key is missing in set"
	SortKeyMising = "Hash key is missing in set"
	DocumentNotFound = "Document not found"
	IndexNotFound = "Index not found"
)

func (bingo *Bingo) TablesArray() []*TableInfo {
	tables := make([]*TableInfo, 0, len(bingo.tables))
	for _, table := range bingo.tables {
		tables = append(tables, table.Info())
	}
	return tables
}

func (bingo *Bingo) Table(name string) (value *Table, ok bool) {
	value, ok = bingo.tables[name]
	return
}

func NewBingoFromConfigFile(configFile string) *Bingo {
	bingo := newBingo()
	err := ParseConfig(bingo, configFile)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("error: %v", err)
	}
	return bingo
}

func newBingo() *Bingo {
	bingo := &Bingo{tables: make(map[string]*Table)}
	bingo.keeper = NewKeeper(bingo)
	bingo.systemMetrics = NewSystemMetrics(bingo)
	bingo.Start()
	return bingo
}

func (bingo *Bingo) Start() {
	bingo.keeper.start()
	bingo.systemMetrics.start()
}

func (bingo *Bingo) Stop() {
	bingo.keeper.stop()
	bingo.systemMetrics.stop()
}

func (bingo *Bingo) setTableMetrics() {
	for _, source := range bingo.tables {
		if config := source.metricsConfig; config != nil {
			NewTableMetrics(
				source,
				bingo.tables[config.Table],
				config.Ttl,
				config.Interval)
		}
	}
}

func (bingo *Bingo) AddScan() {
	atomic.AddInt64(&bingo.systemMetrics.scan, 1)
}

func (bingo *Bingo) AddGet() {
	atomic.AddInt64(&bingo.systemMetrics.get, 1)
}

func (bingo *Bingo) AddPut() {
	atomic.AddInt64(&bingo.systemMetrics.put, 1)
}

func (bingo *Bingo) AddRemove() {
	atomic.AddInt64(&bingo.systemMetrics.remove, 1)
}

func (bingo *Bingo) AddExpire(i int64) {
	atomic.AddInt64(&bingo.systemMetrics.expire, i)
}
