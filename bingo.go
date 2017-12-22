package bingodb

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
)

type Bingo struct {
	tables        map[string]*Table
	keeper        *Keeper
	systemMetrics *SystemMetrics
	ServerConfig  *ServerConfig
}

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
	bingo.Start()
	return bingo
}

func newBingo() *Bingo {
	bingo := &Bingo{tables: make(map[string]*Table)}
	bingo.keeper = NewKeeper(bingo)
	bingo.systemMetrics = NewSystemMetrics(bingo)
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
		if config := source.metricsConfig; !strings.HasPrefix(source.name, "_") && config != nil {
			NewTableMetrics(
				source,
				source.makeMetricsTable(),
				config.Ttl,
				config.Interval)
		}
	}

	// Make global system metrics
	metricsFields := make(map[string]*FieldSchema)
	metricsFields["key"] = &FieldSchema{Name: "key", Type: "string"}
	metricsFields["value"] = &FieldSchema{Name: "value", Type: "integer"}
	metricsFields["time"] = &FieldSchema{Name: "time", Type: "integer"}
	metricsFields["expireAt"] = &FieldSchema{Name: "expireAt", Type: "integer"}

	metricsPrimaryKey := &Key{hashKey: metricsFields["key"], sortKey: metricsFields["time"]}

	bingo.tables["_metrics"] = newTable(
		bingo,
		"_metrics",
		&TableSchema{
			fields:      metricsFields,
			primaryKey:  metricsPrimaryKey,
			expireField: metricsFields["expireAt"],
		},
		&PrimaryIndex{index: newIndex(metricsPrimaryKey)},
		make(map[string]*SubIndex),
		nil,
		true,
	)
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

func (bingo *Bingo) KeeperSize() int64 {
	return bingo.keeper.list.Size()
}
