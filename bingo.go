package bingodb

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"os"
	"github.com/newrelic/go-agent"
)

type Bingo struct {
	tables        map[string]*Table
	keeper        *Keeper
	systemMetrics *SystemMetrics
	BingoConfig   *BingoConfig
	NrApp         *newrelic.Application
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
	)
}

func (bingo *Bingo) SetNewRelicAgent() {
	e := bingo.BingoConfig.NewRelic["enable"]
	enabled := len(e) > 0 && e == "true"

	a := bingo.BingoConfig.NewRelic["applicationName"]
	var appName string
	if len(a) > 0 {
		appName = a
	} else {
		appName = "develop"
	}

	licenseKey := bingo.BingoConfig.NewRelic["licenseKey"]
	logPath := bingo.BingoConfig.NewRelic["logPath"]

	nrConfig := newrelic.NewConfig(appName, licenseKey)

	// nrApp is enabled when 'enable' is 'true' and 'licenseKey' is not empty
	nrConfig.Enabled = enabled && len(licenseKey) > 0

	// File log is enabled when 'logPath' is not empty
	if nrConfig.Enabled {
		if len(logPath) > 0 {
			w, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if nil == err {
				nrConfig.Logger = newrelic.NewLogger(w)
			}
		} else {
			nrConfig.Logger = newrelic.NewDebugLogger(os.Stdout)
		}
	}

	// Create agent
	if nrApp, err := newrelic.NewApplication(nrConfig); nrConfig.Enabled && err != nil {
		log.Fatalln("NewRelic set enabled but error occurred:", err)
	} else {
		bingo.NrApp = &nrApp
	}

	if nrConfig.Enabled {
		fmt.Printf("* NewRelic agent enabled\n\t- Agent version: %s\n\t- App name: %s\n\t- Log path: %s\n",
			newrelic.Version, appName, logPath)
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
