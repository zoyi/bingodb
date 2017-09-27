package bingodb

import (
	"fmt"
	"github.com/robfig/cron"
	"time"
)

type SystemMetrics struct {
	bingo  *Bingo
	scan   int64
	get    int64
	put    int64
	remove int64
	expire int64
	cron   *cron.Cron
}

func NewSystemMetrics(bingo *Bingo) *SystemMetrics {
	systemMetrics := &SystemMetrics{bingo: bingo}
	c := cron.New()
	c.AddFunc("0 * * * * *", systemMetrics.dump)
	systemMetrics.cron = c
	return systemMetrics
}

func (metrics *SystemMetrics) start() {
	metrics.cron.Start()
}

func (metrics *SystemMetrics) stop() {
	metrics.cron.Stop()
}

func (metrics *SystemMetrics) dump() {
	scan := metrics.scan
	get := metrics.get
	put := metrics.put
	remove := metrics.remove
	expire := metrics.expire

	metrics.output("scan", scan)
	metrics.output("get", get)
	metrics.output("put", put)
	metrics.output("remove", remove)
	metrics.output("expire", expire)

	for _, table := range metrics.bingo.tables {
		metrics.output(fmt.Sprintf("#%s", table.name), table.primaryIndex.size)
	}

	metrics.scan = 0
	metrics.get = 0
	metrics.put = 0
	metrics.remove = 0
	metrics.expire = 0
}

func (metrics *SystemMetrics) output(key string, value int64) {
	if table, ok := metrics.bingo.tables["_metrics"]; ok {
		data := Data{}
		data["key"] = key
		data["value"] = value
		data["time"] = time.Now().Unix() * 1000
		data["expireAt"] = time.Now().Add(time.Hour*3).Unix() * 1000
		table.Put(&data, nil)
	}
}
