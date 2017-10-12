package bingodb

import (
	"github.com/zoyi/skiplist/lazy"
	"time"
)

type TableMetrics struct {
	source   *Table
	output   *Table
	ttl      time.Duration
	interval time.Duration
}

func NewTableMetrics(
	source *Table,
	output *Table,
	ttl int64,
	interval int64,
) *TableMetrics {
	metrics := &TableMetrics{
		source:   source,
		output:   output,
		ttl:      time.Millisecond * time.Duration(ttl),
		interval: time.Millisecond * time.Duration(interval)}

	go metrics.run()

	return metrics
}

func (metrics *TableMetrics) put(hash interface{}, skipList *lazyskiplist.SkipList) bool {
	data := Data{}
	hashKey := metrics.source.primaryKey.hashKey.Name
	data[hashKey] = hash
	data[metrics.source.metricsConfig.Count] = skipList.Size()
	data[metrics.source.metricsConfig.Time] = time.Now().Unix() * 1000
	data[metrics.source.metricsConfig.ExpireKey] = time.Now().Add(metrics.ttl).Unix() * 1000
	metrics.output.Put(&data, nil)
	return true
}

func (metrics *TableMetrics) run() {
	for {
		metrics.source.PrimaryIndex().Range(metrics.put)
		time.Sleep(metrics.interval)
	}
}
