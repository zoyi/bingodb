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
	ttl int,
	interval int,
) *TableMetrics {
	metrics := &TableMetrics{
		source:   source,
		output:   output,
		ttl:      time.Second * time.Duration(ttl),
		interval: time.Second * time.Duration(interval)}

	go metrics.run()

	return metrics
}

func (metrics *TableMetrics) put(hash interface{}, skipList *lazyskiplist.SkipList) bool {
	data := Data{}
	hashKey := metrics.source.primaryKey.hashKey.Name
	data[hashKey] = hash
	data["count"] = skipList.Size()
	data["time"] = time.Now().Unix()
	data["expiresAt"] = time.Now().Add(metrics.ttl).Unix()
	metrics.output.Put(&data)
	return true
}

func (metrics *TableMetrics) run() {
	for {
		metrics.source.PrimaryIndex().Range(metrics.put)
		time.Sleep(metrics.interval)
	}
}
