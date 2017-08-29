package bingodb

import (
	"time"
	"github.com/zoyi/skiplist/lazy"
)

type Metrics struct {
	source *Table
	output *Table
	ttl time.Duration
	interval time.Duration
}

func NewMetric(
	source *Table,
	output *Table,
	ttl int,
	interval int,
) *Metrics {
	metrics := &Metrics{
		source: source,
		output: output,
		ttl: time.Second * time.Duration(ttl),
		interval: time.Second * time.Duration(interval)}

	go metrics.run()

	return metrics
}

func (metrics *Metrics) put(hash interface{}, skipList *lazyskiplist.SkipList) bool {
	data := Data{}
	hashKey := metrics.source.hashKey.Name
	data[hashKey] = hash
	data["count"] = skipList.Size()
	data["time"] = time.Now().Unix()
	data["expiresAt"] = time.Now().Add(metrics.ttl).Unix()
	metrics.output.Put(&data)
	return true
}

func (metrics *Metrics) run() {
	for {
		metrics.source.PrimaryIndex().Range(metrics.put)
		time.Sleep(metrics.interval)
	}
}
