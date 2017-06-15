package model

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"fmt"
	"log"
	"bingodb/ds/redblacktree"
)

type Config struct {
	Tables map[string]*Table
}

type tableInfo struct {
	Fields map[string]string `yaml:"fields"`
	HashKey string `yaml:"hashKey"`
	RangeKey string `yaml:"rangeKey"`
	SubIndices map[string]indexInfo `yaml:"subIndices"`
}

type indexInfo struct {
	HashKey string `yaml:"hashKey"`
	RangeKey string `yaml:"rangeKey"`
}

type configInfo struct {
	Tables map[string]tableInfo `yaml:"tables"`
}

func Load(filename string) *Config {
	source,_ := ioutil.ReadFile(filename)

	configInfo := configInfo{}

	err := yaml.Unmarshal(source, &configInfo)

	if err != nil {
		fmt.Println(err)
		log.Fatalf("error: %v", err)
	}

	return parse(&configInfo)
}

func parse(configInfo *configInfo) *Config {
	tables := make(map[string]*Table)

	for tableName, tableInfo := range configInfo.Tables {
		fields := make(map[string]FieldSchema)
		for fieldKey, fieldType := range tableInfo.Fields {
			field := FieldSchema{Name: fieldKey, Type: fieldType}
			fields[fieldKey] = field
		}

		primaryKey := &Key{HashKey: fields[tableInfo.HashKey],
			SortKey: fields[tableInfo.RangeKey]}

		schema := &TableSchema{
			Fields:   fields,
			PrimaryKey: primaryKey,
			SubIndexKeys: make(map[string]*Key)}

		primaryIndex := &PrimaryIndex{&Index{
			Data: make(map[interface{}]*redblacktree.Tree),
			Key: primaryKey}}

		subIndices := make(map[string]*SubIndex)
		for indexName, indexInfo := range tableInfo.SubIndices {
			subKey := &Key{HashKey: fields[indexInfo.HashKey],
				SortKey: fields[indexInfo.RangeKey]}

			schema.SubIndexKeys[indexName] = subKey

			subIndices[indexName] = &SubIndex{&Index{
				Data: make(map[interface{}]*redblacktree.Tree),
				Key: subKey}}
		}

		tables[tableName] = &Table{
			Schema:     schema,
			PrimaryIndex:  primaryIndex,
			SubIndices: subIndices}

	}

	return &Config{Tables: tables}
}