package model

import (
	"fmt"
	"github.com/zoyi/bingodb/ds/redblacktree"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type Bingo struct {
	Tables map[string]*Table
	Keeper *Keeper
}

type tableInfo struct {
	Fields     map[string]string    `yaml:"fields"`
	HashKey    string               `yaml:"hashKey"`
	RangeKey   string               `yaml:"rangeKey"`
	SubIndices map[string]indexInfo `yaml:"subIndices"`
	ExpireKey  string               `yaml:"expireKey"`
}

type indexInfo struct {
	HashKey  string `yaml:"hashKey"`
	RangeKey string `yaml:"rangeKey"`
}

type configInfo struct {
	Tables map[string]tableInfo `yaml:"tables"`
}

func Load(filename string) *Bingo {
	projectPath := filepath.Join(os.Getenv("GOPATH"), "/src/github.com/zoyi/bingodb/")
	absPath, _ := filepath.Abs(filepath.Join(projectPath, "/config", filename))
	source, err := ioutil.ReadFile(absPath)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("error: %v", err)
	}

	configInfo := configInfo{}

	err = yaml.Unmarshal(source, &configInfo)

	if err != nil {
		fmt.Println(err)
		log.Fatalf("error: %v", err)
	}

	return parse(&configInfo)
}

func newBingo() *Bingo {
	return &Bingo{Tables: make(map[string]*Table), Keeper: newKeeper()}
}

func parse(configInfo *configInfo) *Bingo {
	config := newBingo()

	for tableName, tableInfo := range configInfo.Tables {
		fields := make(map[string]*FieldSchema)
		for fieldKey, fieldType := range tableInfo.Fields {
			field := &FieldSchema{Name: fieldKey, Type: fieldType}
			fields[fieldKey] = field
		}

		primaryKey := &Key{
			HashKey: fields[tableInfo.HashKey],
			SortKey: fields[tableInfo.RangeKey]}

		schema := &TableSchema{
			Fields:       fields,
			PrimaryKey:   primaryKey,
			SubIndexKeys: make(map[string]*Key),
			ExpireField:  fields[tableInfo.ExpireKey]}

		primaryIndex := &PrimaryIndex{&Index{
			Data: make(map[interface{}]*redblacktree.Tree),
			Key:  primaryKey}}

		subIndices := make(map[string]*SubIndex)
		for indexName, indexInfo := range tableInfo.SubIndices {
			subKey := &Key{HashKey: fields[indexInfo.HashKey],
				SortKey: fields[indexInfo.RangeKey]}

			schema.SubIndexKeys[indexName] = subKey

			subIndices[indexName] = &SubIndex{&Index{
				Data: make(map[interface{}]*redblacktree.Tree),
				Key:  subKey}}
		}

		config.AddTable(tableName, schema, primaryIndex, subIndices)

	}

	return config
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
