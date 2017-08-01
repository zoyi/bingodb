package model

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"reflect"
	"sync"
)

type TableInfo struct {
	Fields     map[string]string    `yaml:"fields"`
	HashKey    string               `yaml:"hashKey"`
	SortKey    string               `yaml:"sortKey"`
	SubIndices map[string]IndexInfo `yaml:"subIndices"`
	ExpireKey  string               `yaml:"expireKey"`
}

type IndexInfo struct {
	HashKey string `yaml:"hashKey"`
	SortKey string `yaml:"sortKey"`
}

type ConfigInfo struct {
	Tables map[string]TableInfo `yaml:"tables"`
}

// Load configuration file with specified path and
// parse it to create table schema to prepare Bingo.
// It returns any error encountered.
func ParseConfig(bingo *Bingo, path string) error {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return ParseConfigBytes(bingo, configBytes)
}

// Parse specified string of configuration and
// create table schema to prepare Bingo.
// It returns any error encountered.
func ParseConfigString(bingo *Bingo, configString string) error {
	return ParseConfigBytes(bingo, []byte(configString))
}

// Parse specified bytes of configuration and
// create table schema to prepare Bingo.
// It returns any error encountered.
func ParseConfigBytes(bingo *Bingo, config []byte) error {
	configInfo := &ConfigInfo{}

	if err := yaml.Unmarshal(config, configInfo); err != nil {
		return err
	}

	for tableName, tableInfo := range configInfo.Tables {
		fields := make(map[string]*FieldSchema)

		// Validate declared fields
		t := reflect.TypeOf(tableInfo)
		v := reflect.ValueOf(tableInfo)

		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)

			switch sf.Name {
			case "Fields":
				continue
			case "SubIndices":
				for _, indexInfo := range tableInfo.SubIndices {
					iit := reflect.TypeOf(indexInfo)
					iiv := reflect.ValueOf(indexInfo)
					for s := 0; s < iit.NumField(); s++ {
						ssf := iit.Field(s)
						sfv := iiv.Field(s).String()
						if _, ok := tableInfo.Fields[sfv]; !ok {
							return errors.New(
								fmt.Sprintf("Table configuration error - undefined field '%v' for SubIndices '%v'", sfv, ssf.Name))
						}
					}
				}
			default:
				fv := v.Field(i).String()
				if _, ok := tableInfo.Fields[fv]; !ok {
					return errors.New(
						fmt.Sprintf("Table configuration error - undefined field '%v' for '%v'", fv, sf.Name))
				}
			}
		}

		for fieldKey, fieldType := range tableInfo.Fields {
			field := &FieldSchema{Name: fieldKey, Type: fieldType}
			fields[fieldKey] = field
		}

		primaryKey := &Key{
			HashKey: fields[tableInfo.HashKey],
			SortKey: fields[tableInfo.SortKey]}
		schema := &TableSchema{
			Fields:       fields,
			PrimaryKey:   primaryKey,
			SubIndexKeys: new(sync.Map),
			ExpireField:  fields[tableInfo.ExpireKey]}

		primaryIndex := &PrimaryIndex{&Index{
			Data: new(sync.Map),
			Key:  primaryKey}}

		subIndices := new(sync.Map)

		for indexName, indexInfo := range tableInfo.SubIndices {

			subKey := &Key{HashKey: fields[indexInfo.HashKey],
				SortKey: fields[indexInfo.SortKey]}

			schema.SubIndexKeys.Store(indexName, subKey)

			subIndices.Store(indexName, &SubIndex{&Index{
				Data: new(sync.Map),
				Key:  subKey}})
		}

		bingo.AddTable(tableName, schema, primaryIndex, subIndices)
	}

	return nil
}
