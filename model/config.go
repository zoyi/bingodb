package model

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"errors"
	"reflect"
	"fmt"
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

const (
	STRING = "string"
	INTEGER = "integer"
)

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
		if err := validateFields(tableInfo); err != nil {
			return err
		}

		fields := make(map[string]*FieldSchema)

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

func validateFields(tableInfo TableInfo) error {
	// check fields
	if len(tableInfo.Fields) == 0 {
		return errors.New(
			fmt.Sprintf("Table configuration error - fields cannot be empty"))
	}
	for fieldName, fieldType := range tableInfo.Fields {
		if ok := isAllowedFieldType(fieldType); !ok {
			return errors.New(
				fmt.Sprintf("Table configuration error - unknown field type '%v' in '%v'", fieldType, fieldName))
		}
	}

	// check subIndices
	for _, indexInfo := range tableInfo.SubIndices {
		fields := reflect.TypeOf(indexInfo)
		values := reflect.ValueOf(indexInfo)

		for i := 0; i < fields.NumField(); i++ {
			field := fields.Field(i)
			value := values.Field(i).String()
			if _, ok := tableInfo.Fields[value]; !ok {
				return errors.New(
					fmt.Sprintf("Table configuration error - undefined field '%v' for subIndices '%v'", value, field.Name))
			}
		}
	}

	// check valid field
	fields := reflect.TypeOf(tableInfo)
	values := reflect.ValueOf(tableInfo)

	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		switch field.Name {
		case
			"Fields",
			"SubIndices":
			continue

		default:
			value := values.Field(i).String()
			if _, ok := tableInfo.Fields[value]; !ok {
				return errors.New(
					fmt.Sprintf("Table configuration error - undefined field '%v' for '%v'", value, field.Name))
			}
		}
	}

	// check expireKey
	if tableInfo.ExpireKey == "" {
		return errors.New(
			fmt.Sprintf("Table configuration error - expireKey cannot be empty"))
	}
	if tableInfo.Fields[tableInfo.ExpireKey] != INTEGER {
		return errors.New(
			fmt.Sprintf("Table configuration error - Only integer type can be used for expireKey. Current key '%v' is '%v'", tableInfo.ExpireKey, tableInfo.Fields[tableInfo.ExpireKey]))
	}

	return nil
}

func isAllowedFieldType(fieldType string) bool {
	switch fieldType {
	case
		STRING,
		INTEGER:
		return true
	}
	return false
}