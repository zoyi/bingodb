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

	if err := yaml.UnmarshalStrict(config, configInfo); err != nil {
		return err
	}

	for tableName, tableInfo := range configInfo.Tables {
		if err := validateFields(tableName, tableInfo); err != nil {
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

func validateFields(tableName string, tableInfo TableInfo) error {
	// check HashKey, SortKey, ExpireKey has value and it's value contains in fields map
	fields := reflect.TypeOf(tableInfo)
	values := reflect.ValueOf(tableInfo)

	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		switch field.Name {
		case
			"HashKey",
			"SortKey",
			"ExpireKey":
			value := values.Field(i).String()
			if value == "" {
				return errors.New(
					fmt.Sprintf("Table configuration error (Table '%v') - field '%v' cannot be empty", tableName, field.Name))
			}
			if _, ok := tableInfo.Fields[value]; !ok {
				return errors.New(
					fmt.Sprintf("Table configuration error (Table '%v') - undefined field '%v' for '%v'", tableName, value, field.Name))
			}
		}
	}

	// check fields is not empty and field's value type is valid
	if len(tableInfo.Fields) == 0 {
		return errors.New(
			fmt.Sprintf("Table configuration error (Table '%v') - fields cannot be empty", tableName))
	}
	for fieldName, fieldType := range tableInfo.Fields {
		if ok := isAllowedFieldType(fieldType); !ok {
			return errors.New(
				fmt.Sprintf("Table configuration error (Table '%v') - unknown field type '%v' in '%v'", tableName, fieldType, fieldName))
		}
	}

	// check subIndices
	for indexName, indexInfo := range tableInfo.SubIndices {
		// check hashKey empty
		if indexInfo.HashKey == "" {
			return errors.New(
				fmt.Sprintf("Table configuration error (Table '%v') - hashKey caannot be empty in index '%v' for subIndices", tableName, indexName))
		}

		// check sortKey empty
		if indexInfo.SortKey == "" {
			return errors.New(
				fmt.Sprintf("Table configuration error (Table '%v') - sortKey caannot be empty in index '%v' for subIndices", tableName, indexName))
		}

		// check hashKey and sortKey's value containes in fields
		fields := reflect.TypeOf(indexInfo)
		values := reflect.ValueOf(indexInfo)

		for i := 0; i < fields.NumField(); i++ {
			field := fields.Field(i)
			value := values.Field(i).String()
			if _, ok := tableInfo.Fields[value]; !ok {
				return errors.New(
					fmt.Sprintf("Table configuration error (Table '%v') - undefined field '%v' for subIndices '%v'", tableName, value, field.Name))
			}
		}

		// check hashKey and sortKey is different
		if indexInfo.HashKey == indexInfo.SortKey {
			return errors.New(
				fmt.Sprintf("Table configuration error (Table '%v') - hashKey and sortKey must be different in index '%v' for subIndices", tableName, indexName))
		}
	}

	// check expireKey's value type is integer
	if tableInfo.Fields[tableInfo.ExpireKey] != INTEGER {
		return errors.New(
			fmt.Sprintf("Table configuration error (Table '%v') - Only integer type can be used for expireKey. Current key '%v' is '%v'", tableName, tableInfo.ExpireKey, tableInfo.Fields[tableInfo.ExpireKey]))
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