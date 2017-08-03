package model

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
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

	if len(configInfo.Tables) == 0 {
		return errors.New("Table cannot be empty")
	}

	for tableName, tableInfo := range configInfo.Tables {
		if err := isValidTable(tableName, tableInfo); err != nil {
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

func isValidTable(tableName string, tableInfo TableInfo) error {
	format := fmt.Sprintf("Table configuration error (Table '%v')", tableName)

	if err := isValidFields(tableInfo.Fields); err != "" {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	if err := isValidSubIndices(tableInfo.SubIndices, tableInfo.Fields); err != "" {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	if err := isValidKeySet(tableInfo.HashKey, tableInfo.SortKey, tableInfo.Fields); err != "" {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	if err := isValidExpireKey(tableInfo.ExpireKey, tableInfo.Fields); err != "" {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	return nil
}

// check fields is not empty and field's value type is valid
func isValidFields(fields map[string]string) string {
	if len(fields) == 0 {
		return "fields cannot be empty"
	}
	for fieldName, fieldType := range fields {
		if ok := isAllowedFieldType(fieldType); !ok {
			return fmt.Sprintf("unknown field type '%v' in '%v'", fieldType, fieldName)
		}
	}

	return ""
}

// check subIndices empty, hashKey and SortKey's difference, value is contains in fields
func isValidSubIndices(subIndices map[string]IndexInfo, fields map[string]string) string {
	for indexName, indexInfo := range subIndices {
		if err := isValidKeySet(indexInfo.HashKey, indexInfo.SortKey, fields); err != "" {
			return fmt.Sprintf("%v in index '%v' for subIndices", err, indexName)
		}
	}

	return ""
}

func isValidKeySet(hashKey string, sortKey string, fields map[string]string) string {
	if err := validateReferenceField(hashKey, "hashKey", fields); err != "" {
		return err
	}
	if err := validateReferenceField(sortKey, "sortKey", fields); err != "" {
		return err
	}
	if hashKey == sortKey {
		return "hashKey and sortKey must be different"
	}

	return ""
}

func isValidExpireKey(expireKey string, fields map[string]string) string {
	if err := validateReferenceField(expireKey, "expireKey", fields); err != "" {
		return err
	}

	if fields[expireKey] != INTEGER {
		return fmt.Sprintf("Only integer type can be used for expireKey. Current key '%v' is '%v'", expireKey, fields[expireKey])
	}

	return ""
}

func validateReferenceField(key string, name string, fields map[string]string) string {
	if key == "" {
		return fmt.Sprintf("%v cannot be empty", name)
	}
	if _, ok := fields[key]; !ok {
		return fmt.Sprintf("undefined field '%v' for %v", key, name)
	}

	return ""
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