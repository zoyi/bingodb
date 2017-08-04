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
	Stream     StreamInfo           `yaml:"stream"`
}

type IndexInfo struct {
	HashKey string `yaml:"hashKey"`
	SortKey string `yaml:"sortKey"`
}

type ConfigInfo struct {
	Tables   map[string]TableInfo   `yaml:"tables"`
	Adaptors map[string]AdaptorInfo `yaml:"adaptors"`
}

const (
	STRING  = "string"
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

	if err := bingo.loadAdaptors(configInfo); err != nil {
		return err
	}

	for k, v := range bingo.Adaptors {
		fmt.Println(k, v)
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

		stream := makeStream(bingo.Adaptors, tableInfo.Stream)

		bingo.addTable(tableName, schema, primaryIndex, subIndices, stream)
	}

	return nil
}

func isValidTable(tableName string, tableInfo TableInfo) error {
	format := fmt.Sprintf("Table configuration error (Table '%v')", tableName)

	if err := isValidFields(tableInfo.Fields); err != nil {
		return errors.New(fmt.Sprintf("%v - %v", format, err.Error()))
	}

	if err, ok := isValidSubIndices(tableInfo.SubIndices, tableInfo.Fields); !ok {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	if err, ok := isValidKeySet(tableInfo.HashKey, tableInfo.SortKey, tableInfo.Fields); !ok {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	if err, ok := isValidExpireKey(tableInfo.ExpireKey, tableInfo.Fields); !ok {
		return errors.New(fmt.Sprintf("%v - %v", format, err))
	}

	return nil
}

// check fields is not empty and field's value type is valid
func isValidFields(fields map[string]string) error {
	if len(fields) == 0 {
		return errors.New("fields cannot be empty")
	}
	for fieldName, fieldType := range fields {
		if ok := isAllowedFieldType(fieldType); !ok {
			return errors.New(fmt.Sprintf("unknown field type '%v' in '%v'", fieldType, fieldName))
		}
	}

	return nil
}

// check subIndices empty, hashKey and SortKey's difference, value is contains in fields
func isValidSubIndices(subIndices map[string]IndexInfo, fields map[string]string) (string, bool) {
	for indexName, indexInfo := range subIndices {
		if err, ok := isValidKeySet(indexInfo.HashKey, indexInfo.SortKey, fields); !ok {
			return fmt.Sprintf("%v in index '%v' for subIndices", err, indexName), false
		}
	}

	return "", true
}

func isValidKeySet(hashKey string, sortKey string, fields map[string]string) (string, bool) {
	if err, ok := isValidReferenceField(hashKey, "hashKey", fields); !ok {
		return err, false
	}
	if err, ok := isValidReferenceField(sortKey, "sortKey", fields); !ok {
		return err, false
	}
	if hashKey == sortKey {
		return "hashKey and sortKey must be different", false
	}

	return "", true
}

func isValidExpireKey(expireKey string, fields map[string]string) (string, bool) {
	if err, ok := isValidReferenceField(expireKey, "expireKey", fields); !ok {
		return err, false
	}

	if fields[expireKey] != INTEGER {
		return fmt.Sprintf("Only integer type can be used for expireKey. Current key '%v' is '%v'", expireKey, fields[expireKey]), false
	}

	return "", true
}

func isValidReferenceField(key string, name string, fields map[string]string) (string, bool) {
	if key == "" {
		return fmt.Sprintf("%v cannot be empty", name), false
	}
	if _, ok := fields[key]; !ok {
		return fmt.Sprintf("undefined field '%v' for %v", key, name), false
	}

	return "", true
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
