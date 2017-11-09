package bingodb

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type MetricsConfig struct {
	Ttl       int64  `yaml:"ttl"`
	Interval  int64  `yaml:"interval"`
	ExpireKey string `yaml:"expireKey"`
	Count     string `yaml:"count"`
	Time      string `yaml:"time"`
}

type SubIndexConfig struct {
	HashKey string `yaml:"hashKey"`
	SortKey string `yaml:"sortKey"`
}

type TableConfig struct {
	Fields     map[string]string         `yaml:"fields"`
	HashKey    string                    `yaml:"hashKey"`
	SortKey    string                    `yaml:"sortKey"`
	SubIndices map[string]SubIndexConfig `yaml:"subIndices"`
	ExpireKey  string                    `yaml:"expireKey"`
	Metrics    *MetricsConfig            `yaml:"metrics"`
}

type ServerConfig struct {
	Addr    string `yaml:"addr,omitempty"`
	Logging bool   `yaml:"logging,omitempty"`
}

type BingoConfig struct {
	ServerConfig *ServerConfig          `yaml:"server,omitempty"`
	Tables       map[string]TableConfig `yaml:"tables,omitempty"`
}

const (
	STRING  = "string"
	INTEGER = "integer"
)

// NewBingoFromConfigFile configuration file with specified path and
// parse it to create source schema to prepare bingo.
// It returns any error encountered.
func ParseConfig(bingo *Bingo, path string) error {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return ParseConfigBytes(bingo, configBytes)
}

// Parse specified string of configuration and
// create source schema to prepare bingo.
// It returns any error encountered.
func ParseConfigString(bingo *Bingo, configString string) error {
	return ParseConfigBytes(bingo, []byte(configString))
}

// Parse specified bytes of configuration and
// create source schema to prepare bingo.
// It returns any error encountered.
func ParseConfigBytes(bingo *Bingo, configBytes []byte) error {
	bingoConfig := &BingoConfig{}
	if err := yaml.UnmarshalStrict(configBytes, bingoConfig); err != nil {
		return err
	}

	if len(bingoConfig.Tables) == 0 {
		return errors.New("Table cannot be empty")
	}

	for tableName, tableConfig := range bingoConfig.Tables {
		if len(tableName) == 0 {
			continue
		}
		if err := isValidTable(tableName, tableConfig); err != nil {
			return err
		}

		fields := make(map[string]*FieldSchema)

		for fieldKey, fieldType := range tableConfig.Fields {
			field := &FieldSchema{Name: fieldKey, Type: fieldType}
			fields[fieldKey] = field
		}

		primaryKey := &Key{
			hashKey: fields[tableConfig.HashKey],
			sortKey: fields[tableConfig.SortKey],
		}

		schema := &TableSchema{
			fields:      fields,
			primaryKey:  primaryKey,
			expireField: fields[tableConfig.ExpireKey]}

		primaryIndex := &PrimaryIndex{index: newIndex(primaryKey)}

		subIndices := make(map[string]*SubIndex)

		for indexName, indexConfig := range tableConfig.SubIndices {
			subKey := &Key{
				hashKey: fields[indexConfig.HashKey],
				sortKey: fields[indexConfig.SortKey],
			}

			subIndices[indexName] = &SubIndex{
				index:      newIndex(subKey),
				primaryKey: primaryKey,
			}
		}

		bingo.tables[tableName] = newTable(bingo, tableName, schema, primaryIndex, subIndices, tableConfig.Metrics)
	}

	bingo.setTableMetrics()

	bingo.ServerConfig = bingoConfig.ServerConfig

	return nil
}

func isValidTable(tableName string, tableInfo TableConfig) error {
	format := fmt.Sprintf("Table configuration error (Table '%v')", tableName)

	if strings.HasPrefix(tableName, "_") {
		return errors.New("Table name starts with '_' is prohibited.")
	} else if tableName == "metrics" {
		return errors.New("Table name cannot be 'metrics'")
	}

	if err := isValidFields(tableInfo.Fields); err != nil {
		return errors.New(fmt.Sprintf("%v - %v", format, err.Error()))
	}

	if err := isValidMetrics(tableInfo.Metrics); err != nil {
		return errors.New(fmt.Sprintf("%v - %v", format, err.Error()))
	}

	//if err := isValidSubIndices(tableInfo.SubIndices, tableInfo.Fields); err != nil {
	//	return errors.New(fmt.Sprintf("%v - %v", format, err.Error()))
	//}

	if err := isValidKeySet(tableInfo.HashKey, tableInfo.SortKey, tableInfo.Fields); err != nil {
		return errors.New(fmt.Sprintf("%v - %v", format, err.Error()))
	}

	if err := isValidExpireKey(tableInfo.ExpireKey, tableInfo.Fields); err != nil {
		return errors.New(fmt.Sprintf("%v - %v", format, err.Error()))
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

// check fields is not empty and field's value type is valid
func isValidMetrics(metricConfig *MetricsConfig) error {
	if metricConfig == nil {
		return nil
	}

	if metricConfig.Ttl == 0 {
		return errors.New("ttl value must be specified in metrics")
	}
	if metricConfig.Interval == 0 {
		return errors.New("interval value must be specified in metrics")
	}

	return nil
}

// check subIndices empty, HashKey and SortKey's difference, value is contains in fields
//func isValidSubIndices(subIndices map[string]IndexConfig, fields map[string]string) error {
//	for IndexName, indexInfo := range subIndices {
//		if err := isValidKeySet(indexInfo.HashKey, indexInfo.SortKey, fields); err != nil {
//			return errors.New(fmt.Sprintf("%v in index '%v' for subIndices", err.Error(), IndexName))
//		}
//	}
//
//	return nil
//}

func isValidKeySet(hashKey string, sortKey string, fields map[string]string) error {
	if err := isValidReferenceField(hashKey, "HashKey", fields); err != nil {
		return err
	}
	//if err := isValidReferenceField(sortKey, "sortKey", fields); err != nil {
	//	return err
	//}
	if hashKey == sortKey {
		return errors.New("HashKey and sortKey must be different")
	}

	return nil
}

func isValidExpireKey(expireKey string, fields map[string]string) error {
	if err := isValidReferenceField(expireKey, "expireKey", fields); err != nil {
		return err
	}

	if fields[expireKey] != INTEGER {
		return errors.New(
			fmt.Sprintf("Only integer type can be used for expireKey. Current key '%v' is '%v'", expireKey, fields[expireKey]))
	}

	return nil
}

func isValidReferenceField(key string, name string, fields map[string]string) error {
	if key == "" {
		return errors.New(fmt.Sprintf("%v cannot be empty", name))
	}
	if _, ok := fields[key]; !ok {
		return errors.New(fmt.Sprintf("undefined field '%v' for %v", key, name))
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
