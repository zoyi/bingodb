package bingodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zoyi/skiplist/lib"
	"reflect"
	"strconv"
)

type FieldSchema struct {
	Name string
	Type string
}

type Key struct {
	hashKey *FieldSchema
	sortKey *FieldSchema
}

type TableSchema struct {
	fields      map[string]*FieldSchema
	primaryKey  *Key
	subKeys     map[string]*Key
	expireField *FieldSchema
}

type Table struct {
	*TableSchema
	bingo             *Bingo
	name              string
	primaryIndex      *PrimaryIndex
	subIndices        map[string]*SubIndex
	metricsConfig     *MetricsConfig
	expireKeyRequired bool
}

type TableInfo struct {
	Name              string           `json:"name"`
	Size              int              `json:"size"`
	SubIndices        map[string]int64 `json:"subIndices,omitempty"`
	ExpireKeyRequired bool             `json:"expireKeyRequired"`
}

func (table *Table) Info() *TableInfo {
	subIndices := make(map[string]int64)
	for key, index := range table.subIndices {
		subIndices[key] = index.size
	}
	return &TableInfo{
		Name:              table.name,
		Size:              int(table.primaryIndex.size),
		SubIndices:        subIndices,
		ExpireKeyRequired: table.expireKeyRequired}
}

type SubSortKey struct {
	sort        interface{}
	primaryHash interface{}
	primarySort interface{}
}

func (key SubSortKey) Array() interface{} {
	return []interface{}{key.sort, key.primaryHash, key.primarySort}
}

func newTable(
	bingo *Bingo,
	tableName string,
	schema *TableSchema,
	primaryIndex *PrimaryIndex,
	subIndices map[string]*SubIndex,
	metricsConfig *MetricsConfig,
	expireKeyRequired bool,
) *Table {
	return &Table{
		TableSchema:       schema,
		bingo:             bingo,
		name:              tableName,
		primaryIndex:      primaryIndex,
		subIndices:        subIndices,
		metricsConfig:     metricsConfig,
		expireKeyRequired: expireKeyRequired,
	}
}

func (table *Table) makeMetricsTable() *Table {
	if table.metricsConfig == nil {
		return nil
	}
	metricsConfig := table.metricsConfig

	tableName := fmt.Sprintf("_%s", table.name)

	fields := make(map[string]*FieldSchema)

	fields[table.HashKey().Name] = table.HashKey()

	if len(metricsConfig.Time) == 0 {
		metricsConfig.Time = "time"
	}
	if len(metricsConfig.Count) == 0 {
		metricsConfig.Count = "count"
	}
	if len(metricsConfig.ExpireKey) == 0 {
		metricsConfig.ExpireKey = "expireAt"
	}

	fields[metricsConfig.Time] = &FieldSchema{Name: metricsConfig.Time, Type: "integer"}
	fields[metricsConfig.Count] = &FieldSchema{Name: metricsConfig.Count, Type: "integer"}
	fields[metricsConfig.ExpireKey] = &FieldSchema{Name: metricsConfig.ExpireKey, Type: "integer"}

	primaryKey := &Key{
		hashKey: fields[table.HashKey().Name],
		sortKey: fields[metricsConfig.Time],
	}

	schema := &TableSchema{
		fields:      fields,
		primaryKey:  primaryKey,
		expireField: fields[metricsConfig.ExpireKey]}

	primaryIndex := &PrimaryIndex{index: newIndex(primaryKey)}

	subIndices := make(map[string]*SubIndex)

	metricsTable := newTable(table.bingo, tableName, schema, primaryIndex, subIndices, nil, true)
	table.bingo.tables[tableName] = metricsTable

	return metricsTable
}

func (table *Table) HashKey() *FieldSchema {
	return table.primaryKey.hashKey
}

func (table *Table) SortKey() *FieldSchema {
	return table.primaryKey.sortKey
}

func ParseField(field *FieldSchema, raw interface{}) interface{} {
	if field == nil {
		return nil
	} else {
		val, err := field.Parse(raw)
		if err != nil {
			return nil
		}
		return val
	}
}

func (field *FieldSchema) Parse(raw interface{}) (interface{}, error) {
	switch field.Type {
	case "integer":
		switch raw.(type) {
		case json.Number:
			value, err := raw.(json.Number).Int64()
			return value, err

		case string:
			value, err := strconv.ParseInt(raw.(string), 10, 64)
			return value, err

		case []byte:
			value, err := strconv.ParseInt(string(raw.([]byte)), 10, 64)
			return value, err

		case int:
			return int64(raw.(int)), nil

		case int64:
			return raw.(int64), nil

		case float64:
			return int64(raw.(float64)), nil

		case []interface{}:
			return field.Parse(raw.([]interface{})[0])
		}

	case "string":
		switch raw.(type) {
		case []byte:
			if bytes := raw.([]byte); len(bytes) > 0 {
				return string(raw.([]byte)), nil
			} else {
				return nil, nil
			}

		case []interface{}:
			return field.Parse(raw.([]interface{})[0])

		case string:
			if len(raw.(string)) > 0 {
				return raw, nil
			}
		}
	}

	return raw, fmt.Errorf(FieldError, field.Name, field.Type, raw)
}

func NumberComparator(a, b interface{}) int {
	aAsserted := a.(int64)
	bAsserted := b.(int64)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func GeneralCompare(a, b interface{}) int {
	if a == nil || b == nil {
		if a == b {
			return 0
		} else if a == nil {
			return -1
		} else {
			return 1
		}
	}
	if reflect.TypeOf(a).Kind() == reflect.Int64 {
		return NumberComparator(a, b)
	}
	return lib.StringComparator(a, b)
}

func (schema *TableSchema) Compare(a, b *Document) int {
	if a == nil || b == nil {
		if a == b {
			return 0
		} else if a == nil {
			return -1
		} else {
			return 1
		}
	}
	if diff := GeneralCompare(a.Get(schema.primaryKey.hashKey), b.Get(schema.primaryKey.hashKey)); diff != 0 {
		return diff
	}

	return GeneralCompare(a.Get(schema.primaryKey.sortKey), b.Get(schema.primaryKey.sortKey))
}

func (table *Table) Index(name string) IndexInterface {
	if len(name) == 0 {
		return table.primaryIndex
	} else if index, ok := table.subIndices[name]; ok {
		return index
	}
	return nil
}

func (table *Table) PrimaryIndex() *PrimaryIndex {
	return table.primaryIndex
}

func (table *Table) Put(setData *Data, setOnInsertData *Data) (*Document, *Document, bool, error) {
	set, setErr := ParseDoc(setData, table.TableSchema)
	setOnInsert, setOnInsertErr := ParseDoc(setOnInsertData, table.TableSchema)
	if setErr != nil {
		return nil, nil, false, setErr
	}
	if setOnInsertErr != nil {
		return nil, nil, false, setOnInsertErr
	}
	if set == nil && setOnInsert == nil {
		return nil, nil, false, errors.New(SetOrInsertMissing)
	}

	merged := Merge(set, setOnInsert)

	if merged.Fetch(table.HashKey().Name) == nil {
		return nil, nil, false, errors.New(HashKeyMissing)
	}
	if table.SortKey() != nil && merged.Fetch(table.SortKey().Name) == nil {
		return nil, nil, false, errors.New(SortKeyMissing)
	}
	if table.expireKeyRequired && merged.Fetch(table.expireField.Name) == nil {
		return nil, nil, false, errors.New(ExpireKeyMissing)
	}

	onUpdate := func(oldRaw interface{}) interface{} {
		old := oldRaw.(*Document)
		return old.Merge(set)
	}

	// Insert doc into primary index
	old, newbie, replaced := table.primaryIndex.put(merged, onUpdate)

	// Update for sub index
	for _, index := range table.subIndices {
		if replaced {
			index.remove(old)
		}
		index.put(newbie)
	}

	// Update for event list
	keeper := table.bingo.keeper
	if replaced {
		keeper.remove(table, old)
	}
	keeper.put(table, newbie)

	return old, newbie, replaced, nil
}

func (table *Table) Remove(hash interface{}, sort interface{}) (*Document, error) {
	doc, err := table.primaryIndex.remove(hash, sort)
	if err != nil {
		return nil, err
	}

	for _, index := range table.subIndices {
		index.remove(doc)
	}

	table.bingo.keeper.remove(table, doc)

	return doc, nil
}

func (table *Table) RemoveByDocument(doc *Document) (*Document, error) {
	hashValue := doc.Get(table.primaryKey.hashKey)
	sortValue := doc.Get(table.primaryKey.sortKey)

	return table.Remove(hashValue, sortValue)
}
