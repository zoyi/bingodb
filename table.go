package bingodb

import (
	"encoding/json"
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
	bingo         *Bingo
	name          string
	primaryIndex  *PrimaryIndex
	subIndices    map[string]*SubIndex
	metricsConfig *MetricsConfig
}

type TableInfo struct {
	Name       string           `json:"name"`
	Size       int              `json:"size"`
	SubIndices map[string]int64 `json:"subIndices,omitempty"`
}

func (table *Table) Info() *TableInfo {
	subIndices := make(map[string]int64)
	for key, index := range table.subIndices {
		subIndices[key] = index.size
	}
	return &TableInfo{
		Name:       table.name,
		Size:       int(table.primaryIndex.size),
		SubIndices: subIndices}
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
) *Table {
	return &Table{
		TableSchema:   schema,
		bingo:         bingo,
		name:          tableName,
		primaryIndex:  primaryIndex,
		subIndices:    subIndices,
		metricsConfig: metricsConfig,
	}
}

func (table *Table) HashKey() *FieldSchema {
	return table.primaryKey.hashKey
}

func (table *Table) SortKey() *FieldSchema {
	return table.primaryKey.sortKey
}

func (table *Table) ExpiresAt() *FieldSchema {
	return table.expireField
}

func ParseField(field *FieldSchema, raw interface{}) interface{} {
	if field == nil {
		return nil
	} else {
		val, ok := field.Parse(raw)
		if !ok {
			return nil
		}
		return val
	}
}

func (field *FieldSchema) Parse(raw interface{}) (interface{}, bool) {
	if raw == nil {
		return nil, true
	}

	switch field.Type {
	case "integer":
		switch raw.(type) {
		case json.Number:
			value, err := raw.(json.Number).Int64()
			return value, err == nil

		case string:
			value, err := strconv.ParseInt(raw.(string), 10, 64)
			return value, err == nil

		case []byte:
			value, err := strconv.ParseInt(string(raw.([]byte)), 10, 64)
			return value, err == nil

		case int:
			return int64(raw.(int)), true

		case int64:
			return raw.(int64), true

		case float64:
			return int64(raw.(float64)), true

		case []interface{}:
			return field.Parse(raw.([]interface{})[0])
		}

	case "string":
		switch raw.(type) {
		case []byte:
			if bytes := raw.([]byte); len(bytes) > 0 {
				return string(raw.([]byte)), true
			} else {
				return nil, true
			}

		case []interface{}:
			return field.Parse(raw.([]interface{})[0])
		}
	}
	return raw, true
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

func (table *Table) Put(setData *Data, setOnInsertData *Data) (*Document, *Document, bool) {
	set := ParseDoc(setData, table.TableSchema)
	setOnInsert := ParseDoc(setOnInsertData, table.TableSchema)

	onUpdate := func(oldRaw interface{}) interface{} {
		old := oldRaw.(*Document)
		return old.Merge(set)
	}

	// Insert doc into primary index
	old, newbie, replaced := table.primaryIndex.put(set.Merge(setOnInsert), onUpdate)

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

	return old, newbie, replaced
}

func (table *Table) Remove(hash interface{}, sort interface{}) (doc *Document, ok bool) {
	if doc, ok = table.primaryIndex.remove(hash, sort); !ok {
		return
	}

	for _, index := range table.subIndices {
		index.remove(doc)
	}

	table.bingo.keeper.remove(table, doc)

	return
}

func (table *Table) RemoveByDocument(doc *Document) (*Document, bool) {
	hashValue := doc.Get(table.primaryKey.hashKey)
	sortValue := doc.Get(table.primaryKey.sortKey)

	return table.Remove(hashValue, sortValue)
}
