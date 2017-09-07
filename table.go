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

type TableSchema struct {
	fields      map[string]*FieldSchema
	hashKey     *FieldSchema
	sortKey     *FieldSchema
	subSortKeys map[string]*FieldSchema
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
	main interface{}
	sub  interface{}
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
	return table.hashKey
}

func (table *Table) SortKey() *FieldSchema {
	return table.sortKey
}

func (key *SubSortKey) Empty() bool {
	return key.main == nil && key.sub == nil
}

func (field *FieldSchema) Parse(raw interface{}) interface{} {
	if raw == nil {
		return raw
	}

	switch field.Type {
	case "integer":
		switch raw.(type) {
		case json.Number:
			value, _ := raw.(json.Number).Int64()
			return value

		case string:
			value, _ := strconv.ParseInt(raw.(string), 10, 64)
			return value

		case []byte:
			value, _ := strconv.ParseInt(string(raw.([]byte)), 10, 64)
			return value

		case int:
			return int64(raw.(int))

		case int64:
			return raw.(int64)

		case float64:
			return int64(raw.(float64))

		case []interface{}:
			return field.Parse(raw.([]interface{})[0])
		}

	case "string":
		switch raw.(type) {
		case []byte:
			if bytes := raw.([]byte); len(bytes) > 0 {
				return string(raw.([]byte))
			} else {
				return nil
			}

		case []interface{}:
			return field.Parse(raw.([]interface{})[0])
		}
	}
	return raw
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
	hashDiff := GeneralCompare(a.Get(schema.hashKey), b.Get(schema.hashKey))
	if hashDiff != 0 {
		return hashDiff
	}

	return GeneralCompare(a.Get(schema.sortKey), b.Get(schema.sortKey))
}

func (table *Table) Index(name string) IndexInterface {
	if len(name) == 0 {
		return table.primaryIndex
	} else {
		return table.subIndices[name]
	}
}

func (table *Table) PrimaryIndex() *PrimaryIndex {
	return table.primaryIndex
}

func (table *Table) Put(data *Data) (*Document, bool) {
	doc := ParseDoc(*data, table.TableSchema)

	// Insert doc into primary index
	original, replaced := table.primaryIndex.put(doc)

	// Update for sub index

	for _, index := range table.subIndices {
		if replaced {
			index.remove(original)
		}
		index.put(doc)
	}

	// Update for event list
	keeper := table.bingo.keeper
	if replaced {
		keeper.remove(table, original)
	}
	keeper.put(table, doc)

	return doc, replaced
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
	hashValue := doc.Get(table.hashKey)
	sortValue := doc.Get(table.sortKey)

	return table.Remove(hashValue, sortValue)
}
