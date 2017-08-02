package model

import (
	"encoding/json"
	"github.com/emirpasic/gods/utils"
	"reflect"
	"strconv"
	"sync"
)

type FieldSchema struct {
	Name string
	Type string
}

type Key struct {
	HashKey *FieldSchema
	SortKey *FieldSchema
}

type TableSchema struct {
	Fields       map[string]*FieldSchema
	PrimaryKey   *Key
	SubIndexKeys *sync.Map
	ExpireField  *FieldSchema
}

type Table struct {
	Bingo        *Bingo
	Name         string
	PrimaryIndex *PrimaryIndex
	SubIndices   *sync.Map //map of Subindicies
	Schema       *TableSchema
}

type SubSortTreeKey struct {
	Key interface{}
	*Document
}

func (key *SubSortTreeKey) Empty() bool {
	return key.Key == nil && key.Document == nil
}

func (field *FieldSchema) Parse(raw interface{}) interface{} {
	switch field.Type {
	case "integer":
		switch raw.(type) {
		case json.Number:
			value, _ := raw.(json.Number).Int64()
			return value

		case string:
			value, _ := strconv.ParseInt(raw.(string), 10, 64)
			return value

		case int:
			return int64(raw.(int))
		case float64:
			return int64(raw.(float64))
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
	return utils.StringComparator(a, b)
}

func (key *Key) Compare(a, b *Document) int {
	if a == nil || b == nil {
		if a == b {
			return 0
		} else if a == nil {
			return -1
		} else {
			return 1
		}
	}
	hashDiff := GeneralCompare(a.Get(key.HashKey.Name), b.Get(key.HashKey.Name))
	if hashDiff != 0 {
		return hashDiff
	}

	return GeneralCompare(a.Get(key.SortKey.Name), b.Get(key.SortKey.Name))
}

func (schema *TableSchema) IsValidData(data Data) (ok bool) {
	if _, exists := data[schema.PrimaryKey.HashKey.Name]; !exists {
		return false
	}
	if _, exists := data[schema.PrimaryKey.SortKey.Name]; !exists {
		return false
	}
	if _, exists := data[schema.ExpireField.Name]; !exists {
		return false
	}

	ok = true
	schema.SubIndexKeys.Range(func(key, value interface{}) bool {
		if subIndexKey, exists := value.(*Key); exists {
			if _, exists := data[subIndexKey.HashKey.Name]; !exists {
				ok = false
			}
		}
		return true
	})
	return ok
}

func (table *Table) Delete(hash interface{}, sort interface{}) (*Document, bool) {
	doc, removed := table.PrimaryIndex.delete(hash, sort)
	if !removed {
		return nil, false
	}

	table.SubIndices.Range(func(key, value interface{}) bool {
		if index, ok := value.(*SubIndex); ok {
			index.delete(doc)
		}
		return true
	})

	table.Bingo.Keeper.delete(table, doc)

	return doc, removed
}

func (table *Table) Get(hash interface{}, sort interface{}) (*Document, bool) {
	return table.PrimaryIndex.Get(hash, sort)
}

func (table *Table) Index(name string) *SubIndex {
	valueData, _ := table.SubIndices.Load(name)
	if value, ok := valueData.(*SubIndex); ok {
		return value
	}

	return nil
}

func (table *Table) Put(data *Data) *Document {
	doc := ParseDoc(*data, table.Schema)

	// Insert doc into primary index
	removed, replaced := table.PrimaryIndex.put(doc)

	// Update for sub index
	table.SubIndices.Range(func(key, value interface{}) bool {
		if index, ok := value.(*SubIndex); ok {
			if replaced {
				index.delete(removed)
			}
			index.put(doc)
		}

		return true
	})

	// Update for event tree
	keeper := table.Bingo.Keeper
	if replaced {
		keeper.delete(table, removed)
	}
	keeper.put(table, doc)

	return doc
}
