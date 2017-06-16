package model

import (
	"encoding/json"
	"strconv"
	"reflect"
	"github.com/emirpasic/gods/utils"
)

type FieldSchema struct {
	Name string
	Type string
}

type Key struct {
	HashKey FieldSchema
	SortKey FieldSchema
}

type TableSchema struct {
	Fields map[string]FieldSchema
	PrimaryKey *Key
	SubIndexKeys map[string]*Key
}

type Table struct {
	PrimaryIndex  *PrimaryIndex
	SubIndices map[string]*SubIndex
	Schema     *TableSchema
}

type SubSortTreeKey struct {
	Key interface{}
	*Document
}


func (field *FieldSchema) Parse(raw interface{}) interface {} {
	switch field.Type {
	case "integer":
		switch raw.(type) {
		case json.Number:
			value,_ := raw.(json.Number).Int64()
			return value

		case string:
			value, _ := strconv.ParseInt(raw.(string), 10, 64)
			return value

		case int:
			return int64(raw.(int))
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
	if reflect.TypeOf(a).Kind() == reflect.Int64 {
		return NumberComparator(a, b)
	}
	return utils.StringComparator(a, b)
}

func (key *Key) Compare(a, b *Document) int {
	if a == nil || b == nil {
		if a == b {
			return 0;
		} else if a == nil {
			return -1;
		} else {
			return 1;
		}
	}
	hashDiff := GeneralCompare(a.Get(key.HashKey.Name), b.Get(key.HashKey.Name))
	if hashDiff != 0 {
		return hashDiff
	}

	return GeneralCompare(a.Get(key.SortKey.Name), b.Get(key.SortKey.Name))
}


func (table *Table) Delete(hash interface{}, sort interface{}) (*Document, bool) {
	doc, removed := table.PrimaryIndex.delete(hash, sort)
	if !removed {
		return nil, false
	}

	for _, index := range table.SubIndices {
		index.delete(doc)
	}

	return doc, removed
}

func (table *Table) Get(hash interface{}, sort interface{}) (*Document, bool) {
	return table.PrimaryIndex.Get(hash, sort)
}

func (table *Table) Index(name string) *SubIndex {
	return table.SubIndices[name]
}

func (table *Table) Put(data *Data) *Document {
	doc := ParseDoc(*data, table.Schema)

	// Insert doc into primary index
	replaced, removed := table.PrimaryIndex.put(doc)

	for _, index := range table.SubIndices {
		if replaced {
			index.delete(removed.(*Document))
		}
		index.put(doc)
	}

	return doc
}
