package model

import (
	"bingodb/ds/redblacktree"
	"github.com/emirpasic/gods/utils"
	"encoding/json"
	"reflect"
	"fmt"
	"strconv"
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

type Index struct {
	Data    map[interface{}]*redblacktree.Tree
	*Key
}

type PrimaryIndex struct {
	*Index
}

type SubIndex struct {
	*Index
}

type Table struct {
	PrimaryIndex  *PrimaryIndex
	SubIndices map[string]*SubIndex
	Schema     *TableSchema
}

type Data map[string]interface{}

type Document struct {
	data   Data
	schema *TableSchema
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


func ParseDoc(data Data, schema *TableSchema) *Document {
	for _, field := range schema.Fields {
		raw, present := data[field.Name]
		if present {
			data[field.Name] = field.Parse(raw)
		}
	}

	return &Document{data: data, schema: schema}
}


func (table *Table) Put(data *Data) *Document {
	doc := ParseDoc(*data, table.Schema)

	// Insert doc into primary index
	table.PrimaryIndex.put(doc)

	for _, index := range table.SubIndices {
		index.put(doc)
	}

	return doc
}

func (index *PrimaryIndex) put(doc *Document) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := doc.Get(index.SortKey.Name)

	tree, present := index.Data[hashValue]
	if !present {
		tree = redblacktree.NewWithStringComparator()
		// &redblacktree.Tree{Comparator: Compare}
		index.Data[hashValue] = tree
	}

	tree.Put(sortValue, doc)
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

type SubSortTreeKey struct {
	Key interface{}
	*Document
}

func (index *SubIndex) put(doc *Document) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	tree, present := index.Data[hashValue]
	if !present {
		tree = &redblacktree.Tree{Comparator: func(a, b interface{}) int {
			ka := a.(SubSortTreeKey)
			kb := b.(SubSortTreeKey)
			o := GeneralCompare(ka.Key, kb.Key)
			if o != 0 {
				return o
			}

			//fmt.Println("here!!!!!!!!!!!")
			//fmt.Println(ka)
			//fmt.Println(ka.Document)
			//fmt.Println(kb)
			//fmt.Println(kb.Document)
			//fmt.Println("res: ", ka.schema.PrimaryKey.Compare(ka.Document, kb.Document))
			return doc.schema.PrimaryKey.Compare(ka.Document, kb.Document)
		},
		}
		index.Data[hashValue] = tree
	}

	tree.Put(sortValue, nil)
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

func (doc *Document) Get(field string) interface{} {
	return doc.data[field]
}

func (table *Table) Index(name string) *SubIndex {
	return table.SubIndices[name]
}

func (table *Table) Get(hash interface{}, sort interface{}) (*Document, bool) {
	return table.PrimaryIndex.Get(hash, sort)
}

func (index *PrimaryIndex) Get(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	tree, present := index.Data[hash]
	if !present {
		return nil, false
	}

	value, present := tree.Get(sort)
	if !present {
		return nil, false
	}

	return value.(*Document), true
}

func (index *SubIndex) Get(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)
	
	tree, present := index.Data[hash]
	if !present {
		return nil, false
	}

	fmt.Print(tree.String())

	node, present := tree.Ceiling(SubSortTreeKey{Key: sort})
	if !present {
		return nil, false
	}

	return node.Key.(SubSortTreeKey).Document, true
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

func (index *PrimaryIndex) delete(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	tree, present := index.Data[hash]
	if !present {
		return nil, false
	}

	value, present := tree.Remove(sort)
	if !present {
		return nil, false
	}

	return value.(*Document), true
}

func (index *SubIndex) delete(doc *Document) {
	hash := doc.Get(index.HashKey.Name)
	sort := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	tree, present := index.Data[hash]
	if !present {
		return
	}

	tree.Remove(sort)
}
