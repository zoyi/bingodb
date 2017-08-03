package model

import (
	"encoding/json"
)

type Data map[string]interface{}

type Document struct {
	data   Data
	schema *TableSchema
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

func (doc *Document) GetData() Data {
	return doc.data
}

func (doc *Document) ToJSON() ([]byte, error) {
	return json.Marshal(doc.data)
}

func (doc *Document) Get(field string) interface{} {
	return doc.data[field]
}

func (doc *Document) GetExpiresAt() (int64, bool) {
	if doc.schema.ExpireField == nil {
		return 0, false
	}

	value, ok := doc.data[doc.schema.ExpireField.Name]
	if !ok {
		return 0, false
	}

	return value.(int64), ok
}

func (doc *Document) PrimaryKeyValue() (interface{}, interface{}) {
	key := doc.schema.PrimaryKey
	return doc.Get(key.HashKey.Name), doc.Get(key.SortKey.Name)
}
