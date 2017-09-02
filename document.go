package bingodb

import (
	"encoding/json"
)

type Data map[string]interface{}

type Document struct {
	data   Data
	schema *TableSchema
}

func ParseDoc(data Data, schema *TableSchema) *Document {
	for _, field := range schema.fields {
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

func (doc *Document) ToJSON() []byte {
	bytes, err := json.Marshal(doc.data)
	if err != nil {
		return nil
	}
	return bytes
}

func (doc *Document) Get(field string) interface{} {
	return doc.data[field]
}

func (doc *Document) GetExpiresAt() (int64, bool) {
	if doc.schema.expireField == nil {
		return 0, false
	}

	value, ok := doc.data[doc.schema.expireField.Name]
	if !ok {
		return 0, false
	}

	return value.(int64), ok
}
