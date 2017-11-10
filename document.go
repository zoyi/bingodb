package bingodb

import (
	"encoding/json"
)

type Data map[string]interface{}

type Document struct {
	data   Data
	schema *TableSchema
}

func (data *Data) Length() int {
	var d map[string]interface{} = *data
	return len(d)
}

func Merge(doc *Document, op *Document) *Document {
	if op == nil {
		return doc
	}
	if doc == nil {
		return op
	}

	if doc.schema != op.schema {
		panic("The schema of the two docs are different")
	}

	newbie := make(map[string]interface{})

	for k, v := range doc.data {
		newbie[k] = v
	}
	for k, v := range op.data {
		newbie[k] = v
	}

	return &Document{data: newbie, schema: doc.schema}
}

func (doc *Document) Merge(op *Document) *Document {
	if op == nil {
		return doc
	}
	if doc.schema != op.schema {
		panic("The schema of the two docs are different")
	}

	newbie := make(map[string]interface{})

	for k, v := range doc.data {
		newbie[k] = v
	}
	for k, v := range op.data {
		newbie[k] = v
	}

	return &Document{data: newbie, schema: doc.schema}
}

func ParseDoc(data *Data, schema *TableSchema) (*Document, error) {
	//for doc, parsing nil equivalent to success
	if data == nil || data.Length() == 0 {
		return nil, nil
	}

	for _, field := range schema.fields {
		raw, present := (*data)[field.Name]
		if present {
			val, err := field.Parse(raw)
			if err != nil {
				return nil, err
			}
			(*data)[field.Name] = val
		}
	}

	return &Document{data: *data, schema: schema}, nil
}

func (doc *Document) Data() Data {
	return doc.data
}

func (doc *Document) ToJSON() []byte {
	bytes, err := json.Marshal(doc.data)
	if err != nil {
		return nil
	}
	return bytes
}

func (doc *Document) Get(schema *FieldSchema) interface{} {
	if schema != nil {
		return doc.data[schema.Name]
	} else {
		return nil
	}
}

func (doc *Document) Fetch(field string) interface{} {
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
