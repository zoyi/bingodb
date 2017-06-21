package model

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
