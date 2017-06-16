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

