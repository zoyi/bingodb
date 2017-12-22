package bingodb

const (
	FieldError         = "field '%s' is defined as %s type but value '%v' cannot be parsed"
	SetOrInsertMissing = "set or setOnInsert are required"
	HashKeyMissing     = "hash key is missing in set"
	SortKeyMissing     = "sort key is missing in set"
	ExpireKeyMissing   = "expire key is missing in set"
	DocumentNotFound   = "document not found"
)
