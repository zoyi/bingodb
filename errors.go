package bingodb

type BingoError struct {
	Message string
	Code    int
}

const (
	GeneralFieldError    = "Parsing field error"
	FieldError           = "Field '%s' is defined as %s type but value '%v' cannot be parsed"
	SetOrInsertMissing   = "set or setOnInsert are required"
	HashKeyMissing       = "Hash key is missing in set"
	SortKeyMissing       = "Hash key is missing in set"
	DocumentNotFound     = "Document not found"
	IndexNotFound        = "Index not found"
	GeneralTableNotFound = "Table not found"
	TableNotFound        = "Table not found: %s"
	GeneralParsingError  = "Parsing error occurred"
	UnknownError         = "Unknown error"
)

const (
	BingoFieldParsingError       = 100
	BingoSetOrInsertMissingError = 200
	BingoHashKeyMissingError     = 300
	BingoSortKeyMissingError     = 301
	BingoDocumentNotFoundError   = 400
	BingoIndexNotFoundError      = 401
	BingoTableNotFoundError      = 402
	BingoJSONParsingError        = 500
)
