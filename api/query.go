package api


type ScanQuery struct {
	IndexName string
	HashKey   interface{}
	Since     []interface{}
	Limit     int
	Backward  bool
}
