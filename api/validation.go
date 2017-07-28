package api

import (
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/model"
	"strconv"
)

func (rs *Resource) GetKeysForPrimary(
	table *model.Table,
	hashKeyData []byte,
	startKeyData []byte,
	endKeyData []byte) (hashKey, startKey, endKey interface{}) {

	hashKey = table.PrimaryIndex.HashKey.Parse(string(hashKeyData)) //need validation?
	startKey = table.PrimaryIndex.SortKey.Parse(string(startKeyData))
	if len(endKeyData) != 0 {
		endKey = table.PrimaryIndex.SortKey.Parse(string(endKeyData))
	}
	return hashKey, startKey, endKey
}

func (rs *Resource) GetKeysForSubIndex(
	primaryTable *model.Table,
	hashKeyData []byte,
	startKeyData []byte,
	endKeyData []byte,
	tableName string) (hashKey, startKey, endKey interface{}, ok bool) {

	subIndexData, _ := primaryTable.SubIndices.Load(tableName)
	if subIndex, ok := subIndexData.(*model.SubIndex); ok {
		hashKey = subIndex.HashKey.Parse(string(hashKeyData))
		if len(startKeyData) != 0 {
			startKey = subIndex.SortKey.Parse(string(startKeyData))
		}
		if len(endKeyData) != 0 {
			endKey = subIndex.SortKey.Parse(string(endKeyData))
		}

		return hashKey, startKey, endKey, true
	}

	return nil, nil, nil, false
}

func (rs *Resource) IsValidTableName(data []byte) *model.Table {
	if len(data) == 0 {
		return nil
	}

	tableData, ok := rs.Db.Tables.Load(string(data))
	if !ok {
		return nil
	}

	table, ok := tableData.(*model.Table)
	if !ok {
		return nil
	}

	return table
}

func (rs *Resource) IsValidLimit(data []byte) (int, bool) {
	var limit int
	var error error
	if len(data) == 0 {
		limit = 20
	} else {
		limit, error = strconv.Atoi(string(data))
		if error != nil {
			return 0, false
		}
	}

	return limit, true
}

func (rs *Resource) IsValidOrder(data []byte) (string, bool) {
	if len(data) == 0 {
		return "DESC", true
	}

	order := string(data)
	if order != "DESC" && order != "ASC" {
		return "", false
	}

	return order, true
}

func (rs *Resource) IsValidIndexName(data []byte) *string {
	var indexName *string
	if len(data) != 0 {
		*indexName = string(data)
	}

	return indexName
}

func (rs *Resource) ValidateGetParams(
	ctx *fasthttp.RequestCtx) (tableName string, hashKey, sortKey interface{}, ok bool) {

	tableNameData := ctx.QueryArgs().Peek("tableName")
	sortKeyData := ctx.QueryArgs().Peek("sortKey")
	hashKeyData := ctx.QueryArgs().Peek("hashKey")

	if len(tableNameData) == 0 || len(sortKeyData) == 0 || len(hashKeyData) == 0 {
		return "", nil, nil, false
	}

	tableName = string(tableNameData)
	tableData, _ := rs.Db.Tables.Load(tableName)
	if table, ok := tableData.(*model.Table); ok {
		hashKey = table.PrimaryIndex.HashKey.Parse(string(hashKeyData))
		sortKey = table.PrimaryIndex.SortKey.Parse(string(sortKeyData))
	}

	return tableName, hashKey, sortKey, true
}

func (rs *Resource) ValidateGetListParams(ctx *fasthttp.RequestCtx) (*GetParams, bool) {
	var hashKey, startKey, endKey interface{}
	var table *model.Table
	var ok bool

	tableName := ctx.QueryArgs().Peek("tableName")
	indexNameData := ctx.QueryArgs().Peek("indexName")
	hashKeyData := ctx.QueryArgs().Peek("hashKey")
	startKeyData := ctx.QueryArgs().Peek("startKey")
	endKeyData := ctx.QueryArgs().Peek("endKey")
	limitData := ctx.QueryArgs().Peek("limit")
	orderData := ctx.QueryArgs().Peek("order")

	if len(tableName) == 0 || len(hashKeyData) == 0 {
		return nil, false
	}

	table = rs.IsValidTableName(tableName)
	if table == nil {
		return nil, false
	}

	limit, ok := rs.IsValidLimit(limitData)
	if !ok {
		return nil, false
	}

	order, ok := rs.IsValidOrder(orderData)
	if !ok {
		return nil, false
	}

	var subIndexName *string
	if subIndexName = rs.IsValidIndexName(indexNameData); subIndexName != nil {
		hashKey, startKey, endKey, ok = rs.GetKeysForSubIndex(
			table, hashKeyData, startKeyData, endKeyData, *subIndexName)
		if !ok {
			return nil, false
		}
	} else {
		hashKey, startKey, endKey = rs.GetKeysForPrimary(
			table, hashKeyData, startKeyData, endKeyData)
	}

	params := &GetParams{
		TableName:    string(tableName),
		SubIndexName: subIndexName,
		HashKey:      hashKey,
		StartKey:     startKey,
		EndKey:       endKey,
		Limit:        limit,
		Order:        order,
	}

	return params, true
}
