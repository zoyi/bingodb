package api

import (
	"encoding/json"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/model"
	"strconv"
	"strings"
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

func (rs *Resource) IsValidTableName(tableNameData string) *model.Table {
	tableData, ok := rs.Db.Tables.Load(tableNameData)
	if !ok {
		return nil
	}

	table, ok := tableData.(*model.Table)
	if !ok {
		return nil
	}

	return table
}

func (rs *Resource) IsValidLimit(limitData []byte) (int, bool) {
	var result int
	var error error
	if len(limitData) == 0 {
		result = 20
	} else {
		result, error = strconv.Atoi(string(limitData))
		if error != nil {
			return 0, false
		}
	}

	return result, true
}

func (rs *Resource) IsValidOrder(orderData []byte) (string, bool) {
	if len(orderData) == 0 {
		return "DESC", true
	}

	orderString := strings.ToUpper(string(orderData))
	if orderString != "DESC" && orderString != "ASC" {
		return "", false
	}

	return orderString, true
}

func (rs *Resource) ValidateParams(
	ctx *fasthttp.RequestCtx) (string, interface{}, interface{}, int) {

	tableName := ctx.UserValue("tableName").(string)
	sortKeyData := ctx.QueryArgs().Peek("sortKey")
	hashKeyData := ctx.QueryArgs().Peek("hashKey")

	if len(sortKeyData) == 0 || len(hashKeyData) == 0 {
		return "", nil, nil, fasthttp.StatusBadRequest
	}

	table := rs.IsValidTableName(tableName)

	if table == nil {
		return "", nil, nil, fasthttp.StatusNotFound
	}

	hashKey := table.PrimaryIndex.HashKey.Parse(string(hashKeyData))
	sortKey := table.PrimaryIndex.SortKey.Parse(string(sortKeyData))

	return tableName, hashKey, sortKey, fasthttp.StatusOK
}

func (rs *Resource) ValidateGetListParams(ctx *fasthttp.RequestCtx) (*GetParams, int) {
	var hashKey, startKey, endKey interface{}
	var table *model.Table
	var ok bool

	tableName := ctx.UserValue("tableName").(string)
	indexNameData := ctx.QueryArgs().Peek("indexName")
	hashKeyData := ctx.QueryArgs().Peek("hashKey")
	startKeyData := ctx.QueryArgs().Peek("startKey")
	endKeyData := ctx.QueryArgs().Peek("endKey")
	limitData := ctx.QueryArgs().Peek("limit")
	orderData := ctx.QueryArgs().Peek("order")

	if len(hashKeyData) == 0 {
		return nil, fasthttp.StatusBadRequest
	}

	table = rs.IsValidTableName(tableName)
	if table == nil {
		return nil, fasthttp.StatusNotFound
	}

	limit, ok := rs.IsValidLimit(limitData)
	if !ok {
		return nil, fasthttp.StatusBadRequest
	}

	order, ok := rs.IsValidOrder(orderData)
	if !ok {
		return nil, fasthttp.StatusBadRequest
	}

	var subIndexName *string
	if len(indexNameData) == 0 {
		hashKey, startKey, endKey = rs.GetKeysForPrimary(
			table, hashKeyData, startKeyData, endKeyData)
	} else {
		subIndexName = new(string)
		*subIndexName = string(indexNameData)
		hashKey, startKey, endKey, ok = rs.GetKeysForSubIndex(
			table, hashKeyData, startKeyData, endKeyData, *subIndexName)
		if !ok {
			return nil, fasthttp.StatusBadRequest
		}
	}

	params := &GetParams{
		TableName:    tableName,
		SubIndexName: subIndexName,
		HashKey:      hashKey,
		StartKey:     startKey,
		EndKey:       endKey,
		Limit:        limit,
		Order:        order,
	}

	return params, fasthttp.StatusOK
}

func (rs *Resource) ValidatePostParams(
	ctx *fasthttp.RequestCtx) (tableName string, data model.Data, status int) {

	tableName = ctx.UserValue("tableName").(string)
	status = fasthttp.StatusOK

	table := rs.IsValidTableName(tableName)
	if table == nil {
		return "", nil, fasthttp.StatusNotFound
	}

	if err := json.Unmarshal(ctx.PostBody(), &data); err != nil {
		return "", nil, fasthttp.StatusBadRequest
	}

	if !table.Schema.IsValidData(data) {
		return "", nil, fasthttp.StatusBadRequest
	}

	return tableName, data, status
}
