package api

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb/model"
)

type Resource struct {
	Db *model.Bingo
	//TBD
	AccessToken string
}

type ResourceParams struct {
	TableName    string
	SubIndexName *string //optional
	HashKey      interface{}
	StartKey     interface{}
	EndKey       interface{} //optional
	Limit        int         //optional
	Order        string      //optional
}

func (rs *Resource) ValidateGetParams(ctx *fasthttp.RequestCtx) (string, string, string, bool) {
	tableName := ctx.QueryArgs().Peek("tableName")
	sortKey := ctx.QueryArgs().Peek("sortKey")
	hashKey := ctx.QueryArgs().Peek("hashKey")

	if len(tableName) == 0 || len(sortKey) == 0 || len(hashKey) == 0 {
		return "", "", "", false
	}

	tableData, _ := rs.Db.Tables.Load(string(tableName))
	if table, ok := tableData.(*model.Table); ok {
		table.Schema.Fields["hashKey"].Parse(hashKey) //validate and convert?
		table.Schema.Fields["sortKey"].Parse(sortKey) //validate and convert?
	}

	return "", "", "", false
}

func (rs *Resource) ValidateGetListParams(ctx *fasthttp.RequestCtx) (*ResourceParams, bool) {
	var hashKey, startKey, endKey interface{}

	tableName := ctx.QueryArgs().Peek("tableName")
	subIndexName := ctx.QueryArgs().Peek("subIndexName")
	hashKeyData := ctx.QueryArgs().Peek("hashKey")   //type check
	startKeyData := ctx.QueryArgs().Peek("startKey") //type check
	endKeyData := ctx.QueryArgs().Peek("endKey")     //type check
	limitData := ctx.QueryArgs().Peek("limit")
	orderData := ctx.QueryArgs().Peek("order")

	if len(tableName) == 0 || len(hashKeyData) == 0 || len(startKeyData) == 0 {
		return nil, false
	}
	fmt.Println("length 0")

	tableData, ok := rs.Db.Tables.Load(string(tableName))
	if !ok {
		return nil, false
	}
	fmt.Println("load primary table data")

	table, ok := tableData.(*model.Table)
	if !ok {
		return nil, false
	}
	fmt.Println("load primary table")

	var indexName *string = new(string)
	if len(subIndexName) == 0 {
		indexName = nil
	} else {
		*indexName = string(subIndexName)
	}

	var limit int
	if len(limitData) == 0 {
		limit = 50
	}

	var order string
	if len(orderData) == 0 {
		order = "DESC"
	}

	fmt.Printf("field %v\n", table.Schema.Fields)
	hashKey = table.PrimaryIndex.HashKey.Parse(string(hashKeyData))

	subTableData, ok := table.SubIndices.Load(indexName)
	if !ok {
		return nil, false
	}
	fmt.Println("sub table data load complete")
	subTable, ok := subTableData.(*model.Table)
	if !ok {
		return nil, false
	}
	fmt.Println("sub table load complete")
	if indexName != nil {
		hashKey = table.PrimaryIndex.HashKey.Parse(string(hashKeyData))
	} else {
		hashKey = subTable.PrimaryIndex.HashKey.Parse(string(hashKeyData))
	}
	startKey = subTable.PrimaryIndex.SortKey.Parse(string(startKeyData))
	endKey = subTable.PrimaryIndex.SortKey.Parse(string(endKeyData))

	params := &ResourceParams{
		TableName:    string(tableName),
		SubIndexName: indexName,
		HashKey:      hashKey,
		StartKey:     startKey,
		EndKey:       endKey,
		Limit:        limit,
		Order:        order,
	}

	fmt.Printf("constructed param: %v\n", params)
	return params, true
}

func (rs *Resource) fetch(
	table *model.Table,
	params ResourceParams) ([]*model.Document, bool) {
	var docs []*model.Document
	var ok bool

	if params.SubIndexName != nil {
		docs, ok = rs.fetchFromSubIndices(table, params)
	} else {
		docs, ok = rs.fetchFromPrimary(table, params)
	}

	return docs, ok
}

func (rs *Resource) fetchFromPrimary(
	table *model.Table,
	params ResourceParams) ([]*model.Document, bool) {

	var docs []*model.Document

	if params.Order == "DESC" {
		docs, _ = table.PrimaryIndex.RFetch(
			params.HashKey,
			params.StartKey,
			params.EndKey,
			params.Limit)
	} else {
		docs, _ = table.PrimaryIndex.Fetch(
			params.HashKey,
			params.StartKey,
			params.EndKey,
			params.Limit)
	}
	return docs, true
}

func (rs *Resource) fetchFromSubIndices(
	table *model.Table,
	params ResourceParams) ([]*model.Document, bool) {

	subTableData, _ := table.SubIndices.Load(*params.SubIndexName)
	if subIndex, ok := subTableData.(*model.SubIndex); ok {
		var docs []*model.Document

		startKey := model.SubSortTreeKey{
			Key:      params.StartKey,
			Document: nil,
		}

		endKey := model.SubSortTreeKey{
			Key:      params.EndKey,
			Document: nil,
		}

		if params.Order == "DESC" {
			docs, _ = subIndex.RFetch(
				params.HashKey,
				startKey,
				endKey,
				params.Limit)
		} else {
			docs, _ = subIndex.Fetch(
				params.HashKey,
				startKey,
				endKey,
				params.Limit)
		}

		return docs, true
	}

	return nil, false
}

func (rs *Resource) Get(
	tableName string,
	hashKey string,
	sortKey string) []byte {

	tableData, _ := rs.Db.Tables.Load(tableName)

	if table, ok := tableData.(*model.Table); ok {
		if doc, ok := table.PrimaryIndex.Get(hashKey, sortKey); ok {
			if json, ok := doc.ToJSON(); ok {
				return json
			}
		}
	}

	return []byte("{}") //empty result
}

func (rs *Resource) GetMultiple(params *ResourceParams) []byte {
	tableData, _ := rs.Db.Tables.Load(params.TableName)

	if table, ok := tableData.(*model.Table); ok {
		docs, ok := rs.fetch(table, *params)
		if !ok {
			return nil
		}

		dataList := []model.Data{}
		for _, data := range docs {
			dataList = append(dataList, data.GetData())
		}

		if jsonString, err := json.Marshal(dataList); err == nil {
			return jsonString
		}
	}

	return nil
}
