package api

import (
	"encoding/json"
	"github.com/zoyi/bingodb/model"
)

type Resource struct {
	Db *model.Bingo
	//TBD
	AccessToken string
}

type GetParams struct {
	TableName    string
	SubIndexName *string //optional
	HashKey      interface{}
	StartKey     interface{}
	EndKey       interface{} //optional
	Limit        int         //optional
	Order        string      //optional
}

func (rs *Resource) Get(
	tableName string,
	hashKey, sortKey interface{}) []byte {

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

func (rs *Resource) GetMultiple(params *GetParams) []byte {
	tableData, _ := rs.Db.Tables.Load(params.TableName)

	if table, ok := tableData.(*model.Table); ok {
		docs, ok := rs.Fetch(table, *params)
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

func (rs *Resource) Fetch(
	table *model.Table,
	params GetParams) ([]*model.Document, bool) {
	var docs []*model.Document
	var ok bool

	if params.SubIndexName != nil {
		docs, ok = rs.FetchFromSubIndices(table, params)
	} else {
		docs, ok = rs.FetchFromPrimary(table, params)
	}

	return docs, ok
}

func (rs *Resource) FetchFromPrimary(
	table *model.Table,
	params GetParams) ([]*model.Document, bool) {

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

func (rs *Resource) FetchFromSubIndices(
	table *model.Table,
	params GetParams) ([]*model.Document, bool) {

	subIndexData, _ := table.SubIndices.Load(*params.SubIndexName)
	if subIndex, ok := subIndexData.(*model.SubIndex); ok {
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
