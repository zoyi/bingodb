package api

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb"
)

type GetRequest struct {
	IndexName string      `json:"index,omitempty"`
	HashKey   interface{} `json:"hash"`
	SortKey   interface{} `json:"sort,omitempty"` //optional
}

type GetQuery struct {
	indexName string
	hashKey   interface{}
	sortKey   interface{}
}

type Resource struct {
	bingo *bingodb.Bingo
}

type ListResponse struct {
	Values interface{} `json:"values"`
	Next   interface{} `json:"next,omitempty"`
}

func (rs *Resource) Tables(ctx *fasthttp.RequestCtx) {
	if bytes, err := json.Marshal(rs.bingo.TablesArray()); err == nil {
		success(ctx, bytes)
	}
}

func (rs *Resource) TableInfo(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		if bytes, err := json.Marshal(table.Info()); err == nil {
			success(ctx, bytes)
		}
	}
}

func (rs *Resource) Scan(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		query := rs.fetchScanQuery(ctx, table)
		res, next := table.PrimaryIndex().Scan(query.HashKey, query.Since, query.Limit)
		if bytes, err := json.Marshal(ListResponse{Values: res, Next: next}); err == nil {
			success(ctx, bytes)
		}
		//if index := table.Index(getRequest.IndexName); index != nil {
		//	if document, ok := index.Get(getRequest.HashKey, getRequest.SortKey); ok {
		//		success(ctx, document.ToJSON())
		//	} else {
		//		raiseError(ctx, "Not found document")
		//	}
		//} else {
		//	raiseError(ctx, "Not found index")
		//}
	}
	rs.bingo.AddScan()
}

func (rs *Resource) Get(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		getRequest := rs.fetchGetRequest(ctx, table)
		if index := table.Index(getRequest.IndexName); index != nil {
			if document, ok := index.Get(getRequest.HashKey, getRequest.SortKey); ok {
				success(ctx, document.ToJSON())
			} else {
				raiseError(ctx, "Not found document")
			}
		} else {
			raiseError(ctx, "Not found index")
		}
	}
	rs.bingo.AddGet()
}

func (rs *Resource) Put(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		data := rs.fetchData(ctx)

		if document, replaced := table.Put(&data); replaced {
			success(ctx, document.ToJSON())
		} else {
			success(ctx, []byte("{}"))
		}
	}
	rs.bingo.AddPut()
}

func (rs *Resource) Remove(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		getRequest := rs.fetchGetRequest(ctx, table)
		if document, ok := table.Remove(getRequest.HashKey, getRequest.SortKey); ok {
			success(ctx, document.ToJSON())
		} else {
			success(ctx, []byte("{}"))
		}
	}
	rs.bingo.AddRemove()
}

////

func success(ctx *fasthttp.RequestCtx, p []byte) {
	ctx.Success("application/json", p)
}

func raiseError(ctx *fasthttp.RequestCtx, error string) {
	ctx.Error(error, fasthttp.StatusBadRequest)
}

func (rs *Resource) fetchTable(ctx *fasthttp.RequestCtx) *bingodb.Table {
	tableName := ctx.UserValue("table").(string)
	if table, ok := rs.bingo.Table(tableName); ok {
		return table
	}
	raiseError(ctx, fmt.Sprintf("Table not found: %s", tableName))
	return nil
}

func (rs *Resource) fetchScanQuery(ctx *fasthttp.RequestCtx, table *bingodb.Table) (query ScanQuery) {
	hashKey := table.HashKey()
	sortKey := table.SortKey()

	query.HashKey = hashKey.Parse(string(ctx.QueryArgs().Peek(hashKey.Name)))

	query.Since = make([]interface{}, 2)

	if sortKey != nil {
		for i, value := range ctx.QueryArgs().PeekMulti("since") {
			if i >= 2 {
				break
			}
			query.Since[i] = sortKey.Parse(value)
		}
	}

	query.IndexName = string(ctx.QueryArgs().Peek("index"))

	if query.Limit = ctx.QueryArgs().GetUintOrZero("limit"); query.Limit == 0 {
		query.Limit = 20
	}

	query.Backward = ctx.QueryArgs().GetBool("backward")

	return query
}

func (rs *Resource) fetchGetRequest(ctx *fasthttp.RequestCtx, table *bingodb.Table) (request GetRequest) {
	if len(ctx.PostBody()) != 0 {
		if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		}
	} else {
		hashKey := table.HashKey()
		sortKey := table.SortKey()

		request.IndexName = string(ctx.QueryArgs().Peek("index"))
		request.HashKey = hashKey.Parse(ctx.QueryArgs().Peek(hashKey.Name))
		if sortKey != nil {
			request.SortKey = sortKey.Parse(ctx.QueryArgs().Peek(sortKey.Name))
		}
	}
	return
}

func (rs *Resource) fetchData(ctx *fasthttp.RequestCtx) (data bingodb.Data) {
	if err := json.Unmarshal(ctx.PostBody(), &data); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
	}
	return
}
