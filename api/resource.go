package api

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"github.com/zoyi/bingodb"
)

type GetQuery struct {
	HashKey interface{} `json:"hash"`
	SortKey interface{} `json:"sort,omitempty"` //optional
}

type PutQuery struct {
	Set         bingodb.Data `json:"$set"`
	SetOnInsert bingodb.Data `json:"$setOnInsert,omitempty"` //optional
}

type ScanQuery struct {
	HashKey  interface{}
	Since    []interface{}
	Limit    int
	Backward bool
}

type Resource struct {
	bingo *bingodb.Bingo
}

type ScanResult struct {
	Values interface{} `json:"values"`
	Next   interface{} `json:"next,omitempty"`
}

type PutResult struct {
	Old      interface{} `json:"old,omitempty"`
	New      interface{} `json:"new"`
	Replaced bool        `json:"replaced"`
}

func newPutReuslt(old *bingodb.Document, newbie *bingodb.Document, replaced bool) *PutResult {
	var oldDoc, newbieDoc bingodb.Data
	if old != nil {
		oldDoc = old.Data()
	}
	if newbie != nil {
		newbieDoc = newbie.Data()
	}
	return &PutResult{Old: oldDoc, New: newbieDoc, Replaced: replaced}
}

func newListResponse(values []bingodb.Data, next interface{}) *ScanResult {
	if next != nil {
		switch next.(type) {
		case bingodb.SubSortKey:
			next = next.(bingodb.SubSortKey).Array()
		}
	}

	return &ScanResult{Values: values, Next: next}
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
		if index := table.Index(rs.fetchIndex(ctx)); index != nil {
			if query, ok := rs.fetchScanQuery(ctx); ok {
				values, next := index.Scan(query.HashKey, query.Since, query.Limit)
				if bytes, err := json.Marshal(newListResponse(values, next)); err == nil {
					success(ctx, bytes)
				}
			}
		}
	}
	rs.bingo.AddScan()
}

func (rs *Resource) Get(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		if index := table.Index(rs.fetchIndex(ctx)); index != nil {
			if getRequest, ok := rs.fetchGetQuery(ctx, table.SortKey() != nil); ok {
				if document, ok := index.Get(getRequest.HashKey, getRequest.SortKey); ok {
					success(ctx, document.ToJSON())
				} else {
					raiseError(ctx, "Not found document")
				}
			}
		} else {
			raiseError(ctx, "Not found index")
		}
	}
	rs.bingo.AddGet()
}

func (rs *Resource) Put(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		data := rs.fetchPutQuery(ctx)

		old, newbie, replaced := table.Put(&data.Set, &data.SetOnInsert)
		if bytes, err := json.Marshal(newPutReuslt(old, newbie, replaced)); err == nil {
			success(ctx, bytes)
		}
	}
	rs.bingo.AddPut()
}

func (rs *Resource) Remove(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		if getRequest, ok := rs.fetchGetQuery(ctx, table.SortKey() != nil); ok {
			if document, ok := table.Remove(getRequest.HashKey, getRequest.SortKey); ok {
				success(ctx, document.ToJSON())
			} else {
				success(ctx, []byte("{}"))
			}
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

func (rs *Resource) fetchIndex(ctx *fasthttp.RequestCtx) string {
	if index := ctx.UserValue("index"); index != nil {
		return index.(string)
	} else {
		return ""
	}
}

func (rs *Resource) fetchScanQuery(ctx *fasthttp.RequestCtx) (query ScanQuery, ok bool) {
	ok = true
	if query.HashKey = string(ctx.QueryArgs().Peek("hash")); len(query.HashKey.(string)) == 0 {
		raiseError(ctx, fmt.Sprintf("Hash key missing"))
		ok = false
		return
	}

	query.Since = make([]interface{}, 3)
	for i, value := range ctx.QueryArgs().PeekMulti("since") {
		if i >= 3 {
			break
		}
		query.Since[i] = value
	}

	if query.Limit = ctx.QueryArgs().GetUintOrZero("limit"); query.Limit == 0 {
		query.Limit = 20
	}

	query.Backward = ctx.QueryArgs().GetBool("backward")

	return
}

func (rs *Resource) fetchGetQuery(ctx *fasthttp.RequestCtx, hasSortKey bool) (request GetQuery, ok bool) {
	ok = true
	if len(ctx.PostBody()) != 0 {
		if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		}
	} else {
		if request.HashKey = string(ctx.QueryArgs().Peek("hash")); len(request.HashKey.(string)) == 0 {
			raiseError(ctx, fmt.Sprintf("Hash key missing"))
			ok = false
			return
		}
		request.SortKey = ctx.QueryArgs().Peek("sort")
		if request.SortKey == nil && hasSortKey {
			raiseError(ctx, fmt.Sprintf("Sort key missing"))
			ok = false
			return
		}
		if request.SortKey != nil && !hasSortKey {
			raiseError(ctx, fmt.Sprintf("Sort key not exists"))
			ok = false
			return
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

func (rs *Resource) fetchPutQuery(ctx *fasthttp.RequestCtx) (data PutQuery) {
	if err := json.Unmarshal(ctx.PostBody(), &data); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
	}
	return
}
