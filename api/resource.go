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

func newPutResult(old *bingodb.Document, newbie *bingodb.Document, replaced bool) *PutResult {
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
			query := rs.fetchScanQuery(ctx)
			var values []bingodb.Data
			var next interface{}
			
			if query.Backward {
				values, next, _ = index.RScan(query.HashKey, query.Since, query.Limit)
			} else {
				values, next, _ = index.Scan(query.HashKey, query.Since, query.Limit)
			}

			if bytes, err := json.Marshal(newListResponse(values, next)); err == nil {
				success(ctx, bytes)
			}
		} else {
			raiseError(ctx, bingodb.BingoIndexNotFoundError, "")
		}
	}
	rs.bingo.AddScan()
}

func (rs *Resource) Get(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		if index := table.Index(rs.fetchIndex(ctx)); index != nil {
			getRequest := rs.fetchGetQuery(ctx)
			document, err := index.Get(getRequest.HashKey, getRequest.SortKey)
			if err != nil {
				raiseError(ctx, err.Code, err.Message)
			} else {
				success(ctx, document.ToJSON())
			}

		} else {
			raiseError(ctx, bingodb.BingoIndexNotFoundError, "")
		}
	}
	rs.bingo.AddGet()
}

func (rs *Resource) Put(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		if data, ok := rs.fetchPutQuery(ctx); ok {
			old, newbie, replaced, err := table.Put(&data.Set, &data.SetOnInsert)
			if err != nil {
				raiseError(ctx, err.Code, err.Message)
			} else if bytes, err := json.Marshal(newPutResult(old, newbie, replaced)); err == nil {
				success(ctx, bytes)
			}
		}
	}
	rs.bingo.AddPut()
}

func (rs *Resource) Remove(ctx *fasthttp.RequestCtx) {
	if table := rs.fetchTable(ctx); table != nil {
		getRequest := rs.fetchGetQuery(ctx)
		document, err := table.Remove(getRequest.HashKey, getRequest.SortKey)
		if err != nil {
			raiseError(ctx, err.Code, err.Message)
		} else {
			success(ctx, document.ToJSON())
		}
	}
	rs.bingo.AddRemove()
}

func success(ctx *fasthttp.RequestCtx, p []byte) {
	ctx.Success("application/json", p)
}

func raiseError(ctx *fasthttp.RequestCtx, bingoErrorCode int, message string) {
	getMsgAndCode := func(defaultMsg, msg string, code int) (string, int) {
		if msg == "" {
			return defaultMsg, code
		}
		return msg, code
	}
	statusCode := 500

	switch bingoErrorCode {
	case bingodb.BingoFieldParsingError:
		message, statusCode = getMsgAndCode(bingodb.GeneralFieldError, message, fasthttp.StatusBadRequest)
	case bingodb.BingoHashKeyMissingError:
		message, statusCode = getMsgAndCode(bingodb.HashKeyMissing, message, fasthttp.StatusBadRequest)
	case bingodb.BingoSortKeyMissingError:
		message, statusCode = getMsgAndCode(bingodb.SortKeyMissing, message, fasthttp.StatusBadRequest)
	case bingodb.BingoSetOrInsertMissingError:
		message, statusCode = getMsgAndCode(bingodb.SetOrInsertMissing, message, fasthttp.StatusBadRequest)
	case bingodb.BingoDocumentNotFoundError:
		message, statusCode = getMsgAndCode(bingodb.DocumentNotFound, message, fasthttp.StatusBadRequest)
	case bingodb.BingoIndexNotFoundError:
		message, statusCode = getMsgAndCode(bingodb.IndexNotFound, message, fasthttp.StatusNotFound)
	case bingodb.BingoTableNotFoundError:
		message, statusCode = getMsgAndCode(bingodb.GeneralTableNotFound, message, fasthttp.StatusNotFound)
	case bingodb.BingoJSONParsingError:
		message, statusCode = getMsgAndCode(bingodb.GeneralParsingError, message, fasthttp.StatusBadRequest)
	default:
		message = bingodb.UnknownError
		statusCode = fasthttp.StatusInternalServerError
	}

	ctx.Error(message, statusCode)
}

func (rs *Resource) fetchTable(ctx *fasthttp.RequestCtx) *bingodb.Table {
	tableName := ctx.UserValue("table").(string)
	if table, ok := rs.bingo.Table(tableName); ok {
		return table
	}
	raiseError(ctx, bingodb.BingoTableNotFoundError, fmt.Sprintf(bingodb.TableNotFound, tableName))
	return nil
}

func (rs *Resource) fetchIndex(ctx *fasthttp.RequestCtx) string {
	if index := ctx.UserValue("index"); index != nil {
		return index.(string)
	} else {
		return ""
	}
}

func (rs *Resource) fetchScanQuery(ctx *fasthttp.RequestCtx) (query ScanQuery) {
	if query.HashKey = string(ctx.QueryArgs().Peek("hash")); len(query.HashKey.(string)) == 0 {
		query.HashKey = nil
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

func (rs *Resource) fetchGetQuery(ctx *fasthttp.RequestCtx) (request GetQuery) {
	if request.HashKey = string(ctx.QueryArgs().Peek("hash")); len(request.HashKey.(string)) == 0 {
		request.HashKey = nil
	}
	if request.SortKey = string(ctx.QueryArgs().Peek("sort")); len(request.SortKey.(string)) == 0 {
		request.SortKey = nil
	}

	return request
}

func (rs *Resource) fetchPutQuery(ctx *fasthttp.RequestCtx) (data PutQuery, ok bool) {
	if err := json.Unmarshal(ctx.PostBody(), &data); err != nil {
		raiseError(ctx, bingodb.BingoJSONParsingError, err.Error())
		return data, false
	}

	return data, true
}
