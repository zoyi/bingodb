package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zoyi/bingodb"
	"net/http"
	"strconv"
)

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

func (rs *Resource) Tables(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, rs.bingo.TablesArray())
}

func (rs *Resource) TableInfo(ctx *gin.Context) {
	if table, ok := rs.bingo.Table(ctx.Param("table")); ok {
		ctx.JSON(http.StatusOK, table.Info())
	} else {
		ctx.Error(errors.New(TableNotFound))
	}
}

func (rs *Resource) Get(ctx *gin.Context) {
	if table, ok := rs.bingo.Table(ctx.Param("table")); ok {
		if index := table.Index(ctx.Param("index")); index != nil {
			if document, err := index.Get(ctx.Query("hash"), ctx.Query("sort")); err == nil {
				ctx.JSON(http.StatusOK, document.Data())
			} else {
				ctx.Error(err)
			}

		} else {
			ctx.Error(errors.New(IndexNotFound))
		}

	} else {
		ctx.Error(errors.New(TableNotFound))
	}
	rs.bingo.AddGet()
}

func (rs *Resource) Scan(ctx *gin.Context) {
	if table, ok := rs.bingo.Table(ctx.Param("table")); ok {
		if index := table.Index(ctx.Param("index")); index != nil {
			query := rs.fetchScanQuery(ctx)
			var values []bingodb.Data
			var next interface{}

			if query.Backward {
				values, next, _ = index.RScan(query.HashKey, query.Since, query.Limit)
			} else {
				values, next, _ = index.Scan(query.HashKey, query.Since, query.Limit)
			}

			ctx.JSON(http.StatusOK, newListResponse(values, next))
		} else {
			ctx.Error(errors.New(IndexNotFound))
		}

	} else {
		ctx.Error(errors.New(TableNotFound))
	}
	rs.bingo.AddScan()
}

func (rs *Resource) Put(ctx *gin.Context) {
	if table, ok := rs.bingo.Table(ctx.Param("table")); ok {
		decoder := json.NewDecoder(ctx.Request.Body)
		var query PutQuery
		if err := decoder.Decode(&query); err == nil {
			if old, newbie, replaced, err := table.Put(&query.Set, &query.SetOnInsert); err == nil {
				ctx.JSON(http.StatusOK, newPutResult(old, newbie, replaced))
			} else {
				ctx.Error(err)
			}
		} else {
			ctx.Error(err)
		}
	} else {
		ctx.Error(errors.New(TableNotFound))
	}
	rs.bingo.AddPut()
}

func (rs *Resource) Remove(ctx *gin.Context) {
	if table, ok := rs.bingo.Table(ctx.Param("table")); ok {
		if document, err := table.Remove(ctx.Query("hash"), ctx.Query("sort")); err == nil {
			ctx.JSON(http.StatusOK, document.Data())
		} else {
			ctx.Error(err)
		}
	} else {
		ctx.Error(errors.New(TableNotFound))
	}
	rs.bingo.AddRemove()
}

func (rs *Resource) fetchTable(ctx *gin.Context) *bingodb.Table {
	tableName := ctx.Param("table")
	if table, ok := rs.bingo.Table(tableName); ok {
		return table
	} else {
		ctx.Error(fmt.Errorf(TableNotFound, tableName))
		return nil
	}
}

func (rs *Resource) fetchScanQuery(ctx *gin.Context) (query ScanQuery) {
	if value, ok := ctx.GetQuery("hash"); ok {
		query.HashKey = string(value)
	}

	query.Since = make([]interface{}, 3)

	if ary, ok := ctx.GetQueryArray("since"); ok {
		for i, value := range ary {
			if i >= 3 {
				break
			}
			query.Since[i] = value
		}
	}

	if value, ok := ctx.GetQuery("limit"); ok {
		query.Limit, _ = strconv.Atoi(value)
	} else {
		query.Limit = 20
	}

	if value, ok := ctx.GetQuery("backward"); ok {
		query.Backward, _ = strconv.ParseBool(value)
	} else {
		query.Backward = false
	}

	return
}
