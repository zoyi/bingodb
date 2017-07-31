package api

import (
	"github.com/zoyi/bingodb/model"
	"testing"
)

var resource *Resource

func validationPrepare() {
	bingo := model.Load("config.yml")
	resource = &Resource{
		Db:          bingo,
		AccessToken: "",
	}
}

func TestResource_IsValidLimit(t *testing.T) {
	validationPrepare()

	result, ok := resource.IsValidLimit([]byte("123"))
	if !ok {
		t.Error("123 should be valid limit")
	}
	if result != 123 {
		t.Error("error on coverting string to int value")
	}

	_, ok = resource.IsValidLimit([]byte("haha"))
	if ok {
		t.Error("haha should be invalid limit")
	}

	_, ok = resource.IsValidLimit([]byte("123.123"))
	if ok {
		t.Error("floating should be invalid limit")
	}

	_, ok = resource.IsValidLimit([]byte("-1"))
	if !ok {
		t.Error("negative number should be valid")
	}
}

func TestResource_IsValidOrder(t *testing.T) {
	validationPrepare()

	_, ok := resource.IsValidOrder([]byte("DESC"))
	if !ok {
		t.Error("DESC should be valid order")
	}

	_, ok = resource.IsValidOrder([]byte("ASC"))
	if !ok {
		t.Error("DESC should be valid order")
	}

	result, ok := resource.IsValidOrder([]byte(""))
	if !ok {
		t.Error("empty should be valid order")
	}
	if result != "DESC" {
		t.Error("default order should be DESC")
	}

	_, ok = resource.IsValidOrder([]byte("123"))
	if ok {
		t.Error("123 should be invalid")
	}

	_, ok = resource.IsValidOrder([]byte("desc"))
	if !ok {
		t.Error("order should be case insensitive")
	}
}

func TestResource_IsValidIndexName(t *testing.T) {
	validationPrepare()

	tableData, _ := resource.Db.Tables.Load("onlines")
	table, _ := tableData.(*model.Table)

	result := resource.IsValidIndexName(table, []byte("guest"))
	if result == nil {
		t.Error("guest should be valid index name")
	}

	result = resource.IsValidIndexName(table, []byte("test"))
	if result != nil {
		t.Error("test should be invalid index name")
	}

	result = resource.IsValidIndexName(table, []byte("11"))
	if result != nil {
		t.Error("11 should be invalid index name")
	}
}

func TestResource_IsValidTableName(t *testing.T) {
	validationPrepare()

	table := resource.IsValidTableName("onlines")
	if table == nil {
		t.Error("onlines table should be valid")
	}

	table = resource.IsValidTableName("test")
	if table != nil {
		t.Error("test table should be invalid")
	}

	table = resource.IsValidTableName("123")
	if table != nil {
		t.Error("test table should be invalid")
	}
}
