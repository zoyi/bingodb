package api

import (
	"github.com/gavv/httpexpect"
	"net/http"
	"testing"
)

var (
	router = DefaultRouter("test_config.yml")
)

func getExpector(t *testing.T) *httpexpect.Expect {
	e := httpexpect.WithConfig(httpexpect.Config{
		Reporter: httpexpect.NewAssertReporter(t),
		Client: &http.Client{
			Transport: httpexpect.NewFastBinder(router.Handler),
		},
		Printers: []httpexpect.Printer{
			httpexpect.NewCurlPrinter(t),
			httpexpect.NewDebugPrinter(t, true),
		},
	})

	return e
}

func TestGetWithValidParams(t *testing.T) {
	getExpector(t).
		GET("/get/onlines").
		WithQuery("hashKey", "1").
		WithQuery("sortKey", "what").
		Expect().Status(http.StatusOK).
		JSON().Object().
		ValueEqual("id", "what").
		ValueEqual("channelId", "1").
		ContainsKey("expiresAt").
		ContainsKey("lastSeen")
}

func TestGetWithInvalidParams(t *testing.T) {
	getExpector(t).
		GET("get/onlines").
		WithQuery("hashKey", "1").
		Expect().Status(http.StatusBadRequest)
}

func TestGetWithValidParamEmptyResult(t *testing.T) {
	getExpector(t).
		GET("/get/onlines").
		WithQuery("hashKey", "1").
		WithQuery("sortKey", "red").
		Expect().Status(http.StatusOK).
		JSON().Object().Empty()
}

func TestGetsWithValidParams(t *testing.T) {
	getExpector(t).
		GET("gets/onlines").
		WithQuery("indexName", "guest").
		WithQuery("hashKey", "1").
		WithQuery("startKey", "120").
		WithQuery("endKey", "130").
		WithQuery("limit", "20").
		WithQuery("order", "DESC").
		Expect().Status(http.StatusOK).
		JSON().Array().
		Length().Equal(2)
}

func TestGetsWithInvalidParams(t *testing.T) {
	getExpector(t).
		GET("gets/onlines").
		WithQuery("indexName", "guest").
		WithQuery("startKey", "120").
		WithQuery("endKey", "130").
		WithQuery("limit", "20").
		WithQuery("order", "DESC").
		Expect().Status(http.StatusBadRequest)
}
