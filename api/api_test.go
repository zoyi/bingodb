package api


import (
	"github.com/gavv/httpexpect"
	"net/http"
	"testing"
	"github.com/zoyi/bingodb"
	"encoding/json"
	"strings"
)

var (
	bingo = bingodb.NewBingoFromConfigFile("../config/test.yml")
	router = MakeRouter(bingo)
)

func initDefaultSeedData() {
	table, _ := bingo.Table("onlines")
	{
		var s = `{
			"channelId": "1",
			"id": "1",
			"personKey": "person1",
			"lastSeen": 123,
			"expiresAt": 2600000000
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data bingodb.Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
	{
		var s = `{
			"channelId": "1",
			"id": "2",
			"personKey": "person2",
			"lastSeen": 129,
			"expiresAt": 2700000000
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data bingodb.Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
	{
		var s = `{
			"channelId": "1",
			"id": "3",
			"personKey": "person3",
			"lastSeen": 132,
			"expiresAt": 2800000000
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data bingodb.Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
}

func getExpector(t *testing.T) *httpexpect.Expect {
	initDefaultSeedData()

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
		GET("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "person1").
		Expect().Status(http.StatusOK).
		JSON().Object().
		ValueEqual("id", "1").
		ValueEqual("channelId", "1").
		ContainsKey("expiresAt").
		ContainsKey("lastSeen")
}

func TestGetWithInvalidParams(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines").
		WithQuery("hash", "1").
		Expect().Status(http.StatusBadRequest).
		Body().Contains("Not found document")
}

func TestGetWithValidParamEmptyResult(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "red").
		Expect().Status(http.StatusBadRequest).
		Body().Contains("Not found document")
}

func TestScanWithValidParams(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("limit", "20").
		Expect().Status(http.StatusOK).
		JSON().Object().Value("values").
		Array().Length().Equal(3)
}

func TestScanWithInvalidParams(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines/scan").
		WithQuery("limit", "20").
		Expect().Status(http.StatusBadRequest)
}

func TestScanIndexWithInvalidName(t *testing.T)	{
	getExpector(t).
		GET("/tables/onlines/indices/wrong/scan").
		WithQuery("limit", "20").
		Expect().Status(http.StatusBadRequest)
}


//func TestUpdateWithValidParams(t *testing.T) {
//	body := make(map[string]interface{})
//	body["channelId"] = "1"
//	body["id"] = "testing"
//	body["expiresAt"] = 123123
//	getExpector(t).
//		POST("/update/onlines").
//		WithJSON(body).
//		Expect().Status(http.StatusOK).
//		JSON().Object().
//		ValueEqual("id", "testing").
//		ValueEqual("channelId", "1").
//		ValueEqual("expiresAt", 123123)
//
//	getExpector(t).
//		GET("/get/onlines").
//		WithQuery("hashKey", "1").
//		WithQuery("sortKey", "testing").
//		Expect().Status(http.StatusOK).
//		JSON().Object().
//		ValueEqual("id", "testing").
//		ValueEqual("channelId", "1")
//}
//
//func TestUpdateWithInValidParams(t *testing.T) {
//	body := make(map[string]interface{})
//	body["channelId"] = "1"
//	body["id"] = "testing"
//	getExpector(t).
//		POST("/update/onlines").
//		WithJSON(body).
//		Expect().Status(http.StatusBadRequest)
//}
//
//func TestDeleteWithValidParams(t *testing.T) {
//	getExpector(t).
//		DELETE("/delete/onlines").
//		WithQuery("hashKey", "1").
//		WithQuery("sortKey", "what").
//		Expect().Status(http.StatusOK)
//
//	getExpector(t).
//		GET("/get/onlines").
//		WithQuery("hashKey", "1").
//		WithQuery("sortKey", "what").
//		Expect().Status(http.StatusOK).
//		JSON().Object().Empty()
//}
//
//func TestDeleteWithInvalidParams(t *testing.T) {
//	getExpector(t).
//		DELETE("/delete/onlines").
//		WithQuery("hashKey", "1").
//		Expect().Status(http.StatusBadRequest)
//}
