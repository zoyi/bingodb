package api

import (
	"encoding/json"
	"github.com/buaazp/fasthttprouter"
	"github.com/gavv/httpexpect"
	"github.com/zoyi/bingodb"
	"net/http"
	"strings"
	"testing"
)

func initDefaultSeedData(bingo *bingodb.Bingo) {
	table, _ := bingo.Table("onlines")
	{
		var s = `{
			"channelId": "1",
			"personKey": "person1",
			"lastSeen": 123,
			"updatedAt": 1700000000000,
			"expiresAt": 2600000000000
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
			"personKey": "person2",
			"lastSeen": 129,
			"updatedAt": 1600000000000,
			"expiresAt": 2700000000000
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
			"personKey": "person3",
			"lastSeen": 132,
			"updatedAt": 1500000000000,
			"expiresAt": 2800000000000
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data bingodb.Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
	table, _ = bingo.Table("tests")
	{
		var s = `{
			"hash": 0,
			"sort": 0,
			"expiresAt": 2800000000000
		}`

		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var data bingodb.Data
		dec.Decode(&data)
		table.Put(&data, nil)
	}
}

func getRouterForTest() *fasthttprouter.Router {
	bingo := bingodb.NewBingoFromConfigFile("../config/test.yml")
	router := MakeRouter(bingo)
	initDefaultSeedData(bingo)

	return router
}

func getExpector(t *testing.T) *httpexpect.Expect {
	router := getRouterForTest()

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

func TestGetTables(t *testing.T) {
	getExpector(t).
		GET("/tables").
		Expect().Status(http.StatusOK).
		JSON().Array().Length().Equal(2)
}

func TestGetTableInfo(t *testing.T) {
	obj := getExpector(t).
		GET("/tables/onlines/info").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("name").Equal("onlines")
	obj.Value("size").Equal(3)
	obj.Value("subIndices").Object().Value("guest").Equal(3)
}

func TestGetInvalidTableInfo(t *testing.T) {
	getExpector(t).
		GET("/tables/wrong/info").
		Expect().Status(http.StatusBadRequest)
}

func TestGetWithValidParams(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "person1").
		Expect().Status(http.StatusOK).
		JSON().Object().
		ValueEqual("channelId", "1").
		ContainsKey("expiresAt").
		ContainsKey("lastSeen")
}

func TestGetWithValidParamEmptyResult(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "red").
		Expect().Status(http.StatusBadRequest)
}

func TestGetWithInvalidParams(t *testing.T) {
	expector := getExpector(t)

	expector.
		GET("/tables/onlines").
		WithQuery("hash", "1").
		Expect().Status(http.StatusBadRequest)

	expector.
		GET("/tables/onlines").
		Expect().Status(http.StatusBadRequest)

	expector.
		GET("/tables/tests").
		WithQuery("hash", "wrong").
		Expect().Status(http.StatusBadRequest)

	expector.
		GET("/tables/onlines/indices/wrong").
		WithQuery("hash", "1").
		WithQuery("limit", "20").
		Expect().Status(http.StatusBadRequest)
}

func TestScanWithValidParams(t *testing.T) {
	expector := getExpector(t)

	obj := expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("limit", "20").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(3)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person1")
	obj.Value("values").Array().Element(1).Object().Value("personKey").Equal("person2")
	obj.Value("values").Array().Element(2).Object().Value("personKey").Equal("person3")

	obj = expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("limit", "1").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person1")
	obj.Value("next").Equal("person2")

	obj = expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("since", "person2").
		WithQuery("limit", "1").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person2")
	obj.Value("next").Equal("person3")

	obj = expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("since", "person2").
		WithQuery("limit", "1").
		WithQuery("backward", 1).
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person2")
	obj.Value("next").Equal("person1")

}

func TestScanWithInvalidParams(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines/scan").
		WithQuery("limit", "20").
		Expect().Status(http.StatusBadRequest)
}

func TestScanIndexWithValidParams(t *testing.T) {
	expector := getExpector(t)

	obj := expector.
		GET("/tables/onlines/indices/guest/scan").
		WithQuery("hash", "1").
		WithQuery("limit", "20").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(3)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person3")
	obj.Value("values").Array().Element(1).Object().Value("personKey").Equal("person2")
	obj.Value("values").Array().Element(2).Object().Value("personKey").Equal("person1")

	obj = expector.
		GET("/tables/onlines/indices/guest/scan").
		WithQuery("hash", "1").
		WithQuery("limit", "1").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person3")
	subSortNext := obj.Value("next").Array()
	subSortNext.Length().Equal(3)
	subSortNext.Element(0).Equal(1600000000000)
	subSortNext.Element(1).Equal("1")
	subSortNext.Element(2).Equal("person2")

	obj = expector.
		GET("/tables/onlines/indices/guest/scan").
		WithQuery("hash", "1").
		WithQuery("since", 1600000000000).
		WithQuery("limit", "1").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person2")

	subSortNext = obj.Value("next").Array()
	subSortNext.Length().Equal(3)
	subSortNext.Element(0).Equal(1700000000000)
	subSortNext.Element(1).Equal("1")
	subSortNext.Element(2).Equal("person1")

	obj = expector.
		GET("/tables/onlines/indices/guest/scan").
		WithQuery("hash", "1").
		WithQuery("since", 1600000000001).
		WithQuery("limit", "1").
		WithQuery("backward", 1).
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person2")

	subSortNext = obj.Value("next").Array()
	subSortNext.Length().Equal(3)
	subSortNext.Element(0).Equal(1500000000000)
	subSortNext.Element(1).Equal("1")
	subSortNext.Element(2).Equal("person3")

	obj = expector.
		GET("/tables/onlines/indices/guest/scan").
		WithQuery("hash", "1").
		WithQuery("since", 1600000000000).
		WithQuery("since", "1").
		WithQuery("since", "person4").
		WithQuery("since", "dummy").
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("values").Array().Length().Equal(1)
	obj.Value("values").Array().Element(0).Object().Value("personKey").Equal("person1")
	obj.NotContainsKey("next")
}

func TestScanIndexWithInvalidName(t *testing.T) {
	getExpector(t).
		GET("/tables/onlines/indices/wrong/scan").
		WithQuery("limit", "20").
		Expect().Status(http.StatusBadRequest)
}

func makePutBody(set map[string]interface{}, setOnInsert map[string]interface{}) map[string]interface{} {
	body := make(map[string]interface{})

	if set != nil && len(set) != 0 {
		body["$set"] = set
	}
	if setOnInsert != nil && len(setOnInsert) != 0 {
		body["$setOnInsert"] = setOnInsert
	}

	return body
}

func TestPutWithValidParams(t *testing.T) {
	expector := getExpector(t)

	set := make(map[string]interface{})
	set["channelId"] = "1"
	set["personKey"] = "person4"
	set["expiresAt"] = 2800000000000
	set["updatedAt"] = 1400000000000

	obj := expector.
		PUT("/tables/onlines").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("old").Null()

	obj.Value("new").Object().
		ValueEqual("channelId", "1").
		ValueEqual("personKey", "person4").
		ValueEqual("expiresAt", 2800000000000).
		ValueEqual("updatedAt", 1400000000000)

	obj.Value("replaced").Boolean().Equal(false)

	expector.
		GET("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "person4").
		Expect().Status(http.StatusOK).
		JSON().Object().
		ValueEqual("personKey", "person4").
		ValueEqual("channelId", "1")

	expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		Expect().Status(http.StatusOK).
		JSON().Object().Value("values").
		Array().Length().Equal(4)

	delete(set, "expiresAt")
	setOnInsert := make(map[string]interface{})
	setOnInsert["expiresAt"] = 2900000000000
	set["updatedAt"] = 1500000000000

	obj = expector.
		PUT("/tables/onlines").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusOK).
		JSON().Object()

	obj.Value("old").Object().
		ValueEqual("channelId", "1").
		ValueEqual("personKey", "person4").
		ValueEqual("expiresAt", 2800000000000).
		ValueEqual("updatedAt", 1400000000000)

	obj.Value("new").Object().
		ValueEqual("channelId", "1").
		ValueEqual("personKey", "person4").
		ValueEqual("expiresAt", 2800000000000).
		ValueEqual("updatedAt", 1500000000000)

	obj.Value("replaced").Boolean().Equal(true)
}

func TestPutWithInvalidParams(t *testing.T) {
	expector := getExpector(t)

	expector.
		PUT("/tables/onlines").
		WithJSON(makePutBody(nil, nil)).
		Expect().Status(http.StatusBadRequest)

	expector.
		PUT("/tables/onlines").
		WithJSON("{ dummy }").
		Expect().Status(http.StatusBadRequest)

	set := make(map[string]interface{})
	expector.
		PUT("/tables/onlines").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusBadRequest)

	set["channelId"] = "1"
	expector.
		PUT("/tables/onlines").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusBadRequest)

	delete(set, "channelId")
	set["personKey"] = "person"
	expector.
		PUT("/tables/onlines").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusBadRequest)

	set = make(map[string]interface{})
	set["hash"] = "dummy"
	expector.
		PUT("/tables/tests").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusBadRequest)

	set["hash"] = 0
	set["sort"] = "dummy"
	expector.
		PUT("/tables/tests").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusBadRequest)

	set = make(map[string]interface{})
	set["hash"] = "1"
	set["sort"] = "4"
	set["expiresAt"] = "null"
	expector.
		PUT("/tables/tests").
		WithJSON(makePutBody(set, nil)).
		Expect().Status(http.StatusBadRequest)
}

func TestDeleteWithValidParams(t *testing.T) {
	expector := getExpector(t)

	expector.
		DELETE("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "person1").
		Expect().Status(http.StatusOK).
		JSON().Object().Value("personKey").Equal("person1")

	expector.
		DELETE("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "person1").
		Expect().Status(http.StatusOK).
		JSON().Object().Empty()

	expector.
		GET("/tables/onlines").
		WithQuery("hash", "1").
		WithQuery("sort", "person1").
		Expect().Status(http.StatusBadRequest)

	expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("limit", "20").
		Expect().Status(http.StatusOK).
		JSON().Object().Value("values").
		Array().Length().Equal(2)

	expector.
		GET("/tables/onlines/scan").
		WithQuery("hash", "1").
		WithQuery("test", "20").
		Expect().Status(http.StatusOK).
		JSON().Object().Value("values").
		Array().Length().Equal(2)
}