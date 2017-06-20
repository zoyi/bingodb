package bingodb

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestJson(t *testing.T) {
	var jsonBlob = []byte(`[
		{"Name": "Platypus", "Order": "Monotremata"},
		{"Name": "Quoll",    "Order": "Dasyuromorphia"}
	]`)
	type Animal struct {
		Name  string
		Order string
	}
	var animals []Animal
	err := json.Unmarshal(jsonBlob, &animals)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Printf("%+v", animals)
}

func TestJsonAnonymous(t *testing.T) {
	s := `{"Name": "Platypus", "Order": "Monotremata", "ts": 123, "bool": true}`

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()

	var tmp map[string]interface{}
	dec.Decode(&tmp)
	fmt.Println("%+v", tmp)

	//for {
	//	t, err := dec.Token()
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	fmt.Printf("%T: %v", t, t)
	//	if dec.More() {
	//		fmt.Printf(" (more)")
	//	}
	//	fmt.Printf("\n")
	//}

	//var jsonBlob = []byte(s)
	//type Animal struct {
	//	Name  string
	//	Order string
	//	ts int64
	//}
	//
	//var tmp map[string]json.RawMessage
	//err := json.Unmarshal(jsonBlob, &tmp)
	//if err != nil {
	//	fmt.Println("error:", err)
	//}
	//fmt.Println("%+v", tmp)
	//
	//fmt.Println(reflect.TypeOf(tmp["ts"]))
	//
	//dec := json.NewDecoder(strings.NewReader(s))
	//
	//// fmt.Println(dec.Token())
	//
	//type Message struct {
	//	Name, Text string
	//}
	//
	//for {
	//	t, err := dec.Token()
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	fmt.Printf("%T: %v", t, t)
	//	if dec.More() {
	//		fmt.Printf(" (more)")
	//	}
	//	fmt.Printf("\n")
	//}

	//if _, ok := tmp["ts"].(int64); ok {
	//	fmt.Println("it's int32")
	//} else {
	//	fmt.Println("type should be a int")
	//}
}
