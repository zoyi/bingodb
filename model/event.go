package model

import "encoding/json"

type Event struct {
	Event string `json:"event"`
	Ts    int64  `json:"ts"`
	Doc   Data   `json:"doc"`
}

func (event *Event) ToJSON() []byte {
	bytes, _ := json.Marshal(event)
	return bytes
}
