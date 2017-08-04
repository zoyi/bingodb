package model

import "github.com/streadway/amqp"

type StreamInfo map[string]string

type Stream struct {
	adaptor interface{}
	queue   string
}

func makeStream(adaptors Adaptors, streamInfo StreamInfo) *Stream {
	if adaptor, ok := adaptors[streamInfo["adaptor"]]; ok {
		return &Stream{adaptor: adaptor, queue: streamInfo["queue"]}
	}
	return nil
}

func (stream *Stream) Emit(event *Event) {
	switch adaptor := stream.adaptor.(type) {
	case *amqp.Channel:
		adaptor.Publish(
			"",           // exchange
			stream.queue, // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        event.ToJSON(),
			})

	}
}
