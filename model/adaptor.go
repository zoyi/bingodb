package model

import (
	"github.com/streadway/amqp"
)

type AdaptorInfo struct {
	Driver string `yaml:"driver"`
	Url    string `yaml:"url"`
}

type Adaptors map[string]interface{}

func (bingo *Bingo) loadAdaptors(configInfo *ConfigInfo) error {
	adaptors := make(map[string]interface{})

	for adaptorName, adaptorInfo := range configInfo.Adaptors {
		conn, err := amqp.Dial(adaptorInfo.Url)
		if err != nil {
			return err
		}

		channel, err := conn.Channel()
		if err != nil {
			return err
		}

		adaptors[adaptorName] = channel
	}

	bingo.Adaptors = adaptors

	return nil
}
