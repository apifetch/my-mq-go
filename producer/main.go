package main

import (
	"fmt"
	"log"
	"time"
	"whl/rabbit"
)
var qe = &rabbit.QueueExchange{
	QueueName:    "AAA",
	RoutingKey:   "sss",
	ExchangeName: "test.rabbit.mq",
	ExchangeType: "direct",
	AutoAck:      false,
}

func main() {

	err := rabbit.NewRabbitMQ(
		rabbit.RegisterHost("amqp://guest:guest@192.168.210.7:5672/"),
	)

	if err != nil {
		fmt.Println(err)
	}

	defer func() {
		select {}
	}()
	go func() {
		for {
			time.Sleep(5 * time.Second)
			err := rabbit.Publish(*qe, []byte(time.Now().Format("2006-01-02 15:04:5")))
			if err != nil {
				log.Println(err)
			}
		}
	}()
}
