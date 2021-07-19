package main

import (
	"fmt"
	"time"
	"whl/rabbit"
)

type Test struct {
}

var qe = &rabbit.QueueExchange{
	QueueName:    "AAA",
	RoutingKey:   "sss",
	ExchangeName: "test.rabbit.mq",
	ExchangeType: rabbit.Direct,
	AutoAck:      false,
}

func (Test) Register() *rabbit.QueueExchange {
	return qe
}

// 注册队列和交换机
func (Test) HandleMessage(msg []byte) error {
	time.Sleep(20 * time.Second)
	fmt.Println(1, string(msg))
	//return errors.New("ss")
	return nil
}

type Test2 struct {
}

func (Test2) Register() *rabbit.QueueExchange {
	return qe
}

func (Test2) HandleMessage(msg []byte) error {
	time.Sleep(10 * time.Second)
	fmt.Println(2, string(msg))
	return nil
}

func main() {

	err := rabbit.NewRabbitMQ(
		rabbit.RegisterHost("amqp://guest:guest@192.168.210.7:5672/"),
		rabbit.RegisterConsumer(&Test{}, &Test2{}),
	)

	if err != nil {
		fmt.Println(err)
	}

	defer func() {
		select {}
	}()
}
