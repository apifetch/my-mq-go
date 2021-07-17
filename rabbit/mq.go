package rabbit

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Producer struct {
	channel *amqp.Channel
}

type Consumer interface {
	Register() *QueueExchange   // 注册队列和交换机
	HandleMessage([]byte) error // 处理消息
}

type ExType string

const (
	Direct  ExType = "direct"
	Fanout  ExType = "fanout"
	Topic   ExType = "topic"
	Headers ExType = "headers"
)

// QueueExchange 定义队列交换机对象
type QueueExchange struct {
	QueueName    string // 队列名称
	RoutingKey   string // key值
	ExchangeName string // 交换机名称
	ExchangeType ExType // 交换机类型
	AutoAck      bool   // 是否自动应答，默认为false
}

var rabbitMQ *RabbitMQ

type RabbitMQ struct {
	host string

	connection *amqp.Connection

	consumers       []Consumer       // 消费者
	connCloseNotify chan *amqp.Error // conn关闭通知管道

	channels []*RabbitChan
	producer *amqp.Channel

	signalChan chan os.Signal

	exitChan  chan struct{}
	exitCount int32

	mux sync.Mutex
}

type RabbitChan struct {
	state    int32 // 状态： 1正常状态 2消息处理中
	isClose  int32 // 是否关闭：0 正常状态 1关闭状态
	channel  *amqp.Channel
	consumer Consumer
}

type OptionsHandler func(*RabbitMQ) error

// RegisterHost 注册mq连接字符串
func RegisterHost(host string) OptionsHandler {
	return func(mq *RabbitMQ) error {
		mq.host = host
		return connect(mq)
	}
}

// RegisterConsumer 注册消费者
func RegisterConsumer(consumers ...Consumer) OptionsHandler {
	return func(mq *RabbitMQ) error {
		mq.consumers = append(mq.consumers, consumers...)
		return nil
	}
}

// NewRabbitMQ 初始化mq
func NewRabbitMQ(opts ...OptionsHandler) error {
	rabbitMQ = newRabbitMQ()

	for _, v := range opts {
		if err := v(rabbitMQ); err != nil {
			return err
		}
	}

	go func() {
		rabbitMQ.watchClose()
	}()

	if len(rabbitMQ.consumers) > 0 {
		rabbitMQ.watchSignal()
	}
	return rabbitMQ.registerConsumer()
}

func newRabbitMQ() *RabbitMQ {
	return &RabbitMQ{
		connCloseNotify: make(chan *amqp.Error),
		signalChan:      make(chan os.Signal, 1),
		exitChan:        make(chan struct{}, 1),
	}
}

func connect(rabbitMQ *RabbitMQ) error {
	conn, err := amqp.Dial(rabbitMQ.host)
	if err != nil {
		return err
	}

	rabbitMQ.exitCount = 0
	rabbitMQ.connection = conn

	ch, err := rabbitMQ.connection.Channel()
	if err != nil {
		return err
	}

	rabbitMQ.producer = ch

	go func() {
		// 监听连接断开错误
		rabbitMQ.connCloseNotify = rabbitMQ.connection.NotifyClose(make(chan *amqp.Error))
	}()

	return nil
}

func (r *RabbitMQ) reconnect() error {
	err := connect(r)
	if err != nil {
		return err
	}

	r.channels = nil

	return r.registerConsumer()
}

func (r *RabbitMQ) watchClose() {
	for {
		select {
		case err := <-r.connCloseNotify:
			log.Println("连接断开，8秒后重连......", err)
			time.Sleep(8 * time.Second)
			log.Println("开始重连......", err)
			_ = r.reconnect()
		case <-r.signalChan:
			fmt.Printf("开始退出前清理\n")
			for _, v := range r.channels {
				atomic.SwapInt32(&v.isClose, 1)
			}

		case <-r.exitChan:
			atomic.AddInt32(&r.exitCount, 1)
			fmt.Println("一个管道完成执行准备退出")
			fmt.Println(r.exitCount)

			if atomic.LoadInt32(&r.exitCount) >= int32(len(r.channels)) {
				fmt.Println("开始退出")
				os.Exit(9)
			}
		}
	}
}

func (r *RabbitMQ) watchSignal() {
	signal.Notify(r.signalChan, syscall.SIGINT, syscall.SIGTERM)
}

// 消费者处理函数
func (r *RabbitMQ) handleMessage(rabbitChan *RabbitChan) {
	// 用于检查队列是否存在,已经存在不需要重复声明
	queueExchange := rabbitChan.consumer.Register()
	q, err := rabbitChan.channel.QueueDeclare(
		queueExchange.QueueName,
		true,
		queueExchange.AutoAck,
		false,
		true,
		nil)
	if err != nil {
		log.Println(err)
	}

	// 声明交换机
	err = rabbitChan.channel.ExchangeDeclare(
		queueExchange.ExchangeName,
		string(queueExchange.ExchangeType),
		true,
		queueExchange.AutoAck,
		false,
		true,
		nil,
	)

	if err != nil {
		log.Println(err)
	}

	// 绑定交换机
	err = rabbitChan.channel.QueueBind(q.Name, queueExchange.RoutingKey, queueExchange.ExchangeName, true, nil)
	if err != nil {
		log.Println(err)
	}

	// 获取消费通道,确保rabbitMQ一个一个发送消息
	err = rabbitChan.channel.Qos(1, 0, true)
	if err != nil {
		log.Println(err)
	}

	msgChan, err := rabbitChan.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Println(err)
	}

	for msg := range msgChan {
		if atomic.LoadInt32(&rabbitChan.isClose) == 0 {
			//atomic.SwapInt32(&rabbitChan.state, 2)
			err = rabbitChan.consumer.HandleMessage(msg.Body)
			if err != nil {
				_ = msg.Reject(true)
				log.Println(err)
			} else {
				if !queueExchange.AutoAck {
					err = msg.Ack(false)
					if err != nil {
						_ = msg.Reject(true)
						log.Println(err)
					}
				}
			}

			//atomic.SwapInt32(&rabbitChan.state, 1)
		} else {
			_ = rabbitChan.channel.Close()
			rabbitMQ.exitChan <- struct{}{}
		}
	}
}

func (r *RabbitMQ) sendMessage(queueExchange QueueExchange, msg []byte) error {
	// 用于检查队列是否存在,已经存在不需要重复声明
	q, err := r.producer.QueueDeclare(
		queueExchange.QueueName,
		true,
		queueExchange.AutoAck,
		false,
		true,
		nil)
	if err != nil {
		return err
	}

	// 声明交换机
	err = r.producer.ExchangeDeclare(
		queueExchange.ExchangeName,
		string(queueExchange.ExchangeType),
		true,
		queueExchange.AutoAck,
		false,
		true,
		nil,
	)

	if err != nil {
		return err
	}

	// 绑定交换机
	err = r.producer.QueueBind(q.Name, queueExchange.RoutingKey, queueExchange.ExchangeName, true, nil)
	if err != nil {
		return err
	}

	// 获取消费通道,确保rabbitMQ一个一个发送消息
	err = r.producer.Qos(1, 0, true)
	if err != nil {
		return err
	}

	err = r.producer.Publish(queueExchange.ExchangeName, queueExchange.RoutingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         msg,
	})

	return err
}

func (r *RabbitMQ) registerConsumer() error {
	for _, v := range r.consumers {
		ch, err := r.connection.Channel()
		if err != nil {
			return err
		}

		rc := &RabbitChan{
			channel:  ch,
			consumer: v,
			state:    1,
			isClose:  0,
		}

		r.channels = append(r.channels, rc)

		go r.handleMessage(rc)
	}

	return nil
}

func Publish(qe QueueExchange, msg []byte) error {
	return rabbitMQ.sendMessage(qe, msg)
}
