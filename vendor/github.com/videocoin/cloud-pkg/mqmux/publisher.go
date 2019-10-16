package mqmux

import "github.com/streadway/amqp"

type Publisher struct {
	Ch *amqp.Channel
	Q  *amqp.Queue
}
