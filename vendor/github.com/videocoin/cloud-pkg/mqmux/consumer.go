package mqmux

import "github.com/streadway/amqp"

type WorkerConsumer struct {
	Ch      *amqp.Channel
	D       <-chan amqp.Delivery
	Handler WorkerHandlerFunc

	async bool
}
