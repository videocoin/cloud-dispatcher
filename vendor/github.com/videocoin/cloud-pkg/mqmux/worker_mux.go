package mqmux

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type WorkerHandlerFunc func(amqp.Delivery) error

type WorkerMux struct {
	conn       *amqp.Connection
	consumers  map[string]*WorkerConsumer
	publishers map[string]*Publisher
	Logger     *logrus.Entry
}

func NewWorkerMux(uri string, connPrefix string) (*WorkerMux, error) {
	hn, _ := os.Hostname()

	config := amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Properties: amqp.Table{
			"connection_name": fmt.Sprintf("%s:%s", connPrefix, hn),
		},
	}

	conn, err := amqp.DialConfig(uri, config)
	if err != nil {
		return nil, err
	}

	mux := new(WorkerMux)
	mux.conn = conn
	mux.consumers = map[string]*WorkerConsumer{}
	mux.publishers = map[string]*Publisher{}
	mux.Logger = logrus.New().WithField("system", "mq")
	return mux, nil
}

func (m *WorkerMux) Consumer(name string, prefetchCount int, async bool, handler WorkerHandlerFunc) error {
	ch, err := m.conn.Channel()
	if err != nil {
		return err
	}

	err = ch.Qos(prefetchCount, 0, false)
	if err != nil {
		return err
	}

	d, err := ch.Consume(
		name,  // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	m.consumers[name] = &WorkerConsumer{
		Ch:      ch,
		D:       d,
		Handler: handler,
		async:   async,
	}

	return nil
}

func (m *WorkerMux) Publisher(name string) error {
	ch, err := m.conn.Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		name,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	m.publishers[name] = &Publisher{
		Ch: ch,
		Q:  &q,
	}

	return nil
}

func (m *WorkerMux) Run() error {
	m.Logger.Info("running consumers")

	errCh := make(chan error, 1)
	for name, wc := range m.consumers {
		go func(name string, wc *WorkerConsumer) {
			errCh <- m.consume(name, wc)
		}(name, wc)
	}

	select {
	case err := <-errCh:
		return err
	}
}

func (m *WorkerMux) Close() error {
	for name, c := range m.consumers {
		err := c.Ch.Close()
		if err != nil {
			m.Logger.Errorf("failed to close consumer %s: %s", name, err)
		}
	}

	err := m.conn.Close()
	if err != nil {
		m.Logger.Errorf("failed to close connection: %s", err)
	}

	return nil
}

func (m *WorkerMux) Publish(name string, message interface{}) error {
	p, ok := m.publishers[name]
	if !ok {
		return errors.New("unknown publisher")
	}

	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = p.Ch.Publish(
		"",       // exchange
		p.Q.Name, // routing key
		false,    // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		})

	return err
}

func (m *WorkerMux) PublishX(name string, message interface{}, headers amqp.Table) error {
	p, ok := m.publishers[name]
	if !ok {
		return errors.New("unknown publisher")
	}

	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = p.Ch.Publish(
		"",       // exchange
		p.Q.Name, // routing key
		false,    // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Headers:      headers,
		})

	return err
}

func (m *WorkerMux) consume(name string, c *WorkerConsumer) error {
	l := m.Logger.WithField("consumer", name)
	l.Info("starting consumer")

	for d := range c.D {
		l.Info("performing message")

		if c.async {
			go func(l *logrus.Entry, c *WorkerConsumer, d amqp.Delivery) {
				err := c.Handler(d)
				if err != nil {
					l.Errorf("failed to perform message: %s", err.Error())
					d.Reject(false)
					return
				}
				d.Ack(false)
			}(l, c, d)
		} else {
			err := c.Handler(d)
			if err != nil {
				l.Errorf("failed to perform message: %s", err.Error())
				d.Reject(false)
				continue
			}

			d.Ack(false)
		}
	}

	return nil
}
