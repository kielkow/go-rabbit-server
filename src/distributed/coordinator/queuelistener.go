package coordinator

import (
	"encoding/gob"
	"github.com/streadway/amqp"
	"distributed/queueutils"
	"bytes"
	"enconding/gob"
	"distributed/dto"
	"fmt"
)

var url = os.Getenv("AMQP_URL")

// QueueListener func
type QueueListener struct {
	conn 	*amqp.Connection
	ch 		*amqp.Channel
	sources map[string]<-chan amqp.Delivery
}

// NewQueueListener func
func NewQueueListener() *QueueListener {
	ql := QueueListener{
		sources: make(map[string]<-chan amqp.Delivery)
	}

	ql.conn, ql.ch = queueutils.GetChannel(url)

	return &ql
}

// ListenForNewSource func
func (ql *QueueListener) ListenForNewSource() {
	q := queueutils.GetQueue("", ql.ch)

	ql.ch.QueueBind(
		q.Name,
		"",
		"amq.fanout",
		false,
		nil)

	msgs, _ := ql.ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil)

	for msg := range msgs {
		sourceChan, _ := ql.ch.Consume(
			string(msg.Body),
			"",
			true,
			false,
			false,
			false,
			nil)
		
		if ql.sources[string(msg.Body)] == nil {
			ql.sources[string(msg.Body)] = sourceChan

			go ql.AddListener(sourceChan)
		}
	}
}

func (ql *QueueListener) AddListener(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		r := bytes.NewReader(msg.Body)

		d := gob.NewDecoder(r)

		sd := new(dto.SensorMessage)

		d.Decode(sd)

		fmt.Printf("Received message: %v\n", sd)
	}
}