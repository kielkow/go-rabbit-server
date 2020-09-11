package queueutils

import (
	"fmt"
	"log"
	"os"

	"github.com/streadway/amqp"
)

// SensorDiscoveryExchange const
const SensorDiscoveryExchange = os.Getenv("SENSOR_DISCOVERY_EXCHANGE")
const PersistReadingsQueue = os.Getenv("PERSIST_READINGS_QUEUE")

// GetChannel func
func GetChannel(url string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(url)

	failOnError(err, "Failed to establish connection to message broker")

	ch, err := conn.Channel()

	failOnError(err, "Failed to get channel for connection")

	return conn, ch
}

// GetQueue func
func GetQueue(name string, ch *amqp.Channel, autoDelete bool) *amqp.Queue {
	q, err := ch.QueueDeclare(
		name,
		false,
		autoDelete,
		false,
		false,
		nil)

	failOnError(err, "Failed to declare queue")

	return &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err.Error)
		panic(fmt.Sprintf("%s: %s", msg, err.Error))
	}
}

func (ql *QueueListener) DiscoverSensors() {
	ql.ch.ExchangeDeclare(
		queueutils.SensorDiscoveryExchange,
		"fanout",
		false,
		false,
		false,
		false,
		nil) args amqp.Table)
	
	ql.ch.Publish(
		queueutils.SensorDiscoveryExchange,
		"",
		false,
		false,
		amqp.Publishing{})
}