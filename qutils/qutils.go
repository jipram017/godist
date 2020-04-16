package qutils

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

const SensorDiscoveryExchange = "SensorDiscovery"
const PersistentDataQueue = "PersistentData"
const WebappSourceExchange = "WebappSources"
const WebappReadingsExchange = "WebappReading"
const WebappDiscoveryQueue = "WebappDiscovery"

func GetChannel(url string) (*amqp.Channel, *amqp.Connection) {
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to establish connection to message broker")
	channel, err := conn.Channel()
	failOnError(err, "Failed to get channel for a connection")
	return channel, conn
}

func GetQueue(name string, ch *amqp.Channel, autoDelete bool) *amqp.Queue {
	q, err := ch.QueueDeclare(
		name,
		false,
		autoDelete,
		false,
		false,
		nil,
	)

	failOnError(err, "Failed to declare a queue")
	return &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
