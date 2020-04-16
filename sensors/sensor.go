package main

import (
	"bytes"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"encoding/gob"
	"godist/dto"
	"godist/qutils"

	"github.com/streadway/amqp"
)

var url = "amqp://guest@localhost:5672"

var name = flag.String("name", "sensor", "name of the sensor")
var freq = flag.Uint("freq", 5, "update frequency in cycles/sec")
var max = flag.Float64("max", 5., "maximum value for generated readings")
var min = flag.Float64("min", 1., "minimum value for generated readings")
var stepSize = flag.Float64("step", 0.1, "maximum allowable change per measurement")

var r = rand.New(rand.NewSource(time.Now().UnixNano()))
var value = r.Float64()*(*max-*min) + *min
var nom = (*max-*min)/2 + *min

func main() {
	flag.Parse()

	ch, conn := qutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	dataQueue := qutils.GetQueue(*name, ch, false)

	PublishQueueName(ch)

	discoveryQueue := qutils.GetQueue("", ch, true)
	ch.QueueBind(
		discoveryQueue.Name,            //name string,
		"",                             //key string,
		qutils.SensorDiscoveryExchange, //exchange string,
		false,                          //noWait bool,
		nil,                            //args amqp.Table
	)

	go ListenForDiscoveryRequests(discoveryQueue.Name, ch)

	dur, _ := time.ParseDuration(strconv.Itoa(1000/int(*freq)) + "ms")
	signal := time.Tick(dur)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	for range signal {
		calcValue()
		reading := dto.SensorMessage{
			Name:      *name,
			Value:     value,
			Timestamp: time.Now(),
		}
		buf.Reset()
		buf = new(bytes.Buffer)
		enc = gob.NewEncoder(buf)
		enc.Encode(reading)

		msg := amqp.Publishing{
			Body: buf.Bytes(),
		}

		ch.Publish(
			"",
			dataQueue.Name,
			false,
			false,
			msg,
		)

		log.Printf("Reading sent. Value: %v\n", value)
	}
}

func calcValue() {
	var maxStep, minStep float64
	if value < nom {
		maxStep = *stepSize
		minStep = (nom - *min) / (value - *min) * (*stepSize) * (-1)
	} else {
		maxStep = (*max - value) / (*max - nom) * (*stepSize)
		minStep = (-1) * (*stepSize)
	}

	value += r.Float64()*(maxStep-minStep) + minStep
}

func PublishQueueName(ch *amqp.Channel) {
	msg := amqp.Publishing{Body: []byte(*name)}
	ch.Publish(
		"amq.fanout",
		"",
		false,
		false,
		msg,
	)
}

func ListenForDiscoveryRequests(name string, ch *amqp.Channel) {
	msgs, _ := ch.Consume(
		name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	for range msgs {
		PublishQueueName(ch)
	}
}
