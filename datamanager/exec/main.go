package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"godist/datamanager"
	"godist/dto"
	"godist/qutils"
	"log"
)

const url = "amqp://guest@localhost:5672"

func main() {
	ch, conn := qutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	msgs, err := ch.Consume(
		qutils.PersistentDataQueue,
		"",
		false,
		true,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalln("Failed to get a message")
	}
	for msg := range msgs {
		buf := bytes.NewReader(msg.Body)
		dec := gob.NewDecoder(buf)
		sd := &dto.SensorMessage{}
		dec.Decode(sd)

		fmt.Printf("Read: %s", sd)
		err := datamanager.SaveReading(sd)
		if err != nil {
			log.Printf("Failed to save reading from sensor %v. Error: %s", sd.Name, err.Error())
		} else {
			msg.Ack(false)
		}
	}
}
