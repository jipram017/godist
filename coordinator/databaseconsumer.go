package coordinator

import (
	"bytes"
	"encoding/gob"
	"godist/dto"
	"godist/qutils"
	"time"

	"github.com/streadway/amqp"
)

const maxRate = 5 * time.Second

type DatabaseConsumer struct {
	er      EventRaiser
	conn    *amqp.Connection
	ch      *amqp.Channel
	queue   *amqp.Queue
	sources []string
}

func NewDatabaseConsumer(er EventRaiser) *DatabaseConsumer {
	dc := DatabaseConsumer{
		er: er,
	}

	dc.ch, dc.conn = qutils.GetChannel(url)
	dc.queue = qutils.GetQueue(
		qutils.PersistentDataQueue,
		dc.ch,
		false,
	)

	dc.er.AddListener("DataSourceDiscovered", func(data interface{}) {
		dc.SubscribeToDataEvent(data.(string))
	})

	return &dc
}

func (dc *DatabaseConsumer) SubscribeToDataEvent(name string) {
	for _, v := range dc.sources {
		if v == name {
			return
		}
	}

	dc.er.AddListener("MessageReceived_"+name, func() func(interface{}) {
		prevTime := time.Unix(0, 0)
		buf := new(bytes.Buffer)
		return func(data interface{}) {
			ed := data.(EventData)
			if time.Since(prevTime) > maxRate {
				prevTime = time.Now()
				sm := dto.SensorMessage{
					Name:      ed.Name,
					Value:     ed.Value,
					Timestamp: ed.Timestamp,
				}

				buf.Reset()
				buf = new(bytes.Buffer)
				enc := gob.NewEncoder(buf)
				enc.Encode(sm)

				msg := amqp.Publishing{
					Body: buf.Bytes(),
				}

				dc.ch.Publish(
					"",
					qutils.PersistentDataQueue,
					false,
					false,
					msg,
				)
			}
		}
	}())
}
