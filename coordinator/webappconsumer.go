package coordinator

import (
	"bytes"
	"encoding/gob"
	"godist/dto"
	"godist/qutils"
	"time"

	"github.com/streadway/amqp"
)

type WebappConsumer struct {
	er      EventRaiser
	conn    *amqp.Connection
	ch      *amqp.Channel
	sources []string
}

func NewWebappConsumer(er EventRaiser) *WebappConsumer {
	wc := WebappConsumer{
		er: er,
	}

	wc.ch, wc.conn = qutils.GetChannel(url)
	qutils.GetQueue(qutils.PersistentDataQueue, wc.ch, false)

	go wc.ListenForDiscoveryRequests()

	wc.er.AddListener("DataSourceDiscovered",
		func(data interface{}) {
			wc.SubscribeToDataEvent(data.(string))
		})

	wc.ch.ExchangeDeclare(
		qutils.WebappSourceExchange, //name string,
		"fanout",                    //kind string,
		false,                       //durable bool,
		false,                       //autoDelete bool,
		false,                       //internal bool,
		false,                       //noWait bool,
		nil,                         //args amqp.Table
	)

	return &wc
}

func (wc *WebappConsumer) ListenForDiscoveryRequests() {
	q := qutils.GetQueue(qutils.WebappDiscoveryQueue, wc.ch, false)
	msgs, _ := wc.ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	for range msgs {
		for _, src := range wc.sources {
			wc.SendMessageSources(src)
		}
	}
}

func (wc *WebappConsumer) SendMessageSources(src string) {
	wc.ch.Publish(
		qutils.WebappSourceExchange,        //exchange string,
		"",                                 //key string,
		false,                              //mandatory bool,
		false,                              //immediate bool,
		amqp.Publishing{Body: []byte(src)}, //msg amqp.Publishing
	)
}

func (wc *WebappConsumer) SubscribeToDataEvent(name string) {
	for _, v := range wc.sources {
		if v == name {
			return
		}
	}
	wc.sources = append(wc.sources, name)
	wc.SendMessageSources(name)

	wc.er.AddListener("MessageReceived_"+name, func() func(interface{}) {
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

				wc.ch.Publish(
					"",
					qutils.WebappReadingsExchange,
					false,
					false,
					msg,
				)
			}
		}
	}())
}
