package sensor

import (
	"log"
	"flag"
	"time"
	"bytes"
	"strconv"
	"math/rand"
	"encoding/gob"
	"distributed/dto"
	"distributed/queueutils"
	"github.com/streadway/amqp"
)

var url = os.Getenv("AMQP_URL")

var name = flag.String("name", "sensor", "name of the sensor")
var freq = flag.Uint("freq", 5, "update frequency in cycles/sec")
var max = flag.Float64("max", 5., "maximum value for generated readings")
var min = flag.Float64("min", 1., "minimum value for generated readings")
var stepSize = flag.Float64("step", 0.1, "maximum allowable change per measurement")

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

var value = r.Float64()*(*max-*min) + *min
var nom = (*max-*min)/2 + *min

func sensor() {
	flag.Parse()

	conn, ch := queueutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	dataQueue := queueutils.GetQueue(*name, ch)

	msg := amqp.Publishing{Body: []byte(*name)}

	ch.Publish(
		"amq.fanout",
		"",
		false,
		false,
		msg)

	dur, _ := time.ParseDuration(strconv.Itoa(1000/int(*freq)) + "ms")

	signal := time.Tick(dur)

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	for range signal {
		calcValue()
		reading := dto.SensorMessage{
			Name: *name,
			Value: value,
			Timestamp: time.Now()
		}

		buf.Reset()
		enc := gob.NewEncoder(buf)
		enc.Encode(reading)

		msg := amqp.Publishing{
			Body: buf.Bytes()
		}

		ch.Publish(
			"",
			dataQueue.Name,
			false,
			false,
			msg)

		log.Printf("Reading sent. Value: %v\n", value)
	}
}

func calcValue() {
	var maxStep, minStep float64

	if value < nom {
		maxStep = *stepSize
		minStep = -1 * *stepSize * (value - *min) / (nom - *min)
	} else {
		maxStep = *stepSize * (*max - value) / (*max - nom)
		minStep = -1 * *stepSize
	}

	value += r.Float64()*(maxStep-minStep) + minStep
}
