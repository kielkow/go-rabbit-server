package dto

import (
	"time"
	"encoding/gob"
)

// SensorMessage struct
type SensorMessage struct {
	Name string
	Value float64
	Timestamp time.Time
}

func init() {
	gob.Register(SensorMessage{})
}