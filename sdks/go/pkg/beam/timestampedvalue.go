package beam

import (
	"io"
	"time"
)

type TimestampedValue struct {
	Value     interface{}
	Timestamp time.Time
}

func TimestampedOf(value interface{}, timestamp time.Time) TimestampedValue {
	return TimestampedValue{Value: value, Timestamp: timestamp}
}

type TimestampedValueCoder struct {
}

func (tsvc TimestampedValueCoder) Encode(t TimestampedValue, w io.Writer) error {
	return nil
}

func (tsvc TimestampedValueCoder) Decode(r io.Reader) (TimestampedValue, error) {
	return TimestampedValue{}, nil
}
