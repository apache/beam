package coder

import (
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"io"
	"math"
	"time"
)

// EncodeTimestamp encodes a time.Time as an uint64. The encoding is
// millis-since-epoch, but shifted so that the byte representation of negative
// values are lexicographically ordered before the byte representation of
// positive values.
func EncodeEventTime(t typex.EventTime, w io.Writer) error {
	return EncodeUint64((uint64)((time.Time)(t).Unix()-math.MinInt64), w)
}

// DecodeEventTime decodes a time.Time.
func DecodeEventTime(r io.Reader) (typex.EventTime, error) {
	unix, err := DecodeUint64(r)
	if err != nil {
		return typex.EventTime(time.Time{}), err
	}
	return typex.EventTime(time.Unix(0, ((int64)(unix)+math.MinInt64)<<10)), nil
}
