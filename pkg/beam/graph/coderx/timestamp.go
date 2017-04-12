package coderx

import (
	"github.com/apache/beam/sdks/go/pkg/beam/typex"
	"math"
	"time"
)

// EncodeTimestamp encodes a time.Time as an uint64. The encoding is
// millis-since-epoch, but shifted so that the byte representation of negative
// values are lexicographically ordered before the byte representation of
// positive values.
func EncodeTimestamp(t typex.Timestamp) []byte {
	return EncodeUint64((uint64)((time.Time)(t).Unix() - math.MinInt64))
}

// DecodeTimestamp decodes a time.Time.
func DecodeTimestamp(data []byte) (typex.Timestamp, int, error) {
	unix, size, err := DecodeUint64(data)
	if err != nil {
		return typex.Timestamp(time.Time{}), 0, err
	}
	return typex.Timestamp(time.Unix(0, ((int64)(unix)+math.MinInt64)<<10)), size, nil
}
