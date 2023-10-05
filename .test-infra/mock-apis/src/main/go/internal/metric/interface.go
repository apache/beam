package metric

import (
	"context"
	"time"
)

type Writer interface {
	Write(ctx context.Context, name string, unit string, points ...*Point) error
}

type Point struct {
	Timestamp time.Time
	Value     int64
}
