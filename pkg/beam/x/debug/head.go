package debug

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*headFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*headKVFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*headGBKFn)(nil)))

}

// Head returns the first "n" elements it sees, it doesn't enforce any logic
// as to what elements they will be.
func Head(p *beam.Pipeline, col beam.PCollection, n int) beam.PCollection {
	p = p.Composite("debug.Head")

	switch {
	case typex.IsWKV(col.Type()):
		return beam.ParDo(p, &headKVFn{N: n}, col)
	case typex.IsWGBK(col.Type()):
		return beam.ParDo(p, &headGBKFn{N: n}, col)
	default:
		return beam.ParDo(p, &headFn{N: n}, col)
	}
}

type headFn struct {
	N       int `json:"n"`
	Current int `json:"current"`
}

func (h *headFn) ProcessElement(t beam.T, emit func(beam.T)) {
	if h.Current < h.N {
		h.Current++
		emit(t)
	}
}

type headKVFn struct {
	N       int `json:"n"`
	Current int `json:"current"`
}

func (h *headKVFn) ProcessElement(x beam.X, y beam.Y, emit func(beam.X, beam.Y)) {
	if h.Current < h.N {
		h.Current++
		emit(x, y)
	}
}

type headGBKFn struct {
	N       int `json:"n"`
	Current int `json:"current"`
}

func (h *headGBKFn) ProcessElement(x beam.X, iter func(*beam.Y) bool, emit func(beam.X, func(*beam.Y) bool)) {
	if h.Current < h.N {
		h.Current++
		emit(x, iter)
	}
}
