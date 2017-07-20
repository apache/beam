package debug

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"reflect"
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
	N int `json:"n"`
	Current int `json:"current"`
}

func (h *headFn) ProcessElement(t typex.T, emit func(typex.T)) {
	if h.Current < h.N {
		h.Current++
		emit(t)
	}
}

type headKVFn struct {
	N int `json:"n"`
	Current int `json:"current"`
}

func (h *headKVFn) ProcessElement(x typex.X, y typex.Y, emit func(typex.X, typex.Y)) {
	if h.Current < h.N {
		h.Current++
		emit(x, y)
	}
}

type headGBKFn struct {
	N int `json:"n"`
	Current int `json:"current"`
}

func (h *headGBKFn) ProcessElement(x typex.X, iter func(*typex.Y) bool, emit func(typex.X, func(*typex.Y) bool)) {
	if h.Current < h.N {
		h.Current++
		emit(x, iter)
	}
}