package debug

import (
	"log"
	"reflect"

	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*printFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*printKVFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*printGBKFn)(nil)))

}

// Print prints out all data. Use with care.
func Print(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	return Printf(p, "Elm: %v", col)
}

// Printf prints out all data with custom formatting. The given format string
// is used as log.Printf(format, elm) for each element. Use with care.
func Printf(p *beam.Pipeline, format string, col beam.PCollection) beam.PCollection {
	p = p.Composite("debug.Print")

	switch {
	case typex.IsWKV(col.Type()):
		return beam.ParDo(p, &printKVFn{Format: format}, col)
	case typex.IsWGBK(col.Type()):
		return beam.ParDo(p, &printGBKFn{Format: format}, col)
	default:
		return beam.ParDo(p, &printFn{Format: format}, col)
	}
}

type printFn struct {
	Format string `json:"format"`
}

func (f *printFn) ProcessElement(t typex.T) typex.T {
	log.Printf(f.Format, t)
	return t
}

type printKVFn struct {
	Format string `json:"format"`
}

func (f *printKVFn) ProcessElement(x typex.X, y typex.Y) (typex.X, typex.Y) {
	log.Printf(f.Format, fmt.Sprintf("(%v,%v)", x, y))
	return x, y
}

type printGBKFn struct {
	Format string `json:"format"`
}

func (f *printGBKFn) ProcessElement(x typex.X, iter func(*typex.Y) bool) typex.X {
	var ys []string
	var y typex.Y
	for iter(&y) {
		ys = append(ys, fmt.Sprintf("%v", y))
	}
	log.Printf(f.Format, fmt.Sprintf("(%v,%v)", x, ys))
	return x
}

// Discard is a sink that discards all data.
func Discard(p *beam.Pipeline, col beam.PCollection) {
	p = p.Composite("debug.Discard")
	beam.ParDo0(p, discardFn, col)
}

func discardFn(t typex.T) {
	// nop
}
