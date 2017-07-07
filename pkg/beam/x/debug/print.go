package debug

import (
	"log"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*printFn)(nil)))

}

// Print prints out all data. Use with care.
func Print(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	return Printf(p, "Elm: %v", col)
}

// Printf prints out all data with custom formatting. The given format string
// is used as log.Printf(format, elm) for each element. Use with care.
func Printf(p *beam.Pipeline, format string, col beam.PCollection) beam.PCollection {
	p = p.Composite("debug.Print")
	return beam.ParDo(p, &printFn{Format: format}, col)
}

type printFn struct {
	Format string `json:"format"`
}

func (f *printFn) ProcessElement(t typex.T) typex.T {
	log.Printf(f.Format, t)
	return t
}

// Discard is a sink that discards all data.
func Discard(p *beam.Pipeline, col beam.PCollection) {
	p = p.Composite("debug.Discard")
	beam.ParDo0(p, discardFn, col)
}

func discardFn(t typex.T) {
	// nop
}
