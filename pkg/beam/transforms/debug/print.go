package debug

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"log"
)

// Print prints out all data. Use with care.
func Print(p *beam.Pipeline, col beam.PCollection) beam.PCollection {
	p = p.Composite("debug.Print")
	return beam.ParDo(p, printFn, col)
}

func printFn(t typex.T) typex.T {
	log.Printf("Elm: %v", t)
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
