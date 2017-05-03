package debug

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"log"
)

// Print prints out all data. Use with care.
func Print(p *beam.Pipeline, col beam.PCollection) (beam.PCollection, error) {
	p = p.Composite("debug.Print")
	return beam.ParDo(p, printFn, col)
}

// Print prints out all data and then discards it. Use with care.
func Print0(p *beam.Pipeline, col beam.PCollection) error {
	_, err := Print(p, col)
	return err
}

func printFn(t typex.T) typex.T {
	log.Printf("Elm: %v", t)
	return t
}

// Discard is a sink that discards all data.
func Discard(p *beam.Pipeline, col beam.PCollection) error {
	p = p.Composite("debug.Discard")
	return beam.ParDo0(p, discardFn, col)
}

func discardFn(t typex.T) {
	// nop
}
