package debug

import "github.com/apache/beam/sdks/go/pkg/beam"

// Tick adds a single element source with the value "tick".
func Tick(p *beam.Pipeline) beam.PCollection {
	return beam.Source(p, tickFn)
}

func tickFn(emit func(string)) {
	emit("tick")
}
