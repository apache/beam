package debug

import "github.com/apache/beam/sdks/go/pkg/beam"

// Tick adds a single element source with the value "tick".
func Tick(p *beam.Pipeline) beam.PCollection {
	tick, err := beam.Source(p, tickFn)
	if err != nil {
		panic("Failed to create Tick source")
	}
	return tick
}

func tickFn(emit func(string)) {
	emit("tick")
}
