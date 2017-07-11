package ptest

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
)

// TODO(herohde) 7/10/2017: add hooks to verify counters, logs, etc.

// Create creates a pipeline and a PCollection with the given values.
func Create(values []interface{}) (*beam.Pipeline, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.Create(p, values...)
}

// CreateList creates a pipeline and a PCollection with the given values.
func CreateList(values interface{}) (*beam.Pipeline, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.CreateList(p, values)
}

// Create2 creates a pipeline and 2 PCollections with the given values.
func Create2(a, b []interface{}) (*beam.Pipeline, beam.PCollection, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.Create(p, a...), beam.Create(p, b...)
}

// CreateList2 creates a pipeline and 2 PCollections with the given values.
func CreateList2(a, b interface{}) (*beam.Pipeline, beam.PCollection, beam.PCollection) {
	p := beam.NewPipeline()
	return p, beam.CreateList(p, a), beam.CreateList(p, b)
}

// Run runs a pipeline for testing. The semantics of the pipeline is expected
// to be verified through passert.
func Run(p *beam.Pipeline) error {
	return local.Execute(context.Background(), p)
}
