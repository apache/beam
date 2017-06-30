package beam

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
)

// Impulse emits a single empty []byte into the global window. The resulting
// PCollection is a singleton of type []byte. For example:
//
//   foo := beam.Impulse(p)  // foo : W<[]byte>
//
// The purpose of Impulse is to trigger another transform, such as
// ones that take all information as side inputs.
func Impulse(p *Pipeline) PCollection {
	return ImpulseValue(p, []byte{})
}

// ImpulseValue emits the supplied byte slice into the global window. The resulting
// PCollection is a singleton of type []byte. For example:
//
//   foo := beam.ImpulseValue(p, []byte{})  // foo : W<[]byte>
//
func ImpulseValue(p *Pipeline, value []byte) PCollection {
	edge := graph.NewImpulse(p.real, p.parent, value)
	return PCollection{edge.Output[0].To}
}
