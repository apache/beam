package beam

import (
	"fmt"
	"reflect"
)

// Option is an optional value or context to a transformation, used at pipeline
// construction time. The primary use case is providing side inputs. For
// example:
//
//     words := textio.Read(p, "...")
//     sample := textio.Read(p, "...")
//     smallWords := beam.ParDo(p, analyzeFn, words, beam.SideInput{Input: sample})
type Option interface {
	private()
}

// SideInput provides a view of the given PCollection to the transformation.
type SideInput struct {
	Input PCollection

	// WindowFn interface{}
	// ViewFn   interface{}
}

func (s SideInput) private() {}

// TypeDefinition provides construction-time type information that the platform
// cannot infer, such as structured storage sources. These types are universal types
// that appear as output only. Types that are inferrable should not be conveyed via
// this mechanism.
type TypeDefinition struct {
	// Var is the universal type defined.
	Var reflect.Type
	// T is the type it is bound to.
	T reflect.Type
}

func (s TypeDefinition) private() {}

func parseOpts(opts []Option) ([]SideInput, []TypeDefinition) {
	var side []SideInput
	var infer []TypeDefinition

	for _, opt := range opts {
		switch opt.(type) {
		case SideInput:
			side = append(side, opt.(SideInput))
		case TypeDefinition:
			infer = append(infer, opt.(TypeDefinition))
		default:
			panic(fmt.Sprintf("Unexpected opt: %v", opt))
		}
	}
	return side, infer
}
