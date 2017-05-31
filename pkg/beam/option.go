package beam

import (
	"fmt"
)

// Option is an optional value or context to a transformation, used at pipeline
// construction time.
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

func parseOpts(opts []Option) []SideInput {
	var side []SideInput

	for _, opt := range opts {
		switch opt.(type) {
		case SideInput:
			side = append(side, opt.(SideInput))
		default:
			panic(fmt.Sprintf("Unexpected opt: %v", opt))
		}
	}
	return side
}
