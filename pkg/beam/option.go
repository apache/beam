package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
	"reflect"
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

// TODO(herohde) 5/18/2017: maybe remove Data concept and require that user functions
// use a struct to hold both data and methods?

// Data binds a concrete value to data options for a transformation. The actual
// type must match the field type.
type Data struct {
	Data interface{}
}

func (d Data) private() {}

func parseOpts(opts []Option) ([]SideInput, []Data) {
	var side []SideInput
	var data []Data

	for _, opt := range opts {
		switch opt.(type) {
		case Data:
			data = append(data, opt.(Data))
		case SideInput:
			side = append(side, opt.(SideInput))
		default:
			panic(fmt.Sprintf("Unexpected opt: %v", opt))
		}
	}
	return side, data
}

// applyData validates and populates the UserFn Opts.
func applyData(fn *userfn.UserFn, data []Data) error {
	// log.Printf("Data: %v <- %v [size=%v]", fn, data, len(data))

	index, ok := fn.Options()
	if len(data) > 1 || (!ok && len(data) == 1) {
		return fmt.Errorf("too many data options %v for %v", data, fn)
	}
	if ok && len(data) == 0 {
		return fmt.Errorf("missing data to bind for %v", fn)
	}
	if !ok {
		return nil // ok: no data, no opt.
	}

	// Validate that data passed in matched the type expected at runtime. If ok,
	// add it to the DoFn.

	actual := data[0].Data

	f, _ := reflectx.FindTaggedField(fn.Param[index].T, typex.OptTag)
	if !reflect.TypeOf(actual).AssignableTo(f.Type) {
		return fmt.Errorf("mismatched type opt %v in %v for data %v", f.Type, fn.Name, reflect.TypeOf(actual))
	}
	fn.Opt = append(fn.Opt, actual)
	return nil
}
