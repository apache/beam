package xlang

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

func (e ExternalTransform) withNamedInputs(inputs map[string]*graph.Node) ExternalTransform {
	if e.inputs != nil {
		panic(errors.Errorf("inputs already set as: \n%v", e.inputs))
	}
	e.inputs = inputs
	return e
}

func (e ExternalTransform) withNamedOutputs(outputs map[string]typex.FullType) ExternalTransform {
	if e.outputs != nil {
		panic(errors.Errorf("outputs already set as: \n%v", e.outputs))
	}
	e.outputs = outputs
	return e
}

func (e ExternalTransform) withSource(input *graph.Node) ExternalTransform {
	if e.inputs != nil {
		panic(errors.Errorf("inputs already set as: \n%v", e.inputs))
	}
	e.inputs["sourceInput"] = input
	return e
}

func (e ExternalTransform) withSink(output typex.FullType) ExternalTransform {
	if e.outputs != nil {
		panic(errors.Errorf("outputs already set as: \n%v", e.outputs))
	}
	e.outputs["sinkOutput"] = output
	return e
}
