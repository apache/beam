package graph

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

type ExpandedTransform struct {
	Components_     interface{} // *pipepb.Components
	Transform_      interface{} //*pipepb.PTransform
	Requirements_   []string
	BoundedOutputs_ map[string]bool
}

func (exp *ExpandedTransform) Components() *pipepb.Components {
	if c, ok := exp.Components_.(*pipepb.Components); ok {
		return c
	}
	panic(errors.Errorf("malformed components; %v lacks a conforming pipeline component", exp))
}

func (exp *ExpandedTransform) Transform() *pipepb.PTransform {
	if t, ok := exp.Transform_.(*pipepb.PTransform); ok {
		return t
	}
	panic(errors.Errorf("malformed transform; %v lacks a conforming pipeline ptransform", exp))
}

func (exp *ExpandedTransform) Requirements() []string {
	if exp.Requirements_ != nil {
		return exp.Requirements_
	}
	return nil
}

func (exp *ExpandedTransform) BoundedOutputs() map[string]bool {
	if exp.BoundedOutputs_ != nil {
		return exp.BoundedOutputs_
	}
	return nil
}

// ExternalTransform represents the cross-language transform in and out of the Pipeline as a MultiEdge and Expanded proto respectively
type ExternalTransform struct {
	Urn           string
	Payload       []byte
	ExpansionAddr string

	//replace all input/output fields with Inbound and Outbound id maps referencing the orginal Multiedge

	inputs      map[string]*Node
	Outputs     map[string]*Node
	outputTypes map[string]typex.FullType

	Expanded_ *ExpandedTransform
}

func (ext ExternalTransform) WithNamedInputs(inputs map[string]*Node) ExternalTransform {
	if ext.inputs != nil {
		panic(errors.Errorf("inputs already set as: \n%v", ext.inputs))
	}
	ext.inputs = inputs
	return ext
}

func (ext ExternalTransform) WithNamedOutputs(outputTypes map[string]typex.FullType) ExternalTransform {
	if ext.outputTypes != nil {
		panic(errors.Errorf("outputTypes already set as: \n%v", ext.outputTypes))
	}
	ext.outputTypes = outputTypes
	return ext
}

func (ext ExternalTransform) WithSource(input *Node) ExternalTransform {
	if ext.inputs != nil {
		panic(errors.Errorf("inputs already set as: \n%v", ext.inputs))
	}
	ext.inputs = map[string]*Node{"sourceInput": input} // change to random package constants
	return ext
}

func (ext ExternalTransform) WithSink(outputType typex.FullType) ExternalTransform {
	if ext.outputTypes != nil {
		panic(errors.Errorf("outputTypes already set as: \n%v", ext.outputTypes))
	}
	ext.outputTypes = map[string]typex.FullType{"sinkOutput": outputType} // change to random package constants
	return ext
}

func (ext *ExternalTransform) Inputs() map[string]*Node {
	if ext.inputs != nil {
		return ext.inputs
	}
	return nil
}

func (ext *ExternalTransform) OutputTypes() map[string]typex.FullType {
	if ext.outputTypes != nil {
		return ext.outputTypes
	}
	return nil
}

func (ext *ExternalTransform) Expanded() *ExpandedTransform {
	if ext.IsExpanded() {
		return ext.Expanded_
	}
	return nil
}

func (ext *ExternalTransform) IsExpanded() bool {
	return ext.Expanded_ != nil
}
