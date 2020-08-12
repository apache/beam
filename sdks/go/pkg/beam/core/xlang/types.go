package xlang

import (
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

type ExpandedTransform struct {
	components      interface{} // *pipepb.Components
	transform       interface{} //*pipepb.PTransform
	requirements    []string
	outputIsBounded map[string]bool
}

// ExternalTransform represents the cross-language transform in and out of the Pipeline as a MultiEdge and Expanded proto respectively
type ExternalTransform struct {
	Urn           string
	Payload       []byte
	ExpansionAddr string

	inputs  map[string]*graph.Node
	outputs map[string]typex.FullType

	expanded *ExpandedTransform
}
