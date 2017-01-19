package model

// See: https://github.com/kennknowles/beam/blob/c5843ce10e782056c76157169eb5516bf18ed9e4/model/pipeline/src/main/resources/org/apache/beam/model/pipeline/pipeline.json

// TODO(BEAM-115): the model should be generated from a language-agnostic
// protobuf. For now, we hand-converted the tentative one (incl. comments).

// A Pipeline contains the entire hierarchical graph including
// nodes for all values.
//
// Nodes (transform or value or other) are identified by some unique id. This need
// not be readable. It is simplest to think of it as a reified pointer or UUID.
// Cyclic pointer structures are forbidden throughout.
type Pipeline struct {
	// A mapping containing all user-definable functions in the pipeline.
	// The keys must be unique to this pipeline definition, but otherwise are
	// arbitrary.
	UserFns []UserFn `json:"userFns,omitempty"`

	// A mapping containing all transforms in the pipeline.
	Transforms []Transform `json:"transforms,omitempty"`

	// A mapping containing all the primitive values in the pipeline.
	Values []Value `json:"values,omitempty"`

	Coders []Coder `json:"coders,omitempty"`
}

// A user-defined function.
type UserFn struct {
	// A cross-language, stable, unique identifier for the function. It should indicate
	// enough information that naive optimization (or language-to-language replacement)
	// may be possible, since the runner will have no insight into the SDK-specific data.
	URN string `json:"urn"`

	// A URL for the container image containing the SDK that understands how to run
	// this user-definable function.
	SDKContainer string `json:"sdkContainer,omitempty"`

	// A free-form object containing SDK-specific data.
	Data interface{} `json:"data"`
}

type Transform struct {
	// The stable name of the transform application node.
	Label string `json:"label"`

	// Whether or not it is composite, it contains a "kind" string that uniquely
	// identifies what it computes. This is a stable, cross-language string, that
	// can be used for replacement-based implementation and also to parse the node
	// to a concrete language-specific class.
	//
	// A stable, cross-language identifier identifying what the transform computes.
	// This is to be used for replacement-based implementation, whether it is a primitive
	// or composite. Examples include "GroupByKey", "Sum", "ParDo". We should likely
	// adopt a standard namespacing convention, such as URIs.
	URN string `json:"urn"`

	// All of the inputs and outputs for the transform, whether it is composite or primitive.
	// The inputs and outputs of parent and child transforms nodes must be coherent:
	//
	// - The unexpanded inputs of every child transforms must be contained
	//   within some level of the recursive expansion of the inputs to the parent.
	// - Every level of the expanded output of a composite must be contained within
	//   some level of the expansion of some child.
	//
	// These collections of values are unordered. References to values are managed in
	// a transform-specific way. For example, a DoFn will call processContext.output(tag, value)
	// and the tag should contain sufficient information to identify the target PCollection.
	Inputs  []string `json:"inputs"`
	Outputs []string `json:"outputs"`

	// If the node is contained within a composite, a pointer to the parent.
	Parent string `json:"parent,omitempty"`

	// If the node is a runner-specific replacement for a user's runner-agnostic node,
	// it contains a pointer back to it so the user's graph can be reconstructed.
	// "replacementFor": { "type": [ "null", "string" ] },

	// If the node is a defunct user node that has been replaced, it contains a bit
	// to distinguish it from an active node in the graph.
	// "replaced": { "type": "boolean" },

	// Static display data
	// "display_data": { "type": "object", "additionalProperties": { "type": "string" } },

	// The payload must correspond to the kind string and must be null for transforms that
	// are not predefined as part of the Beam model.
	// "payload": {
	// "type": [ "null", "object" ],
	// "oneOf": [
	// { "$ref": "#/definitions/ParDoPayload" },
	// { "$ref": "#/definitions/ReadPayload" },
	// { "$ref": "#/definitions/WindowIntoPayload" },
	// { "$ref": "#/definitions/CombinePayload" }
	// ]
	// }
	ParDoPayload *ParDoPayload `json:"payload,omitempty"`
}

// The parameters to a ParDo transform and further metatdata
type ParDoPayload struct {
	// The DoFn for this ParDo
	DoFn string `json:"doFn"`

	// Whether the ParDo requires access to the current window (this may force runners to perform
	// additional work prior to executing this ParDo)
	// "requiresWindowAccess": { "type": "boolean" },

	// A map from PCollection id to the specification for how to materialize and read it
	// as a side input
	// "sideInputs": {
	// "type": "object",
	// "additionalProperties": { "$ref" : "#/definitions/SideInput" }
	// }
}

type Value struct {
	// A unique program-readable name for the value, such as "foo/baz/bar.out
	Label string `json:"label"`

	// The node ID for the coder of this PCollection
	Coder string `json:"label"`

	// Windowing strategy node id
	// "windowingStrategy": { "type": "string" },

	// This is the static display data recently proposed
	// "displayData": { "$ref": "#/definitions/DisplayData" }
}

type Coder struct {
	// A cross-language, stable, unique identifier for the (possibly parametric) encoding.
	URN string `json:"urn"`

	// If this coder is parametric, such as ListCoder(VarIntCoder), this is a list
	// of the components. In order for encodings to be identical, the encoding_id
	// and all components must be identical.
	// "componentEncodings": { "type": "array", "items": { "type": "string" } },

	// The SDK-specific user-definable coder implementing the described encoding.
	//
	// If present, then the SDK can utilize this coder without a priori knowledge of the
	// encoded format.
	//
	// TBD: multi-SDK support
	// "customCoder": { "type": [ "null", "string" ] }
}
