// Package beam is a design experiment for transformations, build-time
// wiring and graph construction. The previous 3 experiments focused
// on how users could specify DoFns.
package beam

import "reflect"

// VARIANT A: PCollection is a simple public concrete type. A pre-translation graph
// node is a *PCollection:
//
//  * Pipeline: low-level graph builder. Used directly by primitive PTransforms.
//  * PTransform: PCollection-typing (compile time), Element data typing inferred from DoFns, Concrete arity.
//  * DoFn, CombineFn, etc: concrete typing, concrete context.

// Pipeline construction runtime: typing and context inferred from DoFns for PCollections.

type Coder reflect.Type // TBD

type Pipeline struct {
	scopes []string // TBD: Composite PTransform stack for naming
	nodes  []*PCollection
}

// TODO: collect options, coder registry. In the pipeline or externally?

// TODO: make the pipeline API distinguish between sources and sinks? The pipeline
// needs some data to be able to generate the runner graph.

// BAD: composite transforms need to manually push/pop the scope to preserve that
// structure.

func (p *Pipeline) NewNode(owner string) *PCollection {
	ret := &PCollection{id: len(p.nodes)}
	p.nodes = append(p.nodes, ret)
	return ret
}

func (p *Pipeline) Connect(a, b *PCollection /* + transform metadata */) error {
	return nil
}

func (p *Pipeline) Build() /* Fn Runner graph */ error {
	return nil
}

// TODO: we probably want to retain the output element type from a transform. If the next
// input want something else, we need to be aware of any needed conversions at runtime.

// PCollection represents "PCollection<T>".
type PCollection struct {
	Coder Coder        // Coder
	T     reflect.Type // Element type

	id int // internal ID used by owning pipeline.
}

// GOOD: concrete arity for PTransforms avoid the java-layer of aux types: PColView, PColList,
// PColTuple. We use []*PCollection

// GOOD: PTransforms have a natural way to indicate error, if applicable.

// GOOD: PTransforms can be polymorphic by propagating the type+coder ...
// BAD:  ... however, any such transforms using user code would be untyped (taking
// interface{} values or based on runtime reflection, unlike Java generic versions).
// GOOD:: .. but since many transforms work at the coder level (and code equality is
// the real equality), we can implement "generic" transforms that work with []byte, say,
// and thus avoid reflection.

// NEUTRAL: There is no natural first-class PTransform type. Omit it?

// NEUTRAL: Direction of PCollection is not in the type, but the model is simpler.

func ParDo(p *Pipeline, fn interface{}, col *PCollection) (*PCollection, error) {
	ret := p.NewNode("pardo")
	// ret.T + Coder is found by reflection and analysis of the fn.

	if err := p.Connect(col, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// BAD: Need overload if the output has different arity

// GOOD: We don't need any artificial "main" output, arising from the fluent style in Java.

func ParDo2(p *Pipeline, fn interface{}, col *PCollection) ([]*PCollection, error) {
	return nil, nil
}

// GOOD: we have more options for inputs. Notably the below.

// BAD: types cannot express the KV requirements on the element types.

func Join(p *Pipeline, a, b *PCollection) (*PCollection, error) {
	return nil, nil
}

func Flatten(p *Pipeline, list ...*PCollection) (*PCollection, error) {
	return nil, nil
}

func GBK(p *Pipeline, a *PCollection) (*PCollection, error) {
	return nil, nil
}

func CoGBK(p *Pipeline, list ...*PCollection) (*PCollection, error) {
	return nil, nil
}

// VARIANT B: Use interface for PCollection to avoid * and hide internal fields better.
// We might also be able to introduce PTable, say, to capture KV requirements.

// GOOD: allows distinct kinds of nodes more naturally.

// BAD?: No simple field for properties.

type PCollection2 interface {
	T() reflect.Type
}

type PTable2 interface {
	PCollection2

	Key() reflect.Type
	Value() reflect.Type
}
