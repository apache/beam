package beam

import "github.com/apache/beam/sdks/go/pkg/beam/typex"

// Sample data types

// ExampleCombineFn is the sample combine fn.
type ExampleCombineFn struct{}

// AccumulatorType is the sample accumulator type.
type AccumulatorType int64

// OutputType is the sample output type.
type OutputType int64

// InputType is the sample input type.
type InputType int64

// Key is of type beam.EncodedData
type Key typex.EncodedData

// Underlying data type of the combiner needs to be serializable per Beam serialization rules.

// Combine functions specify how to combine a collection of input values (InputType) into a single output
// value (OutputType). This is done via one or more intermediate mutable accumulators (AccumulatorType).
// A combiner can optionally use a key and/or contextual information (options and side inputs).
// This combination effectively produces 4 flavors of combine fn
// CombineFn (neither)
// KeyedCombineFn (only key)
// ContextualCombineFn (only options/side inputs)
// KeyedContextualCombineFn (both)

// Pipeline construction time will verify that the function signatures supplied are consistent
// with one of the flavors above. If the signatures are not consistent, the pipeline will fail
// in the construction phase.

// In all functions, context.Context is optional, and would be the first argument.
// A combiner must provide the methods required based on their descriptions. If a method is missing,
// the pipeline will fail in the construction phase.

// CreateAccumulator produces a fresh accumulator. mandatory iff args
func (e ExampleCombineFn) CreateAccumulator( /* Key */ /* BeamTypes: O, side inputs */ ) AccumulatorType {
	return 0
}

// AddInput updates the accumulator to include the supplied input. mandatory
func (e ExampleCombineFn) AddInput( /* Key */ InputType, AccumulatorType /* BeamTypes: O, side inputs */) {
}

// MergeAccumulators merges the accumulators, returning a single value holding the merged value. mandatory
func (e ExampleCombineFn) MergeAccumulators( /* Key */ []AccumulatorType) AccumulatorType { return 0 }

// ExtractOutput produces an output from the accumulator. mandatory iff AccumulatorType != OutputType
func (e ExampleCombineFn) ExtractOutput( /* Key */ AccumulatorType /* BeamTypes: O, side inputs */) OutputType {
	return 0
}

// Compact returns an accumulator that represents the same logical value as the input accumulator
// but may have a more compact representation. For most combine functions, this method is not
// needed, but should be implemented by combiners that (for example) buffer up elements and
// combine them in batches. Compact is only called for combiners that are keyed. Implementing
// it for a non-keyed combine function will result in pipeline construction failure.
func (e ExampleCombineFn) Compact( /* Key */ AccumulatorType) AccumulatorType { return 0 }
