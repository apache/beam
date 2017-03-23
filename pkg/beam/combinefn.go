package beam

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
type Key EncodedData

// Combine functions specify how to combine a collection of input values (InputType) into a single output
// value (OutputType). This is done via one or more intermediate mutable accumulators (AccumulatorType).
// A combiner can optionally use a key and/or contextual information (options and side inputs).
// This combination effectively produces 4 flavors of combine fn
// CombineFn (neither)
// KeyedCombineFn (only key)
// ContextualCombineFn (only options/side inputs)
// KeyedContextualCombineFn (both)

// Pipeline construction time will verify that the function signatures supplied are consistent
// with one of the flavors above.

// In all functions, context.Context is optional, and would be the first argument.
// A combine function must implement all methods below.

// CreateAccumulator produces a fresh accumulator.
func (e ExampleCombineFn) CreateAccumulator( /* Key */ /* BeamTypes: O, side inputs */ ) AccumulatorType {
	return 0
}

// AddInput updates the accumulator to include the supplied input
func (e ExampleCombineFn) AddInput( /* Key */ InputType, AccumulatorType /* BeamTypes: O, side inputs */) {
}

// MergeAccumulators merges the accumulators, returning a single value holding the merged value.
// TODO(wcn): Java signature is an iterable. Are we really merging so many accumulators? That seems
// unlikely and unwieldy.
func (e ExampleCombineFn) MergeAccumulators( /* Key */ []AccumulatorType) AccumulatorType { return 0 }

// Default returns the zero value of OutputType
func (e ExampleCombineFn) Default() OutputType { return 0 }

// ExtractOutput produces an output from the accumulator.
func (e ExampleCombineFn) ExtractOutput( /* Key */ AccumulatorType /* BeamTypes: O, side inputs */) OutputType {
	return 0
}

// Compact  ...
func (e ExampleCombineFn) Compact( /* Key */ AccumulatorType) AccumulatorType { return 0 }

// Apply produces an output from a sequence of inputs.
func (e ExampleCombineFn) Apply( /* Key */ chan<- InputType) OutputType { return 0 }
