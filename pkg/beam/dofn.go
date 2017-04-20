package beam

// BeamTypes is the struct that holds four types of Beam
// data that can be accessed by user functions.
// O: options
// A: aggregators
// S: state cells (Beam State API)
// T: timers (Beam State API)

// Depending on the type of function, it can only have access to a particular
// subset of these types. The explanations in function types will use the
// one-character abbreviations to denote the valid types.

// ExampleDoFn illustrates the features of a DoFn.
type ExampleDoFn struct{}

// Setup performs 1-time initialization of the DoFn
// prior to processing any bundles.
func (e ExampleDoFn) Setup() {}

// StartBundle is called once prior to processing a batch of elements.
func (e ExampleDoFn) StartBundle( /* output emitters and options */ ) {}

// FinishBundle is called once after all calls to ProcessElement for a
// bundle have been made.
func (e ExampleDoFn) FinishBundle( /* output emitters and BeamTypes(O) */ ) {}

// Teardown performs 1-time cleanup of the state of the DoFn.
// No methods of the DoFn will be called after Teardown is called.
func (e ExampleDoFn) Teardown() {}

// ProcessElement is called to handle elements of the pipeline. With
// channel semantics, it is called once per bundle, pulling items
// out of the channel. The return type of this function is void(?) for
// a regular DoFn, and a ProcessContinuation for a SplittableDoFn.
func (e ExampleDoFn) ProcessElement(
/* context.Context (optional)
   BeamTypes(OAST) (optional)
	 main input
	 side inputs (optional)
	 main output (optional)
	 side outputs (optional)
	 restriction tracker (iff SplittableDoFn) */) {
}

// OnTimer is an example of a callback invoked when the timer for the DoFn fires.
// TODO(wcn): open challenge here. A DoFn can have many timers. How do we register
// the callbacks in a remote-execution environment?
// In a local environment, we could do something like this.
// beam.NewTimer("name_of_timer", e.OnTimer)
// The typechecker can verify that the type signature is correct, but we're passing
// a method on a particular instance around as a function. That almost certainly
// won't serialize. So how can we do this?
func (e ExampleDoFn) OnTimer(
/* context.Context (optional)
BeamTypes (OA)
beam.Timestamp (TODO(wcn): need time domains)
IntervalWindow
*/) {
}

// Restriction trackers are required to be serializable per Beam serialization rules.
// A lot of the details around SplittableDoFns are not well defined. Runner support doesn't
// exist yet. The incorporation into the signature above is the bread crumb to be aware
// of this future work.

// ProcessContinuation is used to capture the state of a DoFn to support
// partially completed progress.
type ProcessContinuation struct {
	// TODO(wcn): define once SplittableDoFn is more stable
}
