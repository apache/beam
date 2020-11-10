// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beam

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

func addParDoCtx(err error, s Scope) error {
	return errors.WithContextf(err, "inserting ParDo in scope %s", s)
}

// TryParDo attempts to insert a ParDo transform into the pipeline. It may fail
// for multiple reasons, notably that the dofn is not valid or cannot be bound
// -- due to type mismatch, say -- to the incoming PCollections.
func TryParDo(s Scope, dofn interface{}, col PCollection, opts ...Option) ([]PCollection, error) {
	side, typedefs, err := validate(s, col, opts)
	if err != nil {
		return nil, addParDoCtx(err, s)
	}

	doFnOpt := graph.NumMainInputs(graph.MainSingle)
	// Check the PCollection for any keyed type (not just KV specifically).
	if typex.IsKV(col.Type()) {
		doFnOpt = graph.NumMainInputs(graph.MainKv)
	} else if typex.IsCoGBK(col.Type()) {
		doFnOpt = graph.CoGBKMainInput(len(col.Type().Components()))
	}
	fn, err := graph.NewDoFn(dofn, doFnOpt)
	if err != nil {
		return nil, addParDoCtx(err, s)
	}

	in := []*graph.Node{col.n}
	for _, s := range side {
		in = append(in, s.Input.n)
	}

	var rc *coder.Coder
	if fn.IsSplittable() {
		sdf := (*graph.SplittableDoFn)(fn)
		rc, err = inferCoder(typex.New(sdf.RestrictionT()))
		if err != nil {
			return nil, addParDoCtx(err, s)
		}
	}

	edge, err := graph.NewParDo(s.real, s.scope, fn, in, rc, typedefs)
	if err != nil {
		return nil, addParDoCtx(err, s)
	}

	var ret []PCollection
	for _, out := range edge.Output {
		c := PCollection{out.To}
		c.SetCoder(NewCoder(c.Type()))
		ret = append(ret, c)
	}
	return ret, nil
}

// ParDoN inserts a ParDo with any number of outputs into the pipeline.
func ParDoN(s Scope, dofn interface{}, col PCollection, opts ...Option) []PCollection {
	return MustN(TryParDo(s, dofn, col, opts...))
}

// ParDo0 inserts a ParDo with zero output transform into the pipeline.
func ParDo0(s Scope, dofn interface{}, col PCollection, opts ...Option) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 0 {
		panic(formatParDoError(dofn, len(ret), 0))
	}
}

// ParDo is the core element-wise PTransform in Apache Beam, invoking a
// user-specified function on each of the elements of the input PCollection
// to produce zero or more output elements, all of which are collected into
// the output PCollection. Use one of the ParDo variants for a different
// number of output PCollections. The PCollections do no need to have the
// same types.
//
// Elements are processed independently, and possibly in parallel across
// distributed cloud resources. The ParDo processing style is similar to what
// happens inside the "Mapper" or "Reducer" class of a MapReduce-style
// algorithm.
//
// DoFns
//
// The function to use to process each element is specified by a DoFn, either as
// single function or as a struct with methods, notably ProcessElement. The
// struct may also define Setup, StartBundle, FinishBundle and Teardown methods.
// The struct is JSON-serialized and may contain construction-time values.
//
// Conceptually, when a ParDo transform is executed, the elements of the input
// PCollection are first divided up into some number of "bundles". These are
// farmed off to distributed worker machines (or run locally, if using the
// direct runner). For each bundle of input elements processing proceeds as
// follows:
//
//  * If a struct, a fresh instance of the argument DoFn is created on a
//    worker from json serialization, and the Setup method is called on this
//    instance, if present. A runner may reuse DoFn instances for multiple
//    bundles. A DoFn that has terminated abnormally (by returning an error)
//    will never be reused.
//  * The DoFn's StartBundle method, if provided, is called to initialize it.
//  * The DoFn's ProcessElement method is called on each of the input elements
//    in the bundle.
//  * The DoFn's FinishBundle method, if provided, is called to complete its
//    work. After FinishBundle is called, the framework will not again invoke
//    ProcessElement or FinishBundle until a new call to StartBundle has
//    occurred.
//  * If any of Setup, StartBundle, ProcessElement or FinishBundle methods
//    return an error, the Teardown method, if provided, will be called on the
//    DoFn instance.
//  * If a runner will no longer use a DoFn, the Teardown method, if provided,
//    will be called on the discarded instance.
//
// Each of the calls to any of the DoFn's processing methods can produce zero
// or more output elements. All of the of output elements from all of the DoFn
// instances are included in an output PCollection.
//
// For example:
//
//    words := beam.ParDo(s, &Foo{...}, ...)
//    lengths := beam.ParDo(s, func (word string) int) {
//          return len(word)
//    }, words)
//
//
// Each output element has the same timestamp and is in the same windows as its
// corresponding input element. The timestamp can be accessed and/or emitted by
// including a EventTime-typed parameter. The name of the function or struct is
// used as the DoFn name. Function literals do not have stable names and should
// thus not be used in production code.
//
// Side Inputs
//
// While a ParDo processes elements from a single "main input" PCollection, it
// can take additional "side input" PCollections. These SideInput along with
// the DoFn parameter form express styles of accessing PCollection computed by
// earlier pipeline operations, passed in to the ParDo transform using SideInput
// options, and their contents accessible to each of the DoFn operations. For
// example:
//
//     words := ...
//     cufoff := ...  // Singleton PCollection<int>
//     smallWords := beam.ParDo(s, func (word string, cutoff int, emit func(string)) {
//           if len(word) < cutoff {
//                emit(word)
//           }
//     }, words, beam.SideInput{Input: cutoff})
//
// Additional Outputs
//
// Optionally, a ParDo transform can produce zero or multiple output
// PCollections. Note the use of ParDo2 to specfic 2 outputs. For example:
//
//     words := ...
//     cufoff := ...  // Singleton PCollection<int>
//     small, big := beam.ParDo2(s, func (word string, cutoff int, small, big func(string)) {
//           if len(word) < cutoff {
//                small(word)
//           } else {
//                big(word)
//           }
//     }, words, beam.SideInput{Input: cutoff})
//
//
// By default, the Coders for the elements of each output PCollections is
// inferred from the concrete type.
//
// No Global Shared State
//
// There are three main ways to initialize the state of a DoFn instance
// processing a bundle:
//
//  * Define public instance variable state. This state will be automatically
//    JSON serialized and then deserialized in the DoFn instances created for
//    bundles. This method is good for state known when the original DoFn is
//    created in the main program, if it's not overly large. This is not
//    suitable for any state which must only be used for a single bundle, as
//    DoFn's may be used to process multiple bundles.
//
//  * Compute the state as a singleton PCollection and pass it in as a side
//    input to the DoFn. This is good if the state needs to be computed by the
//    pipeline, or if the state is very large and so is best read from file(s)
//    rather than sent as part of the DoFn's serialized state.
//
//  * Initialize the state in each DoFn instance, in a StartBundle method.
//    This is good if the initialization doesn't depend on any information
//    known only by the main program or computed by earlier pipeline
//    operations, but is the same for all instances of this DoFn for all
//    program executions, say setting up empty caches or initializing constant
//    data.
//
// ParDo operations are intended to be able to run in parallel across multiple
// worker machines. This precludes easy sharing and updating mutable state
// across those machines. There is no support in the Beam model for
// communicating and synchronizing updates to shared state across worker
// machines, so programs should not access any mutable global variable state in
// their DoFn, without understanding that the Go processes for the main program
// and workers will each have its own independent copy of such state, and there
// won't be any automatic copying of that state across Java processes. All
// information should be communicated to DoFn instances via main and side
// inputs and serialized state, and all output should be communicated from a
// DoFn instance via output PCollections, in the absence of external
// communication mechanisms written by user code.
//
// Splittable DoFns (Experimental)
//
// Warning: Splittable DoFns are still experimental, largely untested, and
// likely to have bugs.
//
// Splittable DoFns are DoFns that are able to split work within an element,
// as opposed to only at element boundaries like normal DoFns. This is useful
// for DoFns that emit many outputs per input element and can distribute that
// work among multiple workers. The most common examples of this are sources.
//
// In order to split work within an element, splittable DoFns use the concept of
// restrictions, which are objects that are associated with an element and
// describe a portion of work on that element. For example, a restriction
// associated with a filename might describe what byte range within that file to
// process. In addition to restrictions, splittable DoFns also rely on
// restriction trackers to track progress and perform splits on a restriction
// currently being processed. See the `RTracker` interface in core/sdf/sdf.go
// for more details.
//
// Splitting
//
// Splitting means taking one restriction and splitting into two or more that
// cover the entire input space of the original one. In other words, processing
// all the split restrictions should produce identical output to processing
// the original one.
//
// Splitting occurs in two stages. The initial splitting occurs before any
// restrictions have started processing. This step is used to split large
// restrictions into smaller ones that can then be distributed among multiple
// workers for processing. Initial splitting is user-defined and optional.
//
// Dynamic splitting occurs during the processing of a restriction in runners
// that have implemented it. If there are available workers, runners may split
// the unprocessed portion of work from a busy worker and shard it to available
// workers in order to better distribute work. With unsplittable DoFns this can
// only occur on element boundaries, but for splittable DoFns this split
// can land within a restriction and will require splitting that restriction.
//
// * Note: The Go SDK currently does not support dynamic splitting for SDFs,
//   only initial splitting. Only initially split restrictions can be
//   distributed by liquid sharding. Stragglers will not be split during
//   execution with dynamic splitting.
//
// Splittable DoFn Methods
//
// Making a splittable DoFn requires the following methods to be implemented on
// a DoFn in addition to the usual DoFn requirements. In the following
// method signatures `elem` represents the main input elements to the DoFn, and
// should match the types used in ProcessElement. `restriction` represents the
// user-defined restriction, and can be any type as long as it is consistent
// throughout all the splittable DoFn methods:
//
// * `CreateInitialRestriction(element) restriction`
//     CreateInitialRestriction creates an initial restriction encompassing an
//     entire element. The restriction created stays associated with the element
//     it describes.
// * `SplitRestriction(elem, restriction) []restriction`
//     SplitRestriction takes an element and its initial restriction, and
//     optionally performs an initial split on it, returning a slice of all the
//     split restrictions. If no splits are desired, the method returns a slice
//     containing only the original restriction. This method will always be
//     called on each newly created restriction before they are processed.
// * `RestrictionSize(elem, restriction) float64`
//     RestrictionSize returns a cheap size estimation for a restriction. This
//     size is an abstract scalar value that represents how much work a
//     restriction takes compared to other restrictions in the same DoFn. For
//     example, a size of 200 represents twice as much work as a size of
//     100, but the numbers do not represent anything on their own. Size is
//     used by runners to estimate work for liquid sharding.
// * `CreateTracker(restriction) restrictionTracker`
//     CreateTracker creates and returns a restriction tracker (a concrete type
//     implementing the `sdf.RTracker` interface) given a restriction. The
//     restriction tracker is used to track progress processing a restriction,
//     and to allow for dynamic splits. This method is called on each
//     restriction right before processing begins.
// * `ProcessElement(sdf.RTracker, element, func emit(output))`
//     For splittable DoFns, ProcessElement requires a restriction tracker
//     before inputs, and generally requires emits to be used for outputs, since
//     restrictions will generally produce multiple outputs. For more details
//     on processing restrictions in a splittable DoFn, see `sdf.RTracker`.
//
// Fault Tolerance
//
// In a distributed system, things can fail: machines can crash, machines can
// be unable to communicate across the network, etc. While individual failures
// are rare, the larger the job, the greater the chance that something,
// somewhere, will fail. Beam runners may strive to mask such failures by
// retrying failed DoFn bundles. This means that a DoFn instance might process
// a bundle partially, then crash for some reason, then be rerun (often as a
// new process) on that same bundle and on the same elements as before.
// Sometimes two or more DoFn instances will be running on the same bundle
// simultaneously, with the system taking the results of the first instance to
// complete successfully. Consequently, the code in a DoFn needs to be written
// such that these duplicate (sequential or concurrent) executions do not cause
// problems. If the outputs of a DoFn are a pure function of its inputs, then
// this requirement is satisfied. However, if a DoFn's execution has external
// side-effects, such as performing updates to external HTTP services, then
// the DoFn's code needs to take care to ensure that those updates are
// idempotent and that concurrent updates are acceptable. This property can be
// difficult to achieve, so it is advisable to strive to keep DoFns as pure
// functions as much as possible.
//
// Optimization
//
// Beam runners may choose to apply optimizations to a pipeline before it is
// executed. A key optimization, fusion, relates to ParDo operations. If one
// ParDo operation produces a PCollection that is then consumed as the main
// input of another ParDo operation, the two ParDo operations will be fused
// together into a single ParDo operation and run in a single pass; this is
// "producer-consumer fusion". Similarly, if two or more ParDo operations
// have the same PCollection main input, they will be fused into a single ParDo
// that makes just one pass over the input PCollection; this is "sibling
// fusion".
//
// If after fusion there are no more unfused references to a PCollection (e.g.,
// one between a producer ParDo and a consumer ParDo), the PCollection itself
// is "fused away" and won't ever be written to disk, saving all the I/O and
// space expense of constructing it.
//
// When Beam runners apply fusion optimization, it is essentially "free" to
// write ParDo operations in a very modular, composable style, each ParDo
// operation doing one clear task, and stringing together sequences of ParDo
// operations to get the desired overall effect. Such programs can be easier to
// understand, easier to unit-test, easier to extend and evolve, and easier to
// reuse in new programs. The predefined library of PTransforms that come with
// Beam makes heavy use of this modular, composable style, trusting to the
// runner to "flatten out" all the compositions into highly optimized stages.
//
// See https://beam.apache.org/documentation/programming-guide/#pardo
// for the web documentation for ParDo
func ParDo(s Scope, dofn interface{}, col PCollection, opts ...Option) PCollection {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 1 {
		panic(formatParDoError(dofn, len(ret), 1))
	}
	return ret[0]
}

// TODO(herohde) 6/1/2017: add windowing aspects to above documentation.

// ParDo2 inserts a ParDo with 2 outputs into the pipeline.
func ParDo2(s Scope, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 2 {
		panic(formatParDoError(dofn, len(ret), 2))
	}
	return ret[0], ret[1]
}

// ParDo3 inserts a ParDo with 3 outputs into the pipeline.
func ParDo3(s Scope, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, PCollection) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 3 {
		panic(formatParDoError(dofn, len(ret), 3))
	}
	return ret[0], ret[1], ret[2]
}

// ParDo4 inserts a ParDo with 4 outputs into the pipeline.
func ParDo4(s Scope, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, PCollection, PCollection) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 4 {
		panic(formatParDoError(dofn, len(ret), 4))
	}
	return ret[0], ret[1], ret[2], ret[3]
}

// ParDo5 inserts a ParDo with 5 outputs into the pipeline.
func ParDo5(s Scope, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, PCollection, PCollection, PCollection) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 5 {
		panic(formatParDoError(dofn, len(ret), 5))
	}
	return ret[0], ret[1], ret[2], ret[3], ret[4]
}

// ParDo6 inserts a ParDo with 6 outputs into the pipeline.
func ParDo6(s Scope, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, PCollection, PCollection, PCollection, PCollection) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 6 {
		panic(formatParDoError(dofn, len(ret), 6))
	}
	return ret[0], ret[1], ret[2], ret[3], ret[4], ret[5]
}

// ParDo7 inserts a ParDo with 7 outputs into the pipeline.
func ParDo7(s Scope, dofn interface{}, col PCollection, opts ...Option) (PCollection, PCollection, PCollection, PCollection, PCollection, PCollection, PCollection) {
	ret := MustN(TryParDo(s, dofn, col, opts...))
	if len(ret) != 7 {
		panic(formatParDoError(dofn, len(ret), 7))
	}
	return ret[0], ret[1], ret[2], ret[3], ret[4], ret[5], ret[6]
}

// formatParDoError is a helper function to provide a more concise error
// message to the users when a DoFn and its ParDo pairing is incorrect.
//
// We construct a new graph.Fn using the doFn which is passed. We explicitly
// ignore the error since we already know that its already a DoFn type as
// TryParDo would have panicked otherwise.
func formatParDoError(doFn interface{}, emitSize int, parDoSize int) string {
	doFun, _ := graph.NewFn(doFn)
	doFnName := doFun.Name()

	thisParDo := parDoForSize(parDoSize) // Conveniently keeps the API slim.
	correctParDo := parDoForSize(emitSize)

	return fmt.Sprintf("DoFn %v has %v outputs, but %v requires %v outputs, use %v instead.", doFnName, emitSize, thisParDo, parDoSize, correctParDo)
}

// parDoForSize takes a in a DoFns emit dimension and recommends the correct
// ParDo to use.
func parDoForSize(emitDim int) string {
	switch emitDim {
	case 0, 2, 3, 4, 5, 6, 7:
		return fmt.Sprintf("ParDo%d", emitDim)
	case 1:
		return "ParDo"
	default:
		return "ParDoN"
	}
}
