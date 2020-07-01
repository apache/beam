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

package exec

import (
	"context"
	"fmt"
	"path"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// PairWithRestriction is an executor for the expanded SDF step of the same
// name. This is the first step of an expanded SDF. It pairs each main input
// element with a restriction via the SDF's associated sdf.RestrictionProvider.
// This step is followed by SplitAndSizeRestrictions.
type PairWithRestriction struct {
	UID UnitID
	Fn  *graph.DoFn
	Out Node

	inv *cirInvoker
}

// ID returns the UnitID for this unit.
func (n *PairWithRestriction) ID() UnitID {
	return n.UID
}

// Up performs one-time setup for this executor.
func (n *PairWithRestriction) Up(ctx context.Context) error {
	fn := (*graph.SplittableDoFn)(n.Fn).CreateInitialRestrictionFn()
	var err error
	if n.inv, err = newCreateInitialRestrictionInvoker(fn); err != nil {
		return errors.WithContextf(err, "%v", n)
	}
	return nil
}

// StartBundle currently does nothing.
func (n *PairWithRestriction) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

// ProcessElement expects elm to be the main input to the ParDo. See
// exec.FullValue for more details on the expected input.
//
// ProcessElement creates an initial restriction representing the entire input.
// The output is in the structure <elem, restriction>, where elem is the main
// input originally passed in (i.e. the parameter elm). Windows and Timestamp
// are copied to the outer *FullValue. They can be left within the original
// element, but won't be used by later SDF steps.
//
// Output Diagram:
//
//   *FullValue {
//     Elm: *FullValue (original input)
//     Elm2: Restriction
//     Windows
//     Timestamps
//   }
func (n *PairWithRestriction) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	rest := n.inv.Invoke(elm)
	output := FullValue{Elm: elm, Elm2: rest, Timestamp: elm.Timestamp, Windows: elm.Windows}

	return n.Out.ProcessElement(ctx, &output, values...)
}

// FinishBundle resets the invokers.
func (n *PairWithRestriction) FinishBundle(ctx context.Context) error {
	n.inv.Reset()
	return n.Out.FinishBundle(ctx)
}

// Down currently does nothing.
func (n *PairWithRestriction) Down(ctx context.Context) error {
	return nil
}

// String outputs a human-readable description of this transform.
func (n *PairWithRestriction) String() string {
	return fmt.Sprintf("SDF.PairWithRestriction[%v] UID:%v Out:%v", path.Base(n.Fn.Name()), n.UID, IDs(n.Out))
}

// SplitAndSizeRestrictions is an executor for the expanded SDF step of the
// same name. It is the second step of the expanded SDF, occuring after
// CreateInitialRestriction. It performs initial splits on the initial restrictions
// and adds sizing information, producing one or more output elements per input
// element. This step is followed by ProcessSizedElementsAndRestrictions.
type SplitAndSizeRestrictions struct {
	UID UnitID
	Fn  *graph.DoFn
	Out Node

	splitInv *srInvoker
	sizeInv  *rsInvoker
}

// ID returns the UnitID for this unit.
func (n *SplitAndSizeRestrictions) ID() UnitID {
	return n.UID
}

// Up performs one-time setup for this executor.
func (n *SplitAndSizeRestrictions) Up(ctx context.Context) error {
	fn := (*graph.SplittableDoFn)(n.Fn).SplitRestrictionFn()
	var err error
	if n.splitInv, err = newSplitRestrictionInvoker(fn); err != nil {
		return errors.WithContextf(err, "%v", n)
	}

	fn = (*graph.SplittableDoFn)(n.Fn).RestrictionSizeFn()
	if n.sizeInv, err = newRestrictionSizeInvoker(fn); err != nil {
		return errors.WithContextf(err, "%v", n)
	}

	return nil
}

// StartBundle currently does nothing.
func (n *SplitAndSizeRestrictions) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

// ProcessElement expects elm.Elm to hold the original input while elm.Elm2
// contains the restriction.
//
// Input Diagram:
//
//   *FullValue {
//     Elm: *FullValue (original input)
//     Elm2: Restriction
//     Windows
//     Timestamps
//   }
//
// ProcessElement splits the given restriction into one or more restrictions and
// then sizes each. The outputs are in the structure <<elem, restriction>, size>
// where elem is the original main input to the unexpanded SDF. Windows and
// Timestamps are copied to each split output.
//
// Output Diagram:
//
//   *FullValue {
//     Elm: *FullValue {
//       Elm:  *FullValue (original input)
//       Elm2: Restriction
//     }
//     Elm2: float64 (size)
//     Windows
//     Timestamps
//   }
func (n *SplitAndSizeRestrictions) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	rest := elm.Elm2
	mainElm := elm.Elm.(*FullValue)

	splitRests := n.splitInv.Invoke(mainElm, rest)
	if len(splitRests) == 0 {
		err := errors.Errorf("initial splitting returned 0 restrictions.")
		return errors.WithContextf(err, "%v", n)
	}

	for _, splitRest := range splitRests {
		size := n.sizeInv.Invoke(mainElm, splitRest)
		output := &FullValue{}

		output.Timestamp = elm.Timestamp
		output.Windows = elm.Windows
		output.Elm = &FullValue{Elm: mainElm, Elm2: splitRest}
		output.Elm2 = size

		if err := n.Out.ProcessElement(ctx, output, values...); err != nil {
			return err
		}
	}

	return nil
}

// FinishBundle resets the invokers.
func (n *SplitAndSizeRestrictions) FinishBundle(ctx context.Context) error {
	n.splitInv.Reset()
	n.sizeInv.Reset()
	return n.Out.FinishBundle(ctx)
}

// Down currently does nothing.
func (n *SplitAndSizeRestrictions) Down(ctx context.Context) error {
	return nil
}

// String outputs a human-readable description of this transform.
func (n *SplitAndSizeRestrictions) String() string {
	return fmt.Sprintf("SDF.SplitAndSizeRestrictions[%v] UID:%v Out:%v", path.Base(n.Fn.Name()), n.UID, IDs(n.Out))
}

// ProcessSizedElementsAndRestrictions is an executor for the expanded SDF step
// of the same name. It is the final step of the expanded SDF. It sets up and
// invokes the user's SDF methods, similar to exec.ParDo but with slight
// changes to support the SDF's method signatures and the expected structure
// of the FullValue being received.
type ProcessSizedElementsAndRestrictions struct {
	PDo *ParDo

	inv *ctInvoker

	// Rt allows this unit to send out restriction trackers being processed.
	// Receivers of the tracker do not own it, and must send it back through the
	// same channel once finished with it.
	Rt chan sdf.RTracker
}

// ID calls the ParDo's ID method.
func (n *ProcessSizedElementsAndRestrictions) ID() UnitID {
	return n.PDo.ID()
}

// Up performs some one-time setup and then calls the ParDo's Up method.
func (n *ProcessSizedElementsAndRestrictions) Up(ctx context.Context) error {
	fn := (*graph.SplittableDoFn)(n.PDo.Fn).CreateTrackerFn()
	var err error
	if n.inv, err = newCreateTrackerInvoker(fn); err != nil {
		return errors.WithContextf(err, "%v", n)
	}
	n.Rt = make(chan sdf.RTracker, 1)
	return n.PDo.Up(ctx)
}

// StartBundle calls the ParDo's StartBundle method.
func (n *ProcessSizedElementsAndRestrictions) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.PDo.StartBundle(ctx, id, data)
}

// ProcessElement expects the same structure as the output of
// SplitAndSizeRestrictions, approximately <<elem, restriction>, size>. The
// only difference is that if the input was decoded in between the two steps,
// then single-element inputs were lifted from the *FullValue they were
// stored in.
//
// Input Diagram:
//
//   *FullValue {
//     Elm: *FullValue {
//       Elm:  *FullValue (KV input) or InputType (single-element input)
//       Elm2: Restriction
//     }
//     Elm2: float64 (size)
//     Windows
//     Timestamps
//   }
//
// ProcessElement then creates a restriction tracker from the stored restriction
// and processes each element using the underlying ParDo and adding the
// restriction tracker to the normal invocation. Sizing information is present
// but currently ignored. Output is forwarded to the underlying ParDo's outputs.
func (n *ProcessSizedElementsAndRestrictions) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	if n.PDo.status != Active {
		err := errors.Errorf("invalid status %v, want Active", n.PDo.status)
		return errors.WithContextf(err, "%v", n)
	}

	rest := elm.Elm.(*FullValue).Elm2
	rt := n.inv.Invoke(rest)
	n.Rt <- rt
	mainIn := &MainInput{
		Values:   values,
		RTracker: rt,
	}

	// For the key, the way we fill it out depends on whether the input element
	// is a KV or single-element. Single-elements might have been lifted out of
	// their FullValue if they were decoded, so we need to have a case for that.
	// Also, we use the the top-level windows and timestamp.
	// TODO(BEAM-9798): Optimize this so it's decided in exec/translate.go
	// instead of checking per-element.
	if userElm, ok := elm.Elm.(*FullValue).Elm.(*FullValue); ok {
		mainIn.Key = FullValue{
			Elm:       userElm.Elm,
			Elm2:      userElm.Elm2,
			Timestamp: elm.Timestamp,
			Windows:   elm.Windows,
		}
	} else {
		mainIn.Key = FullValue{
			Elm:       elm.Elm.(*FullValue).Elm,
			Timestamp: elm.Timestamp,
			Windows:   elm.Windows,
		}
	}

	err := n.PDo.processMainInput(mainIn)
	<-n.Rt
	return err
}

// FinishBundle resets the invokers and then calls the ParDo's FinishBundle method.
func (n *ProcessSizedElementsAndRestrictions) FinishBundle(ctx context.Context) error {
	n.inv.Reset()
	return n.PDo.FinishBundle(ctx)
}

// Down closes open channels and calls the ParDo's Down method.
func (n *ProcessSizedElementsAndRestrictions) Down(ctx context.Context) error {
	close(n.Rt)
	return n.PDo.Down(ctx)
}

// String outputs a human-readable description of this transform.
func (n *ProcessSizedElementsAndRestrictions) String() string {
	return fmt.Sprintf("SDF.ProcessSizedElementsAndRestrictions[%v] UID:%v Out:%v", path.Base(n.PDo.Fn.Name()), n.PDo.ID(), IDs(n.PDo.Out...))
}

// SdfFallback is an executor used when an SDF isn't expanded into steps by the
// runner, indicating that the runner doesn't support splitting. It executes all
// the SDF steps together in one unit.
type SdfFallback struct {
	PDo *ParDo

	initRestInv *cirInvoker
	splitInv    *srInvoker
	trackerInv  *ctInvoker
}

// ID calls the ParDo's ID method.
func (n *SdfFallback) ID() UnitID {
	return n.PDo.UID
}

// Up performs some one-time setup and then calls the ParDo's Up method.
func (n *SdfFallback) Up(ctx context.Context) error {
	dfn := (*graph.SplittableDoFn)(n.PDo.Fn)
	addContext := func(err error) error {
		return errors.WithContextf(err, "%v", n)
	}
	var err error
	if n.initRestInv, err = newCreateInitialRestrictionInvoker(dfn.CreateInitialRestrictionFn()); err != nil {
		return addContext(err)
	}
	if n.splitInv, err = newSplitRestrictionInvoker(dfn.SplitRestrictionFn()); err != nil {
		return addContext(err)
	}
	if n.trackerInv, err = newCreateTrackerInvoker(dfn.CreateTrackerFn()); err != nil {
		return addContext(err)
	}
	return n.PDo.Up(ctx)
}

// StartBundle calls the ParDo's StartBundle method.
func (n *SdfFallback) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.PDo.StartBundle(ctx, id, data)
}

// ProcessElement performs all the work from the steps above in one transform.
// This means creating initial restrictions, performing initial splits on those
// restrictions, and then creating restriction trackers and processing each
// restriction with the underlying ParDo. This executor skips the sizing step
// because sizing information is unnecessary for unexpanded SDFs.
func (n *SdfFallback) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	if n.PDo.status != Active {
		err := errors.Errorf("invalid status %v, want Active", n.PDo.status)
		return errors.WithContextf(err, "%v", n)
	}

	rest := n.initRestInv.Invoke(elm)
	splitRests := n.splitInv.Invoke(elm, rest)
	if len(splitRests) == 0 {
		err := errors.Errorf("initial splitting returned 0 restrictions.")
		return errors.WithContextf(err, "%v", n)
	}

	for _, splitRest := range splitRests {
		rt := n.trackerInv.Invoke(splitRest)
		mainIn := &MainInput{
			Key:      *elm,
			Values:   values,
			RTracker: rt,
		}
		if err := n.PDo.processMainInput(mainIn); err != nil {
			return err
		}
	}

	return nil
}

// FinishBundle resets the invokers and then calls the ParDo's FinishBundle method.
func (n *SdfFallback) FinishBundle(ctx context.Context) error {
	n.initRestInv.Reset()
	n.splitInv.Reset()
	n.trackerInv.Reset()
	return n.PDo.FinishBundle(ctx)
}

// Down calls the ParDo's Down method.
func (n *SdfFallback) Down(ctx context.Context) error {
	return n.PDo.Down(ctx)
}

// String outputs a human-readable description of this transform.
func (n *SdfFallback) String() string {
	return fmt.Sprintf("SDF.SdfFallback[%v] UID:%v Out:%v", path.Base(n.PDo.Fn.Name()), n.PDo.ID(), IDs(n.PDo.Out...))
}
