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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"path"

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
		return errors.WithContextf(err, "PairWithRestriction transform with UID %v", n.ID())
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
// are copied to the outer *FullValue. They still remain within the original
// element as well, but will no longer be used.
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

// FinishBundle does some teardown for the end of the bundle.
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
	return fmt.Sprintf("SDF.PairWithRestriction[%v] Out:%v", path.Base(n.Fn.Name()), IDs(n.Out))
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
		return errors.WithContextf(err, "SplitAndSizeRestrictions transform with UID %v", n.ID())
	}

	fn = (*graph.SplittableDoFn)(n.Fn).RestrictionSizeFn()
	if n.sizeInv, err = newRestrictionSizeInvoker(fn); err != nil {
		return errors.WithContextf(err, "SplitAndSizeRestrictions transform with UID %v", n.ID())
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
		return errors.WithContextf(err, "SplitAndSizeRestrictions transform with UID %v", n.ID())
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

// FinishBundle does some teardown for the end of the bundle.
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
	return fmt.Sprintf("SDF.SplitAndSizeRestrictions[%v] Out:%v", path.Base(n.Fn.Name()), IDs(n.Out))
}

// ProcessSizedElementsAndRestrictions is an executor for the expanded SDF step
// of the same name. It is the final step of the expanded SDF. It sets up and
// invokes the user's SDF methods, similar to exec.ParDo but with slight
// changes to support the SDF's method signatures and the expected structure
// of the FullValue being received.
type ProcessSizedElementsAndRestrictions struct {
	PDo *ParDo

	inv *ctInvoker
}

// ID just defers to the ParDo's ID method.
func (n *ProcessSizedElementsAndRestrictions) ID() UnitID {
	return n.PDo.ID()
}

// Up performs some one-time setup and then defers to the ParDo's Up method.
func (n *ProcessSizedElementsAndRestrictions) Up(ctx context.Context) error {
	fn := (*graph.SplittableDoFn)(n.PDo.Fn).CreateTrackerFn()
	var err error
	if n.inv, err = newCreateTrackerInvoker(fn); err != nil {
		return errors.WithContextf(err, "ProcessSizedElementsAndRestrictions transform with UID %v", n.ID())
	}
	return n.PDo.Up(ctx)
}

// StartBundle just defers to the ParDo's StartBundle method.
func (n *ProcessSizedElementsAndRestrictions) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.PDo.StartBundle(ctx, id, data)
}

// ProcessElement expects the same structure as the output of
// SplitAndSizeRestrictions, approximately <<elem, restriction>, size>
//
// Input Diagram:
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
//
// ProcessElement then creates a restriction tracker from the stored restriction
// and processes each element using the underlying ParDo and adding the
// restriction tracker to the normal invocation. Sizing information is present
// but currently ignored. Output is forwarded to the underlying ParDo's outputs.
func (n *ProcessSizedElementsAndRestrictions) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	if n.PDo.status != Active {
		return errors.Errorf("invalid status for ParDo %v: %v, want Active", n.PDo.UID, n.PDo.status)
	}

	userElm := elm.Elm.(*FullValue).Elm.(*FullValue)
	rest := elm.Elm.(*FullValue).Elm2
	rt := n.inv.Invoke(rest)

	return n.PDo.processMainInput(&MainInput{
		Key: FullValue{ // User userElm's values but the top-level windows and timestamp.
			Elm:       userElm.Elm,
			Elm2:      userElm.Elm2,
			Timestamp: elm.Timestamp,
			Windows:   elm.Windows,
		},
		Values:   values,
		RTracker: rt,
	})
}

// FinishBundle does some teardown for the end of the bundle and then defers to
// the ParDo's FinishBundle method.
func (n *ProcessSizedElementsAndRestrictions) FinishBundle(ctx context.Context) error {
	n.inv.Reset()
	return n.PDo.FinishBundle(ctx)
}

// Down just defers to the ParDo's Down method.
func (n *ProcessSizedElementsAndRestrictions) Down(ctx context.Context) error {
	return n.PDo.Down(ctx)
}

// String outputs a human-readable description of this transform.
func (n *ProcessSizedElementsAndRestrictions) String() string {
	return fmt.Sprintf("SDF.ProcessSizedElementsAndRestrictions[%v] Out:%v", path.Base(n.PDo.Fn.Name()), IDs(n.PDo.Out...))
}
