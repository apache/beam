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
	"math"
	"path"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
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
func (n *PairWithRestriction) Up(_ context.Context) error {
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
func (n *PairWithRestriction) Down(_ context.Context) error {
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
func (n *SplitAndSizeRestrictions) Up(_ context.Context) error {
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

	for _, splitRest := range splitRests {
		size := n.sizeInv.Invoke(mainElm, splitRest)
		if size < 0 {
			err := errors.Errorf("size returned expected to be non-negative but received %v.", size)
			return errors.WithContextf(err, "%v", n)
		}
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
func (n *SplitAndSizeRestrictions) Down(_ context.Context) error {
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
	PDo     *ParDo
	TfId    string // Transform ID. Needed for splitting.
	ctInv   *ctInvoker
	sizeInv *rsInvoker

	// SU is a buffered channel for indicating when this unit is splittable.
	// When this unit is processing an element, it sends a SplittableUnit
	// interface through the channel. That interface can be received on other
	// threads and used to perform splitting or other related operation.
	//
	// This channel should be received on in a non-blocking manner, to avoid
	// hanging if no element is processing.
	//
	// Receiving the SplittableUnit prevents the current element from finishing
	// processing, so the element does not unexpectedly change during a split.
	// Therefore, receivers of the SplittableUnit must send it back through the
	// channel once finished with it, or it will block indefinitely.
	SU chan SplittableUnit

	elm   *FullValue   // Currently processing element.
	rt    sdf.RTracker // Currently processing element's restriction tracker.
	currW int          // Index of the current window in elm being processed.

	// Number of windows being processed. This number can differ from the number
	// of windows in an element, indicating to only process a subset of windows.
	// This can change during processing due to splits, but it should always be
	// set greater than currW.
	numW int
}

// ID calls the ParDo's ID method.
func (n *ProcessSizedElementsAndRestrictions) ID() UnitID {
	return n.PDo.ID()
}

// Up performs some one-time setup and then calls the ParDo's Up method.
func (n *ProcessSizedElementsAndRestrictions) Up(ctx context.Context) error {
	fn := (*graph.SplittableDoFn)(n.PDo.Fn).CreateTrackerFn()
	var err error
	if n.ctInv, err = newCreateTrackerInvoker(fn); err != nil {
		return errors.WithContextf(err, "%v", n)
	}
	fn = (*graph.SplittableDoFn)(n.PDo.Fn).RestrictionSizeFn()
	if n.sizeInv, err = newRestrictionSizeInvoker(fn); err != nil {
		return errors.WithContextf(err, "%v", n)
	}
	n.SU = make(chan SplittableUnit, 1)
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
func (n *ProcessSizedElementsAndRestrictions) ProcessElement(_ context.Context, elm *FullValue, values ...ReStream) error {
	if n.PDo.status != Active {
		err := errors.Errorf("invalid status %v, want Active", n.PDo.status)
		return errors.WithContextf(err, "%v", n)
	}

	// Package our element in a MainInput struct so the underlying ParDo can
	// process it.
	mainIn := &MainInput{
		Values: values,
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

	// Begin processing elements, exploding windows if necessary.
	n.currW = 0
	if !mustExplodeWindows(n.PDo.inv.fn, elm, len(n.PDo.Side) > 0) {
		// If windows don't need to be exploded (i.e. aren't observed), treat
		// all windows as one as an optimization.
		rest := elm.Elm.(*FullValue).Elm2
		rt := n.ctInv.Invoke(rest)
		mainIn.RTracker = rt

		n.numW = 1 // Even if there's more than one window, treat them as one.
		n.rt = rt
		n.elm = elm
		n.SU <- n
		defer func() {
			<-n.SU
		}()
		return n.PDo.processSingleWindow(mainIn)
	} else {
		// If we need to process the element in multiple windows, each one needs
		// its own RTracker and progress must be tracked among all windows by
		// currW updated between processing.
		n.numW = len(elm.Windows)

		for i := 0; i < n.numW; i++ {
			rest := elm.Elm.(*FullValue).Elm2
			rt := n.ctInv.Invoke(rest)
			key := &mainIn.Key
			w := elm.Windows[i]
			wElm := FullValue{Elm: key.Elm, Elm2: key.Elm2, Timestamp: key.Timestamp, Windows: []typex.Window{w}}

			n.currW = i
			n.rt = rt
			n.elm = elm
			n.SU <- n
			err := n.PDo.processSingleWindow(&MainInput{Key: wElm, Values: mainIn.Values, RTracker: rt})
			if err != nil {
				<-n.SU
				return n.PDo.fail(err)
			}
			<-n.SU
		}
	}
	return nil
}

// FinishBundle resets the invokers and then calls the ParDo's FinishBundle method.
func (n *ProcessSizedElementsAndRestrictions) FinishBundle(ctx context.Context) error {
	n.ctInv.Reset()
	n.sizeInv.Reset()
	return n.PDo.FinishBundle(ctx)
}

// Down calls the ParDo's Down method.
func (n *ProcessSizedElementsAndRestrictions) Down(ctx context.Context) error {
	return n.PDo.Down(ctx)
}

// String outputs a human-readable description of this transform.
func (n *ProcessSizedElementsAndRestrictions) String() string {
	return fmt.Sprintf("SDF.ProcessSizedElementsAndRestrictions[%v] UID:%v Out:%v", path.Base(n.PDo.Fn.Name()), n.PDo.ID(), IDs(n.PDo.Out...))
}

// SplittableUnit is an interface that defines sub-element splitting operations
// for a unit, and provides access to them on other threads.
type SplittableUnit interface {
	// Split performs a split on a fraction of a currently processing element
	// and returns zero or more primaries and residuals resulting from it, or an
	// error if the split failed.
	//
	// Zero primaries/residuals can be returned if the split succeeded but
	// resulted in no change. In this case, an empty slice is returned.
	//
	// More than one primary/residual can happen if the split result cannot be
	// fully represented in just one.
	Split(fraction float64) (primaries, residuals []*FullValue, err error)

	// GetProgress returns the fraction of progress the current element has
	// made in processing. (ex. 0.0 means no progress, and 1.0 means fully
	// processed.)
	GetProgress() float64

	// GetTransformId returns the transform ID of the splittable unit.
	GetTransformId() string

	// GetInputId returns the local input ID of the input that the element being
	// split was received from.
	GetInputId() string
}

// Split splits the currently processing element using its restriction tracker.
// Then it returns zero or more primaries and residuals, following the expected
// input structure to this unit, including updating the size of the split
// elements.
//
// This implementation of Split considers whether windows are being exploded
// for window-observing DoFns, and has significantly different behavior if
// windows need to be taken into account. For implementation details on when
// each case occurs and the implementation details, see the documentation for
// the singleWindowSplit and multiWindowSplit methods.
func (n *ProcessSizedElementsAndRestrictions) Split(f float64) ([]*FullValue, []*FullValue, error) {
	addContext := func(err error) error {
		return errors.WithContext(err, "Attempting split in ProcessSizedElementsAndRestrictions")
	}

	// Errors checking.
	if n.rt == nil {
		return nil, nil, addContext(errors.New("Restriction tracker missing."))
	}
	if err := n.rt.GetError(); err != nil {
		return nil, nil, addContext(err)
	}

	// Split behavior differs depending on whether this is a window-observing
	// DoFn or not.
	if len(n.elm.Windows) > 1 {
		p, r, err := n.multiWindowSplit(f)
		if err != nil {
			return nil, nil, addContext(err)
		}
		return p, r, nil
	}

	// Not window-observing, or window-observing but only one window.
	p, r, err := n.singleWindowSplit(f)
	if err != nil {
		return nil, nil, addContext(err)
	}
	return p, r, nil
}

// singleWindowSplit is intended for splitting elements in non window-observing
// DoFns (or single-window elements in window-observing DoFns, since the
// behavior is identical). A single restriction split will occur and all windows
// present in the unsplit element will be present in both the resulting primary
// and residual.
func (n *ProcessSizedElementsAndRestrictions) singleWindowSplit(f float64) ([]*FullValue, []*FullValue, error) {
	if n.rt.IsDone() { // Not an error, but not splittable.
		return []*FullValue{}, []*FullValue{}, nil
	}

	p, r, err := n.rt.TrySplit(f)
	if err != nil {
		return nil, nil, err
	}
	if r == nil { // If r is nil then the split failed/returned an empty residual.
		return []*FullValue{}, []*FullValue{}, nil
	}

	pfv, err := n.newSplitResult(p, n.elm.Windows)
	if err != nil {
		return nil, nil, err
	}
	rfv, err := n.newSplitResult(r, n.elm.Windows)
	if err != nil {
		return nil, nil, err
	}
	return []*FullValue{pfv}, []*FullValue{rfv}, nil
}

// multiWindowSplit is intended for splitting multi-window elements in
// window-observing DoFns. In window-observing DoFns, windows are exploded,
// and are processed one at a time, creating a new RTracker each time. This
// means a split occurs in a single spot somewhere in that sequence of single
// windows, so a restriction will be split only inside one window, or the split
// may occur at a boundary between windows and the restriction will not be split
// at all.
//
// Therefore, when such a split happens, the split result consists of:
// 1. A primary containing the unsplit restriction and a subset of windows that
//    fall within the primary.
// 2. (Optional) A second primary containing the primary half of a split
//    restriction and the single window it was split in.
// 3. A residual containing the unsplit restriction and a subset of windows that
//    fall within the residual.
// 4. (Optional) A second residual containing the residual half of a split
//    restriction and the window it was split in (same window as primary half).
//
// The current implementation does not split restrictions outside of the current
// RTracker (i.e. the current window). Otherwise, the split will occur at the
// nearest window boundary.
//
// This method also updates the current number of windows (n.numW) so that
// windows in the residual will no longer be processed.
func (n *ProcessSizedElementsAndRestrictions) multiWindowSplit(f float64) ([]*FullValue, []*FullValue, error) {
	// Get the split point in window range, to see what window it falls in.
	done, rem := n.rt.GetProgress()
	cwp := done / (done + rem)                      // Progress in current window.
	p := (float64(n.currW) + cwp) / float64(n.numW) // Progress of whole element.
	sp := p + (f * (1.0 - p))                       // Split point in range of entire element [0, 1].
	wsp := sp * float64(n.numW)                     // Split point in window range [0, numW].

	if int(wsp) == n.currW {
		// Split point lands in current window, so we can split via RTracker.
		if n.rt.IsDone() {
			// Current RTracker is done so we can't split within the window, so
			// split at window boundary instead.
			return n.windowBoundarySplit(n.currW + 1)
		}

		// Get the fraction of remaining work in the current window to split at.
		cwsp := wsp - float64(n.currW) // Split point in current window.
		rf := (cwsp - cwp) / (1 - cwp) // Fraction of work in RTracker to split at.

		return n.currentWindowSplit(rf)
	} else {
		// Split at nearest window boundary to split point.
		wb := math.Round(wsp)
		return n.windowBoundarySplit(int(wb))
	}
}

// currentWindowSplit performs an appropriate split at the given fraction of
// remaining work in the current window. Also updates numW to stop after the
// current window.
func (n *ProcessSizedElementsAndRestrictions) currentWindowSplit(f float64) ([]*FullValue, []*FullValue, error) {
	p, r, err := n.rt.TrySplit(f)
	if err != nil {
		return nil, nil, err
	}
	if r == nil {
		// If r is nil then the split failed/returned an empty residual, but
		// we can still split at a window boundary.
		return n.windowBoundarySplit(n.currW + 1)
	}

	// Split of currently processing restriction in a single window.
	ps := make([]*FullValue, 1)
	newP, err := n.newSplitResult(p, n.elm.Windows[n.currW:n.currW+1])
	if err != nil {
		return nil, nil, err
	}
	ps[0] = newP
	rs := make([]*FullValue, 1)
	newR, err := n.newSplitResult(r, n.elm.Windows[n.currW:n.currW+1])
	if err != nil {
		return nil, nil, err
	}
	rs[0] = newR
	// Window boundary split surrounding the split restriction above.
	full := n.elm.Elm.(*FullValue).Elm2
	if 0 < n.currW {
		newP, err := n.newSplitResult(full, n.elm.Windows[0:n.currW])
		if err != nil {
			return nil, nil, err
		}
		ps = append(ps, newP)
	}
	if n.currW+1 < n.numW {
		newR, err := n.newSplitResult(full, n.elm.Windows[n.currW+1:n.numW])
		if err != nil {
			return nil, nil, err
		}
		rs = append(rs, newR)
	}
	n.numW = n.currW + 1
	return ps, rs, nil
}

// windowBoundarySplit performs an appropriate split at a window boundary. The
// split point taken should be the index of the first window in the residual.
// Also updates numW to stop at the split point.
func (n *ProcessSizedElementsAndRestrictions) windowBoundarySplit(splitPt int) ([]*FullValue, []*FullValue, error) {
	// If this is at the boundary of the last window, split is a no-op.
	if splitPt == n.numW {
		return []*FullValue{}, []*FullValue{}, nil
	}
	full := n.elm.Elm.(*FullValue).Elm2
	pFv, err := n.newSplitResult(full, n.elm.Windows[0:splitPt])
	if err != nil {
		return nil, nil, err
	}
	rFv, err := n.newSplitResult(full, n.elm.Windows[splitPt:n.numW])
	if err != nil {
		return nil, nil, err
	}
	n.numW = splitPt
	return []*FullValue{pFv}, []*FullValue{rFv}, nil
}

// newSplitResult creates a FullValue containing a properly structured and sized
// element restriction pair based on the currently processing element, but with
// a modified restriction and windows. Intended for creating primaries and
// residuals to return as split results.
func (n *ProcessSizedElementsAndRestrictions) newSplitResult(rest interface{}, w []typex.Window) (*FullValue, error) {
	var size float64
	elm := n.elm.Elm.(*FullValue).Elm
	if fv, ok := elm.(*FullValue); ok {
		size = n.sizeInv.Invoke(fv, rest)
		if size < 0 {
			err := errors.Errorf("size returned expected to be non-negative but received %v.", size)
			return nil, errors.WithContextf(err, "%v", n)
		}
	} else {
		fv := &FullValue{Elm: elm}
		size = n.sizeInv.Invoke(fv, rest)
		if size < 0 {
			err := errors.Errorf("size returned expected to be non-negative but received %v.", size)
			return nil, errors.WithContextf(err, "%v", n)
		}
	}
	return &FullValue{
		Elm: &FullValue{
			Elm:  elm,
			Elm2: rest,
		},
		Elm2:      size,
		Timestamp: n.elm.Timestamp,
		Windows:   w,
	}, nil
}

// GetProgress returns the current restriction tracker's progress as a fraction.
// This implementation accounts for progress across windows in window-observing
// DoFns, so 1.0 is only returned once all windows have been processed.
func (n *ProcessSizedElementsAndRestrictions) GetProgress() float64 {
	d, r := n.rt.GetProgress()
	frac := d / (d + r)

	if n.numW == 1 {
		return frac
	}
	// Frac only covers currently processing element+window pair, so adjust it
	// to measure finished work throughout all windows.
	return (float64(n.currW) + frac) / float64(n.numW)
}

// GetTransformId returns this transform's transform ID.
func (n *ProcessSizedElementsAndRestrictions) GetTransformId() string {
	return n.TfId
}

// GetInputId returns the main input ID, since main input elements are being
// split.
func (n *ProcessSizedElementsAndRestrictions) GetInputId() string {
	return indexToInputId(0)
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
func (n *SdfFallback) ProcessElement(_ context.Context, elm *FullValue, values ...ReStream) error {
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
