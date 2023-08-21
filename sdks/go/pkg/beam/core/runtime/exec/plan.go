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

// Package exec contains runtime plan representation and execution. A pipeline
// must be translated to a runtime plan to be executed.
package exec

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Plan represents the bundle execution plan. It will generally be constructed
// from a part of a pipeline. A plan can be used to process multiple bundles
// serially.
type Plan struct {
	id          string // id of the bundle descriptor for this plan
	roots       []Root
	units       []Unit
	pcols       []*PCollection
	bf          *bundleFinalizer
	checkpoints []*Checkpoint

	status Status // Uses atomic getter and setter to avoid dataraces on Splits.

	// TODO: there can be more than 1 DataSource in a bundle.
	source *DataSource
}

// NewPlan returns a new bundle execution plan from the given units.
func NewPlan(id string, units []Unit) (*Plan, error) {
	var roots []Root
	var pcols []*PCollection
	var source *DataSource
	bf := bundleFinalizer{
		callbacks:         []bundleFinalizationCallback{},
		lastValidCallback: time.Now(),
	}
	var onTimers map[string]*ParDo

	for _, u := range units {
		if u == nil {
			return nil, errors.Errorf("no <nil> units")
		}
		if r, ok := u.(Root); ok {
			roots = append(roots, r)
		}
		if s, ok := u.(*DataSource); ok {
			source = s
		}
		if p, ok := u.(*PCollection); ok {
			pcols = append(pcols, p)
		}
		if pd, ok := u.(*ParDo); ok && pd.HasOnTimer() {
			if onTimers == nil {
				onTimers = map[string]*ParDo{}
			}
			onTimers[pd.PID] = pd
		}
		if p, ok := u.(needsBundleFinalization); ok {
			p.AttachFinalizer(&bf)
		}
	}
	if len(roots) == 0 {
		return nil, errors.Errorf("no root units")
	}

	if len(onTimers) > 0 {
		source.OnTimerTransforms = onTimers
	}

	return &Plan{
		id:     id,
		status: Initializing,
		roots:  roots,
		units:  units,
		pcols:  pcols,
		bf:     &bf,
		source: source,
	}, nil
}

func (p *Plan) getStatus() Status {
	return Status(atomic.LoadInt32((*int32)(&p.status)))
}

func (p *Plan) setStatus(s Status) {
	atomic.StoreInt32((*int32)(&p.status), int32(s))
}

// ID returns the plan identifier.
func (p *Plan) ID() string {
	return p.id
}

// SourcePTransformID returns the ID of the data's origin PTransform.
func (p *Plan) SourcePTransformID() string {
	return p.source.SID.PtransformID
}

// Execute executes the plan with the given data context and bundle id. Units
// are brought up on the first execution. If a bundle fails, the plan cannot
// be reused for further bundles. Does not panic. Blocking.
func (p *Plan) Execute(ctx context.Context, id string, manager DataContext) error {
	if p.getStatus() == Initializing {
		for _, u := range p.units {
			if err := callNoPanic(ctx, u.Up); err != nil {
				p.setStatus(Broken)
				return errors.Wrapf(err, "while executing Up for %v", p)
			}
		}
		p.setStatus(Up)
	}
	if p.source != nil {
		p.source.InitSplittable()
	}

	if s := p.getStatus(); s != Up {
		return errors.Errorf("invalid status for plan %v: %v", p.id, s)
	}

	// Process bundle. If there are any kinds of failures, we bail and mark the plan broken.

	p.setStatus(Active)
	for _, root := range p.roots {
		if err := callNoPanic(ctx, func(ctx context.Context) error { return root.StartBundle(ctx, id, manager) }); err != nil {
			p.setStatus(Broken)
			return errors.Wrapf(err, "while executing StartBundle for %v", p)
		}
	}
	for _, root := range p.roots {
		if err := callNoPanic(ctx, func(ctx context.Context) error {
			cps, err := root.Process(ctx)
			p.checkpoints = cps
			return err
		}); err != nil {
			p.setStatus(Broken)
			return errors.Wrapf(err, "while executing Process for %v", p)
		}
	}
	for _, root := range p.roots {
		if err := callNoPanic(ctx, root.FinishBundle); err != nil {
			p.setStatus(Broken)
			return errors.Wrapf(err, "while executing FinishBundle for %v", p)
		}
	}
	p.setStatus(Up)
	return nil
}

// Finalize runs any callbacks registered by the bundleFinalizer. Should be run on bundle finalization.
func (p *Plan) Finalize() error {
	if s := p.getStatus(); s != Up {
		return errors.Errorf("invalid status for plan %v: %v", p.id, s)
	}
	failedIndices := []int{}
	for idx, bfc := range p.bf.callbacks {
		if time.Now().Before(bfc.validUntil) {
			if err := bfc.callback(); err != nil {
				failedIndices = append(failedIndices, idx)
			}
		}
	}

	newFinalizer := bundleFinalizer{
		callbacks:         []bundleFinalizationCallback{},
		lastValidCallback: time.Now(),
	}

	for _, idx := range failedIndices {
		newFinalizer.callbacks = append(newFinalizer.callbacks, p.bf.callbacks[idx])
		if newFinalizer.lastValidCallback.Before(p.bf.callbacks[idx].validUntil) {
			newFinalizer.lastValidCallback = p.bf.callbacks[idx].validUntil
		}
	}

	p.bf = &newFinalizer

	if len(failedIndices) > 0 {
		return errors.Errorf("Plan %v failed %v callbacks", p.ID(), len(failedIndices))
	}
	return nil
}

// GetExpirationTime returns the last expiration time of any of the callbacks registered by the bundleFinalizer.
// Once we have passed this time, it is safe to move this plan to inactive without missing any valid callbacks.
func (p *Plan) GetExpirationTime() time.Time {
	return p.bf.lastValidCallback
}

// Down takes the plan and associated units down. Does not panic.
func (p *Plan) Down(ctx context.Context) error {
	// Technically racy, but only one thread calls this method on the plan.
	if p.getStatus() == Down {
		return nil // ok: already down
	}
	p.setStatus(Down)

	var errs []error
	for _, u := range p.units {
		if err := callNoPanic(ctx, u.Down); err != nil {
			errs = append(errs, err)
		}
	}

	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errors.Wrapf(errs[0], "plan %v failed", p.id)
	default:
		return errors.Errorf("plan %v failed with multiple errors: %v", p.id, errs)
	}
}

func (p *Plan) String() string {
	var units []string
	for i := len(p.units) - 1; i >= 0; i-- {
		u := p.units[i]
		units = append(units, fmt.Sprintf("%v: %v", u.ID(), u))
	}
	return fmt.Sprintf("Plan[%v]:\n%v", p.ID(), strings.Join(units, "\n"))
}

// PlanSnapshot contains system metrics for the current run of the plan.
type PlanSnapshot struct {
	Source ProgressReportSnapshot
	PCols  []PCollectionSnapshot
}

// Progress returns a snapshot of progress of the plan, and associated metrics.
// The retuend boolean indicates whether the plan includes a DataSource, which is
// important for handling legacy metrics. This boolean will be removed once
// we no longer return legacy metrics.
func (p *Plan) Progress() (PlanSnapshot, bool) {
	pcolSnaps := make([]PCollectionSnapshot, 0, len(p.pcols)+1) // include space for the datasource pcollection.
	for _, pcol := range p.pcols {
		pcolSnaps = append(pcolSnaps, pcol.snapshot())
	}
	snap := PlanSnapshot{PCols: pcolSnaps}
	if p.source != nil {
		snap.Source = p.source.Progress()
		snap.PCols = append(pcolSnaps, snap.Source.pcol)
		return snap, true
	}
	return snap, false
}

// SplitPoints captures the split requested by the Runner.
type SplitPoints struct {
	// Splits is a list of desired split indices.
	Splits []int64
	Frac   float64

	// Estimated total number of elements (including unsent) for the source.
	// A zero value indicates unknown, instead use locally known size.
	BufSize int64
}

// SplitResult contains the result of performing a split on a Plan.
type SplitResult struct {
	Unsuccessful bool // Indicates the split was unsuccessful.

	// Indices are always included, for both channel and sub-element splits.
	PI int64 // Primary index, last element of the primary.
	RI int64 // Residual index, first element of the residual.

	// Extra information included for sub-element splits. If PS and RS are
	// present then a sub-element split occurred.
	PS   [][]byte // Primary splits. If an element is split, these are the encoded primaries.
	RS   [][]byte // Residual splits. If an element is split, these are the encoded residuals.
	TId  string   // Transform ID of the transform receiving the split elements.
	InId string   // Input ID of the input the split elements are received from.

	OW map[string]*timestamppb.Timestamp // Map of outputs to output watermark for the plan being split
}

// Split takes a set of potential split indexes, and if successful returns
// the split result.
// Returns an error when unable to split.
func (p *Plan) Split(ctx context.Context, s SplitPoints) (SplitResult, error) {
	// Can't split inactive plans.
	// Split occurs asynchronously, so the state check here must be atomic.
	if p.getStatus() != Active {
		return SplitResult{Unsuccessful: true}, nil
	}
	// TODO: When bundles with multiple sources, are supported, perform splits
	// on all sources.
	return p.source.Split(ctx, s.Splits, s.Frac, s.BufSize)
}

// Checkpoint attempts to split an SDF if the DoFn self-checkpointed.
func (p *Plan) Checkpoint() []*Checkpoint {
	defer func() { p.checkpoints = nil }()
	return p.checkpoints
}
