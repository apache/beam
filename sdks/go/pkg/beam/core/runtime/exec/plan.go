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
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Plan represents the bundle execution plan. It will generally be constructed
// from a part of a pipeline. A plan can be used to process multiple bundles
// serially.
type Plan struct {
	id       string
	roots    []Root
	units    []Unit
	parDoIDs []string

	status Status

	// While the store is threadsafe, the reference to it
	// is not, so we need to protect the store field to be
	// able to asynchronously provide tentative metrics.
	storeMu sync.Mutex
	store   *metrics.Store

	// TODO: there can be more than 1 DataSource in a bundle.
	source *DataSource
}

// hasPID provides a common interface for extracting PTransformIDs
// from Units.
type hasPID interface {
	GetPID() string
}

// NewPlan returns a new bundle execution plan from the given units.
func NewPlan(id string, units []Unit) (*Plan, error) {
	var roots []Root
	var source *DataSource
	var pardoIDs []string

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
		if p, ok := u.(hasPID); ok {
			pardoIDs = append(pardoIDs, p.GetPID())
		}
	}
	if len(roots) == 0 {
		return nil, errors.Errorf("no root units")
	}

	return &Plan{
		id:       id,
		status:   Initializing,
		roots:    roots,
		units:    units,
		parDoIDs: pardoIDs,
		source:   source,
	}, nil
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
	ctx = metrics.SetBundleID(ctx, p.id)
	p.storeMu.Lock()
	p.store = metrics.GetStore(ctx)
	p.storeMu.Unlock()
	if p.status == Initializing {
		for _, u := range p.units {
			if err := callNoPanic(ctx, u.Up); err != nil {
				p.status = Broken
				return errors.Wrapf(err, "while executing Up for %v", p)
			}
		}
		p.status = Up
	}
	if p.source != nil {
		p.source.InitSplittable()
	}

	if p.status != Up {
		return errors.Errorf("invalid status for plan %v: %v", p.id, p.status)
	}

	// Process bundle. If there are any kinds of failures, we bail and mark the plan broken.

	p.status = Active
	for _, root := range p.roots {
		if err := callNoPanic(ctx, func(ctx context.Context) error { return root.StartBundle(ctx, id, manager) }); err != nil {
			p.status = Broken
			return errors.Wrapf(err, "while executing StartBundle for %v", p)
		}
	}
	for _, root := range p.roots {
		if err := callNoPanic(ctx, root.Process); err != nil {
			p.status = Broken
			return errors.Wrapf(err, "while executing Process for %v", p)
		}
	}
	for _, root := range p.roots {
		if err := callNoPanic(ctx, root.FinishBundle); err != nil {
			p.status = Broken
			return errors.Wrapf(err, "while executing FinishBundle for %v", p)
		}
	}

	p.status = Up
	return nil
}

// Down takes the plan and associated units down. Does not panic.
func (p *Plan) Down(ctx context.Context) error {
	if p.status == Down {
		return nil // ok: already down
	}
	p.status = Down

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
	for _, u := range p.units {
		units = append(units, fmt.Sprintf("%v: %v", u.ID(), u))
	}
	return fmt.Sprintf("Plan[%v]:\n%v", p.ID(), strings.Join(units, "\n"))
}

// Progress returns a snapshot of input progress of the plan, and associated metrics.
func (p *Plan) Progress() (ProgressReportSnapshot, bool) {
	if p.source != nil {
		return p.source.Progress(), true
	}
	return ProgressReportSnapshot{}, false
}

// Store returns the metric store for the last use of this plan.
func (p *Plan) Store() *metrics.Store {
	p.storeMu.Lock()
	defer p.storeMu.Unlock()
	return p.store
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
	// Indices are always included, for both channel and sub-element splits.
	PI int64 // Primary index, last element of the primary.
	RI int64 // Residual index, first element of the residual.

	// Extra information included for sub-element splits. If PS and RS are
	// present then a sub-element split occurred.
	PS   []byte // Primary split. If an element is split, this is the encoded primary.
	RS   []byte // Residual split. If an element is split, this is the encoded residual.
	TId  string // Transform ID of the transform receiving the split elements.
	InId string // Input ID of the input the split elements are received from.
}

// Split takes a set of potential split indexes, and if successful returns
// the split result.
// Returns an error when unable to split.
func (p *Plan) Split(s SplitPoints) (SplitResult, error) {
	// TODO: When bundles with multiple sources, are supported, perform splits
	// on all sources.
	if p.source != nil {
		return p.source.Split(s.Splits, s.Frac, s.BufSize)
	}
	return SplitResult{}, fmt.Errorf("failed to split at requested splits: {%v}, Source not initialized", s)
}
