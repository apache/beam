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
)

// UnitID is a unit identifier. Used for debugging.
type UnitID int

// Unit represents a processing unit capable of processing multiple bundles
// serially. Units are not required to be concurrency-safe. Each unit is
// responsible for propagating each data processing call downstream, i.e.,
// all calls except Up/Down, as appropriate.
type Unit interface {
	// ID returns the unit ID.
	ID() UnitID

	// Up initializes the unit. It is separate from Unit construction to
	// make panic/error handling easier.
	Up(ctx context.Context) error

	// StartBundle signals that processing preconditions, such as availability
	// of side input, are met and starts the given bundle.
	StartBundle(ctx context.Context, id string, data DataContext) error

	// FinishBundle signals end of input and thus finishes the bundle. Any
	// data connections must be closed.
	FinishBundle(ctx context.Context) error

	// Down tears down the processing node. It is notably called if the unit
	// or plan encounters an error and must thus robustly handle cleanup of
	// unfinished bundles. If a unit itself (as opposed to downstream units)
	// is the cause of breakage, the error returned should indicate the root
	// cause.
	Down(ctx context.Context) error
}

// Root represents a root processing unit. It contains its processing
// continuation, notably other nodes.
type Root interface {
	Unit

	// Process processes the entire source, notably emitting elements to
	// downstream nodes.
	Process(ctx context.Context) ([]*Checkpoint, error)
}

// ElementProcessor presents a component that can process an element.
type ElementProcessor interface {
	// Call processes a single element. If GBK or CoGBK result, the values
	// are populated. Otherwise, they're empty.
	// The *FullValue  is owned by the caller, and is not safe to cache.
	ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error
}

// Node represents an single-bundle processing unit. Each node contains
// its processing continuation, notably other nodes.
type Node interface {
	Unit
	ElementProcessor
}
