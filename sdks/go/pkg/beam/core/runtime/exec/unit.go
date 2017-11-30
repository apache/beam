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

type UnitID int

// Unit represents a processing unit.
type Unit interface {
	// ID returns the unit ID. Used for debugging.
	ID() UnitID
	// Up brings up the processing node. It currently signals that processing
	// preconditions, such as side input, are met and starts the bundle.
	Up(ctx context.Context) error
	// Down signals end of input and thus finishes the bundle. It also takes
	// down the processing node.
	Down(ctx context.Context) error

	// Status()
	// Error() error
}

// Root represents a root processing unit. It contains its processing
// continuation, notably other other nodes.
type Root interface {
	Unit

	// Process processes the entire source, notably emitting elements to
	// downstream nodes.
	Process(ctx context.Context) error
}

// Node represents an single-bundle processing unit. Each node contains
// its processing continuation, notably other nodes.
type Node interface {
	Unit

	// Call processes a single element. If GBK or CoGBK result, the values
	// are populated. Otherwise, they're empty.
	ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error
}

// GenID is a UnitID generator.
type GenID struct {
	last int
}

func (g *GenID) New() UnitID {
	g.last++
	return UnitID(g.last)
}

func Up(ctx context.Context, list ...Node) error {
	for _, out := range list {
		if err := out.Up(ctx); err != nil {
			return err
		}
	}
	return nil
}

func Down(ctx context.Context, list ...Node) error {
	for _, out := range list {
		if err := out.Down(ctx); err != nil {
			return err
		}
	}
	return nil
}

func IDs(list ...Node) []UnitID {
	var ret []UnitID
	for _, n := range list {
		ret = append(ret, n.ID())
	}
	return ret
}
