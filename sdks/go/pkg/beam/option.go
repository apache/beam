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
	"reflect"
)

// Option is an optional value or context to a transformation, used at pipeline
// construction time. The primary use case is providing side inputs.
type Option interface {
	private()
}

// SideInput provides a view of the given PCollection to the transformation.
type SideInput struct {
	Input PCollection

	// WindowFn interface{}
	// ViewFn   interface{}
}

func (s SideInput) private() {}

// TypeDefinition provides construction-time type information that the platform
// cannot infer, such as structured storage sources. These types are universal types
// that appear as output only. Types that are inferrable should not be conveyed via
// this mechanism.
type TypeDefinition struct {
	// Var is the universal type defined.
	Var reflect.Type
	// T is the type it is bound to.
	T reflect.Type
}

func (s TypeDefinition) private() {}

func parseOpts(opts []Option) ([]SideInput, []TypeDefinition) {
	var side []SideInput
	var infer []TypeDefinition

	for _, opt := range opts {
		switch opt.(type) {
		case SideInput:
			side = append(side, opt.(SideInput))
		case TypeDefinition:
			infer = append(infer, opt.(TypeDefinition))
		default:
			panic(fmt.Sprintf("Unexpected opt: %v", opt))
		}
	}
	return side, infer
}
