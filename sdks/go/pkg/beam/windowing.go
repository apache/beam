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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

type WindowIntoOption interface {
	windowIntoOption()
}

type WindowTrigger struct {
	Name window.Trigger
}

func (t WindowTrigger) windowIntoOption() {}

// Trigger applies `tr` trigger to the window.
func Trigger(tr window.Trigger) WindowTrigger {
	return WindowTrigger{Name: tr}
}

type AccumulationMode struct {
	Mode window.AccumulationMode
}

func (m AccumulationMode) windowIntoOption() {}

// PanesAccumulate applies an Accumulating AccumulationMode to the window.
func PanesAccumulate() AccumulationMode {
	return AccumulationMode{Mode: window.Accumulating}
}

// PanesDiscard applies a Discarding AccumulationMode to the window.
func PanesDiscard() AccumulationMode {
	return AccumulationMode{Mode: window.Discarding}
}

// WindowInto applies the windowing strategy to each element.
func WindowInto(s Scope, ws *window.Fn, col PCollection, opts ...WindowIntoOption) PCollection {
	return Must(TryWindowInto(s, ws, col, opts...))
}

// TryWindowInto attempts to insert a WindowInto transform.
func TryWindowInto(s Scope, wfn *window.Fn, col PCollection, opts ...WindowIntoOption) (PCollection, error) {
	if !s.IsValid() {
		return PCollection{}, errors.New("invalid scope")
	}
	if !col.IsValid() {
		return PCollection{}, errors.New("invalid input pcollection")
	}
	ws := window.WindowingStrategy{Fn: wfn, Trigger: window.Trigger{}}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case WindowTrigger:
			ws.Trigger = opt.Name
		case AccumulationMode:
			ws.AccumulationMode = opt.Mode
		default:
			panic(fmt.Sprintf("Unknown WindowInto option type: %T: %v", opt, opt))
		}
	}

	edge := graph.NewWindowInto(s.real, s.scope, &ws, col.n)
	ret := PCollection{edge.Output[0].To}
	return ret, nil
}
