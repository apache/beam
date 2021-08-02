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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

type WindowIntoOption interface {
	windowIntoOption()
}

type WindowTrigger struct {
	Name window.TriggerType
}

func (t WindowTrigger) windowIntoOption() {}

func (t WindowTrigger) GetName() window.TriggerType {
	return t.Name
}

// WindowInto applies the windowing strategy to each element.
func WindowInto(s Scope, ws *window.Fn, col PCollection, opts ...WindowIntoOption) PCollection {
	return Must(TryWindowInto(s, ws, col, opts...))
}

// TryWindowInto attempts to insert a WindowInto transform.
func TryWindowInto(s Scope, ws *window.Fn, col PCollection, opts ...WindowIntoOption) (PCollection, error) {
	if !s.IsValid() {
		return PCollection{}, errors.New("invalid scope")
	}
	if !col.IsValid() {
		return PCollection{}, errors.New("invalid input pcollection")
	}
	var edge *graph.MultiEdge
	for _, opt := range opts {
		switch opt.(type) {
		case WindowTrigger:
			edge = graph.NewWindowInto(s.real, s.scope, &window.WindowingStrategy{Fn: ws, Trigger: opt.(WindowTrigger).GetName()}, col.n)
		default:
			edge = graph.NewWindowInto(s.real, s.scope, &window.WindowingStrategy{Fn: ws, Trigger: window.Default}, col.n)
		}
	}

	if len(opts) == 0 {
		edge = graph.NewWindowInto(s.real, s.scope, &window.WindowingStrategy{Fn: ws, Trigger: window.Default}, col.n)
	}

	ret := PCollection{edge.Output[0].To}
	return ret, nil
}
