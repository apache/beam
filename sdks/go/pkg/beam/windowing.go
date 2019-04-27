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

// WindowInto applies the windowing strategy to each element.
func WindowInto(s Scope, ws *window.Fn, col PCollection) PCollection {
	return Must(TryWindowInto(s, ws, col))
}

// TryWindowInto attempts to insert a WindowInto transform.
func TryWindowInto(s Scope, ws *window.Fn, col PCollection) (PCollection, error) {
	if !s.IsValid() {
		return PCollection{}, errors.New("invalid scope")
	}
	if !col.IsValid() {
		return PCollection{}, errors.New("invalid input pcollection")
	}

	edge := graph.NewWindowInto(s.real, s.scope, ws, col.n)
	ret := PCollection{edge.Output[0].To}
	return ret, nil
}
