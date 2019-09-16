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
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Flatten is a PTransform that takes either multiple PCollections of type 'A'
// and returns a single PCollection of type 'A' containing all the elements in
// all the input PCollections. The name "Flatten" suggests taking a list of lists
// and flattening them into a single list.
//
// By default, the Coder of the output PCollection is the same as the Coder
// of the first PCollection.
func Flatten(s Scope, cols ...PCollection) PCollection {
	return Must(TryFlatten(s, cols...))
}

// TryFlatten merges incoming PCollections of type 'A' to a single PCollection
// of type 'A'. Returns an error indicating the set of PCollections that could
// not be flattened.
func TryFlatten(s Scope, cols ...PCollection) (PCollection, error) {
	if !s.IsValid() {
		return PCollection{}, errors.New("invalid scope")
	}
	for i, in := range cols {
		if !in.IsValid() {
			return PCollection{}, errors.Errorf("invalid pcollection to flatten: index %v", i)
		}
	}
	if len(cols) == 0 {
		return PCollection{}, errors.New("no input pcollections")
	}
	if len(cols) == 1 {
		return cols[0], nil // no-op
	}

	var in []*graph.Node
	for _, s := range cols {
		in = append(in, s.n)
	}
	edge, err := graph.NewFlatten(s.real, s.scope, in)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(cols[0].Coder())
	return ret, nil
}
