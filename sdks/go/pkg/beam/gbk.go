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

// GroupByKey is a PTransform that takes a PCollection of type KV<A,B>,
// groups the values by key and windows, and returns a PCollection of type
// GBK<A,B> representing a map from each distinct key and window of the
// input PCollection to an iterable over all the values associated with
// that key in the input per window. Each key in the output PCollection is
// unique within each window.
//
// GroupByKey is analogous to converting a multi-map into a uni-map, and
// related to GROUP BY in SQL. It corresponds to the "shuffle" step between
// the Mapper and the Reducer in the MapReduce framework.
//
// Two keys of type A are compared for equality by first encoding each of the
// keys using the Coder of the keys of the input PCollection, and then
// comparing the encoded bytes. This admits efficient parallel evaluation.
// Note that this requires that the Coder of the keys be deterministic.
//
// By default, input and output PCollections share a key Coder and iterable
// values in the input and output PCollection share an element Coder.
//
// GroupByKey is a key primitive in data-parallel processing, since it is the
// main way to efficiently bring associated data together into one location.
// It is also a key determiner of the performance of a data-parallel pipeline.
//
// See CoGroupByKey for a way to group multiple input PCollections by a common
// key at once.
func GroupByKey(s Scope, a PCollection) PCollection {
	return CoGroupByKey(s, a)
}

// TODO(herohde) 5/30/2017: add windowing aspects to above documentation.
// TODO(herohde) 6/23/2017: support createWithFewKeys and other variants?

// TryGroupByKey inserts a GBK transform into the pipeline. Returns
// an error on failure.
func TryGroupByKey(s Scope, a PCollection) (PCollection, error) {
	return TryCoGroupByKey(s, a)
}

// CoGroupByKey inserts a CoGBK transform into the pipeline.
func CoGroupByKey(s Scope, cols ...PCollection) PCollection {
	return Must(TryCoGroupByKey(s, cols...))
}

func addCoGBKCtx(err error, s Scope) error {
	return errors.WithContextf(err, "inserting CoGroupByKey in scope %s", s)
}

// TryCoGroupByKey inserts a CoGBK transform into the pipeline. Returns
// an error on failure.
func TryCoGroupByKey(s Scope, cols ...PCollection) (PCollection, error) {
	if !s.IsValid() {
		return PCollection{}, addCoGBKCtx(errors.New("invalid scope"), s)
	}
	if len(cols) < 1 {
		return PCollection{}, addCoGBKCtx(errors.New("need at least 1 pcollection"), s)
	}
	for i, in := range cols {
		if !in.IsValid() {
			return PCollection{}, addCoGBKCtx(errors.Errorf("invalid pcollection to CoGBK: index %v", i), s)
		}
	}

	var in []*graph.Node
	for _, s := range cols {
		in = append(in, s.n)
	}

	edge, err := graph.NewCoGBK(s.real, s.scope, in)
	if err != nil {
		return PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

// Reshuffle copies a PCollection of the same kind and using the same element
// coder, and maintains the same windowing information. Importantly, it allows
// the result PCollection to be processed with a different sharding, in a
// different stage than the input PCollection.
//
// For example, if a computation needs a lot of parallelism but
// produces only a small amount of output data, then the computation
// producing the data can run with as much parallelism as needed,
// while the output file is written with a smaller amount of
// parallelism, using the following pattern:
//
//   pc := bigHairyComputationNeedingParallelism(scope) // PCollection<string>
//   resharded := beam.Reshuffle(scope, pc)                // PCollection<string>
//
// Another use case is when one has a non-deterministic DoFn followed by one
// that performs externally-visible side effects. Inserting a Reshuffle
// between these DoFns ensures that retries of the second DoFn will always be
// the same, which is necessary to make side effects idempotent.
//
// A Reshuffle will force a break in the optimized pipeline. Consequently,
// this operation should be used sparingly, only after determining that the
// pipeline without reshuffling is broken in some way and performing an extra
// operation is worth the cost.
func Reshuffle(s Scope, col PCollection) PCollection {
	return Must(TryReshuffle(s, col))
}

// TryReshuffle inserts a Reshuffle into the pipeline, and returns an error if
// the pcollection's unable to be reshuffled.
func TryReshuffle(s Scope, col PCollection) (PCollection, error) {
	addContext := func(err error, s Scope) error {
		return errors.WithContextf(err, "inserting Reshard in scope %s", s)
	}
	if !s.IsValid() {
		return PCollection{}, addContext(errors.New("invalid scope"), s)
	}
	if !col.IsValid() {
		return PCollection{}, addContext(errors.New("invalid pcollection"), s)
	}
	edge, err := graph.NewReshuffle(s.real, s.scope, col.n)
	if err != nil {
		return PCollection{}, addContext(err, s)
	}
	col.n.WindowingStrategy()
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}
