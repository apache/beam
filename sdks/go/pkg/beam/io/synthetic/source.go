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

// Package synthetic contains transforms for creating synthetic pipelines.
// Synthetic pipelines are pipelines that simulate the behavior of possible
// pipelines in order to test performance, splitting, liquid sharding, and
// various other infrastructure used for running pipelines. This category of
// tests is not concerned with the correctness of the elements themselves, but
// needs to simulate transforms that output many elements throughout varying
// pipeline shapes.
package synthetic

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/rtrackers/offsetrange"
	"math/rand"
	"time"
)

// Source creates a synthetic source transform that emits randomly
// generated KV<[]byte, []byte> elements.
//
// This transform accepts a PCollection of SourceConfig, where each SourceConfig
// determines the synthetic source's behavior for that element and outputs the
// randomly generated elements.
//
// SourceConfigs are recommended to be created via the DefaultSourceConfig and
// then sent to a beam.Create transform once modified. Example:
//
//    cfg1 := synthetic.DefaultSourceConfig()
//    cfg1.NumElements = 1000
//    cfg2 := synthetic.DefaultSourceConfig()
//    cfg2.NumElements = 5000
//    cfg2.InitialSplits = 2
//    cfgs := beam.Create(s, cfg1, cfg2)
//    src := synthetic.Source(s, cfgs)
func Source(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("synthetic.Source")

	return beam.ParDo(s, &sourceFn{}, col)
}

// sourceFn is a splittable DoFn implementing behavior for synthetic sources.
// For usage information, see synthetic.Source.
//
// The sourceFn is expected to receive elements of type sourceConfig and follow
// that config to determine its behavior when splitting and emitting elements.
type sourceFn struct {
	rng randWrapper
}

// CreateInitialRestriction creates an offset range restriction representing
// the number of elements to emit.
func (fn *sourceFn) CreateInitialRestriction(config SourceConfig) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   int64(config.NumElements),
	}
}

// SplitRestriction splits restrictions equally according to the number of
// initial splits specified in SourceConfig. Each restriction output by this
// method will contain at least one element, so the number of splits will not
// exceed the number of elements.
func (fn *sourceFn) SplitRestriction(config SourceConfig, rest offsetrange.Restriction) (splits []offsetrange.Restriction) {
	if config.InitialSplits <= 1 {
		// Don't split, just return original restriction.
		return append(splits, rest)
	}

	// TODO(BEAM-9978) Move this implementation of the offset range restriction
	// splitting to the restriction itself, and add testing.
	num := int64(config.InitialSplits)
	offset := rest.Start
	size := rest.End - rest.Start
	for i := int64(0); i < num; i++ {
		split := offsetrange.Restriction{
			Start: offset + (i * size / num),
			End:   offset + ((i + 1) * size / num),
		}
		// Skip restrictions that end up empty.
		if split.End-split.Start <= 0 {
			continue
		}
		splits = append(splits, split)
	}
	return splits
}

// RestrictionSize outputs the size of the restriction as the number of elements
// that restriction will output.
func (fn *sourceFn) RestrictionSize(config SourceConfig, rest offsetrange.Restriction) float64 {
	// TODO(BEAM-9978) Move this size implementation to the offset range restriction itself.
	return float64(rest.End - rest.Start)
}

// CreateTracker just creates an offset range restriction tracker for the
// restriction.
func (fn *sourceFn) CreateTracker(rest offsetrange.Restriction) *offsetrange.Tracker {
	return offsetrange.NewTracker(rest)
}

// Setup sets up the random number generator.
func (fn *sourceFn) Setup() {
	fn.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ProcessElement creates a number of random elements based on the restriction
// tracker received. Each element is a random byte slice key and value, in the
// form of KV<[]byte, []byte>.
func (fn *sourceFn) ProcessElement(rt *offsetrange.Tracker, config SourceConfig, emit func([]byte, []byte)) error {
	for i := rt.Rest.Start; rt.TryClaim(i) == true; i++ {
		key := make([]byte, 8)
		val := make([]byte, 8)
		if _, err := fn.rng.Read(key); err != nil {
			return err
		}
		if _, err := fn.rng.Read(val); err != nil {
			return err
		}
		emit(key, val)
	}
	return nil
}

// DefaultSourceConfig creates a SourceConfig with intended defaults for its
// fields. SourceConfigs should be initialized with this method.
func DefaultSourceConfig() SourceConfig {
	return SourceConfig{
		NumElements:   1, // Defaults shouldn't drop elements, so at least 1.
		InitialSplits: 1, // Defaults to 1, i.e. no initial splitting.
	}
}

// SourceConfig is a struct containing all the configuration options for a
// synthetic source.
type SourceConfig struct {
	// NumElements is the number of elements for the source to generate and
	// emit.
	NumElements int

	// InitialSplits determines the number of initial splits to perform in the
	// source's SplitRestriction method. Note that in some edge cases, the
	// number of splits performed might differ from this config value. Each
	// restriction will always have one element in it, and at least one
	// restriction will always be output, so the number of splits will be in
	// the range of [1, N] where N is the size of the original restriction.
	InitialSplits int
}
