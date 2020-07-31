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
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"math/rand"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/rtrackers/offsetrange"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*sourceFn)(nil)).Elem())
}

// Source creates a synthetic source transform that emits randomly
// generated KV<[]byte, []byte> elements.
//
// This transform accepts a PCollection of SourceConfig, where each SourceConfig
// determines the synthetic source's behavior for producing a batch of elements.
// This allows multiple batches of elements to be produced with different
// behavior, in order to simulate a source transform that reads from multiple
// differently behaving sources, such as a file read that received small files
// and large files.
//
// The recommended way to create SourceConfigs is via the SourceConfigBuilder.
// Usage example:
//
//    cfgs := beam.Create(s,
//        synthetic.DefaultSourceConfig().NumElements(1000).Build(),
//        synthetic.DefaultSourceConfig().NumElements(5000).InitialSplits(2).Build())
//    src := synthetic.Source(s, cfgs)
func Source(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("synthetic.Source")

	return beam.ParDo(s, &sourceFn{}, col)
}

// SourceSingle creates a synthetic source transform that emits randomly
// generated KV<[]byte, []byte> elements.
//
// This transform is a version of Source for when only one SourceConfig is
// needed. This transform accepts one SourceConfig which determines the
// synthetic source's behavior.
//
// The recommended way to create SourceConfigs are via the SourceConfigBuilder.
// Usage example:
//
//    src := synthetic.SourceSingle(s,
//        synthetic.DefaultSourceConfig().NumElements(5000).InitialSplits(2).Build())
func SourceSingle(s beam.Scope, cfg SourceConfig) beam.PCollection {
	s = s.Scope("synthetic.Source")

	col := beam.Create(s, cfg)
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
	return rest.EvenSplits(int64(config.InitialSplits))
}

// RestrictionSize outputs the size of the restriction as the number of elements
// that restriction will output.
func (fn *sourceFn) RestrictionSize(_ SourceConfig, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker just creates an offset range restriction tracker for the
// restriction.
func (fn *sourceFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// Setup sets up the random number generator.
func (fn *sourceFn) Setup() {
	fn.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ProcessElement creates a number of random elements based on the restriction
// tracker received. Each element is a random byte slice key and value, in the
// form of KV<[]byte, []byte>.
func (fn *sourceFn) ProcessElement(rt *sdf.LockRTracker, _ SourceConfig, emit func([]byte, []byte)) error {
	for i := rt.GetRestriction().(offsetrange.Restriction).Start; rt.TryClaim(i) == true; i++ {
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

// SourceConfigBuilder is used to initialize SourceConfigs. See
// SourceConfigBuilder's methods for descriptions of the fields in a
// SourceConfig and how they can be set. The intended approach for using this
// builder is to begin by calling the DefaultSourceConfig function, followed by
// calling setters, followed by calling Build.
//
// Usage example:
//
//    cfg := synthetic.DefaultSourceConfig().NumElements(5000).InitialSplits(2).Build()
type SourceConfigBuilder struct {
	cfg SourceConfig
}

// DefaultSourceConfig creates a SourceConfigBuilder set with intended defaults
// for the SourceConfig fields. This function is the intended starting point for
// initializing a SourceConfig and should always be used to create
// SourceConfigBuilders.
//
// To see descriptions of the various SourceConfig fields and their defaults,
// see the methods to SourceConfigBuilder.
func DefaultSourceConfig() *SourceConfigBuilder {
	return &SourceConfigBuilder{
		cfg: SourceConfig{
			NumElements:   1, // 0 is invalid (drops elements).
			InitialSplits: 1, // 0 is invalid (drops elements).
		},
	}
}

// NumElements is the number of elements for the source to generate and emit.
//
// Valid values are in the range of [1, ...] and the default value is 1. Values
// of 0 (and below) are invalid as they result in sources that emit no elements.
func (b *SourceConfigBuilder) NumElements(val int) *SourceConfigBuilder {
	b.cfg.NumElements = val
	return b
}

// InitialSplits determines the number of initial splits to perform in the
// source's SplitRestriction method. Restrictions in synthetic sources represent
// the number of elements being emitted, and this split is performed evenly
// across that number of elements.
//
// Each resulting restriction will have at least 1 element in it, and each
// element being emitted will be contained in exactly one restriction. That
// means that if the desired number of splits is greater than the number of
// elements N, then N initial restrictions will be created, each containing 1
// element.
//
// Valid values are in the range of [1, ...] and the default value is 1. Values
// of 0 (and below) are invalid as they would result in dropping elements that
// are expected to be emitted.
func (b *SourceConfigBuilder) InitialSplits(val int) *SourceConfigBuilder {
	b.cfg.InitialSplits = val
	return b
}

// Build constructs the SourceConfig initialized by this builder. It also
// performs error checking on the fields, and panics if any have been set to
// invalid values.
func (b *SourceConfigBuilder) Build() SourceConfig {
	if b.cfg.InitialSplits <= 0 {
		panic(fmt.Sprintf("SourceConfig.InitialSplits must be >= 1. Got: %v", b.cfg.InitialSplits))
	}
	if b.cfg.NumElements <= 0 {
		panic(fmt.Sprintf("SourceConfig.NumElements must be >= 1. Got: %v", b.cfg.NumElements))
	}
	return b.cfg
}

// SourceConfig is a struct containing all the configuration options for a
// synthetic source. It should be created via a SourceConfigBuilder, not by
// directly initializing it (the fields are public to allow encoding).
type SourceConfig struct {
	NumElements   int
	InitialSplits int
}
