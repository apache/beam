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

package primitives

import (
	"context"
	"flag"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
)

var (
	output = flag.String("output", "", "output pubsub topic to write to.")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*TruncateFn)(nil)).Elem())
}

type RangeEstimator struct {
	End int64
}

func (r *RangeEstimator) Estimate() int64 {
	return r.End
}

func (r *RangeEstimator) SetEstimate(estimate int64) {
	r.End = estimate
}

type TruncateFn struct {
	Estimator RangeEstimator
}

// CreateInitialRestriction creates an initial restriction
func (fn *TruncateFn) CreateInitialRestriction(b []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: int64(1),
		End:   int64(len(b)),
	}
}

// CreateTracker wraps the fiven restriction into a LockRTracker type.
func (fn *TruncateFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	// return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
	fn.Estimator = RangeEstimator{50}
	tracker, err := offsetrange.NewGrowableTracker(rest.Start, &fn.Estimator)
	if err != nil {
		panic(err)
	}
	return sdf.NewLockRTracker(tracker)
}

// RestrictionSize returns the size of the current restriction
func (fn *TruncateFn) RestrictionSize(_ []byte, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// SplitRestriction is similar to the one used in checkpointing.go test.
func (fn *TruncateFn) SplitRestriction(_ []byte, rest offsetrange.Restriction) []offsetrange.Restriction {
	size := int64(1)
	s := rest.Start
	var splits []offsetrange.Restriction
	for e := s + size; e < rest.End; s, e = e, e+size {
		if s == e {
			continue
		}
		splits = append(splits, offsetrange.Restriction{Start: s, End: e})
	}
	if s == rest.End {
		return splits
	}
	splits = append(splits, offsetrange.Restriction{Start: s, End: rest.End})
	return splits
}

func (fn *TruncateFn) TruncateRestriction(rt *sdf.LockRTracker, _ []byte) offsetrange.Restriction {
	log.Debug(context.Background(), "triggering the truncate restriction")

	return offsetrange.Restriction{
		Start: rt.GetRestriction().(offsetrange.Restriction).Start,
		End:   rt.GetRestriction().(offsetrange.Restriction).End / 4,
	}
}

// ProcessElement continually gets the start position of the restriction and emits the element as it is.
func (fn *TruncateFn) ProcessElement(rt *sdf.LockRTracker, p []byte, emit func([]byte)) sdf.ProcessContinuation {
	position := rt.GetRestriction().(offsetrange.Restriction).Start
	if rt.TryClaim(position) {
		position += 1
		emit(p)
		return sdf.ResumeProcessingIn(1 * time.Second)
	} else if rt.GetError() != nil || rt.IsDone() {
		return sdf.StopProcessing()
	} else {
		return sdf.ResumeProcessingIn(5 * time.Second)
	}
}

func Drain(s beam.Scope) {
	beam.Init()
	project := "pubsub-public-data"

	input := "taxirides-realtime"
	output := *output

	col := pubsubio.Read(s, project, input, &pubsubio.ReadOptions{})
	cap := beam.ParDo(s, &TruncateFn{}, col)
	project = gcpopts.GetProject(context.Background())
	pubsubio.Write(s, project, output, cap)
}
