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

package internal

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/test/integration/primitives"
)

// This file covers pipelines with features that aren't yet supported by Prism.

func intTestName(fn any) string {
	name := reflectx.FunctionName(fn)
	n := strings.LastIndex(name, "/")
	return name[n+1:]
}

// TestUnimplemented validates that the kinds of pipelines that are expected
// to fail due to unimplemented features, do.
func TestUnimplemented(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		// These tests don't terminate, so can't be run.
		// {pipeline: primitives.Drain}, // Can't test drain automatically yet.
		// {pipeline: primitives.Checkpoints},  // Doesn't self terminate?
		// {pipeline: primitives.Flatten}, // Times out, should be quick.
		// {pipeline: primitives.FlattenDup}, // Times out, should be quick.

		{pipeline: primitives.TestStreamBoolSequence},
		{pipeline: primitives.TestStreamByteSliceSequence},
		{pipeline: primitives.TestStreamFloat64Sequence},
		{pipeline: primitives.TestStreamInt64Sequence},
		{pipeline: primitives.TestStreamStrings},
		{pipeline: primitives.TestStreamTwoBoolSequences},
		{pipeline: primitives.TestStreamTwoFloat64Sequences},
		{pipeline: primitives.TestStreamTwoInt64Sequences},

		// Needs teststream
		{pipeline: primitives.Panes},

		// Triggers (Need teststream and are unimplemented.)
		{pipeline: primitives.TriggerAlways},
		{pipeline: primitives.TriggerAfterAll},
		{pipeline: primitives.TriggerAfterAny},
		{pipeline: primitives.TriggerAfterEach},
		{pipeline: primitives.TriggerAfterEndOfWindow},
		{pipeline: primitives.TriggerAfterProcessingTime},
		{pipeline: primitives.TriggerAfterSynchronizedProcessingTime},
		{pipeline: primitives.TriggerElementCount},
		{pipeline: primitives.TriggerNever},
		{pipeline: primitives.TriggerOrFinally},
		{pipeline: primitives.TriggerRepeat},

		// Reshuffle (Due to missing windowing strategy features)
		{pipeline: primitives.Reshuffle},
		{pipeline: primitives.ReshuffleKV},

		// State API
		{pipeline: primitives.BagStateParDo},
		{pipeline: primitives.BagStateParDoClear},
		{pipeline: primitives.MapStateParDo},
		{pipeline: primitives.MapStateParDoClear},
		{pipeline: primitives.SetStateParDo},
		{pipeline: primitives.SetStateParDoClear},
		{pipeline: primitives.CombiningStateParDo},
		{pipeline: primitives.ValueStateParDo},
		{pipeline: primitives.ValueStateParDoClear},
		{pipeline: primitives.ValueStateParDoWindowed},

		// TODO: Timers integration tests.
	}

	for _, test := range tests {
		t.Run(intTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err == nil {
				t.Fatalf("pipeline passed, but feature should be unimplemented in Prism")
			}
		})
	}
}
