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

package internal_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/test/integration/primitives"
)

// This file covers pipelines with features that aren't yet supported by Prism.

func initTestName(fn any) string {
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
		// {pipeline: primitives.Drain}, // Can't test drain automatically yet.

		// Triggers (Need teststream and are unimplemented.)
		{pipeline: primitives.TriggerAfterAll},
		{pipeline: primitives.TriggerAfterAny},
		{pipeline: primitives.TriggerAfterEach},
		{pipeline: primitives.TriggerAfterEndOfWindow},
		{pipeline: primitives.TriggerAfterProcessingTime},
		{pipeline: primitives.TriggerAfterSynchronizedProcessingTime},
		{pipeline: primitives.TriggerElementCount},
		{pipeline: primitives.TriggerOrFinally},
		{pipeline: primitives.TriggerRepeat},
		{pipeline: primitives.TriggerAlways},
	}

	for _, test := range tests {
		t.Run(initTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err == nil {
				t.Fatalf("pipeline passed, but feature should be unimplemented in Prism")
			}
		})
	}
}

// TODO move these to a more appropriate location.
// Mostly placed here to have structural parity with the above test
// and make it easy to move them to a "it works" expectation.
func TestImplemented(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.Reshuffle},
		{pipeline: primitives.Flatten},
		{pipeline: primitives.FlattenDup},
		{pipeline: primitives.Checkpoints},
		{pipeline: primitives.CoGBK},
		{pipeline: primitives.ReshuffleKV},

		// The following have been "allowed" to unblock further development
		// But it's not clear these tests truly validate the expected behavior
		// of the triggers or panes.
		{pipeline: primitives.TriggerNever},
		{pipeline: primitives.Panes},
	}

	for _, test := range tests {
		t.Run(initTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
			}
		})
	}
}

func TestStateAPI(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.BagStateParDo},
		{pipeline: primitives.BagStateParDoClear},
		{pipeline: primitives.CombiningStateParDo},
		{pipeline: primitives.ValueStateParDo},
		{pipeline: primitives.ValueStateParDoClear},
		{pipeline: primitives.ValueStateParDoWindowed},
		{pipeline: primitives.MapStateParDo},
		{pipeline: primitives.MapStateParDoClear},
		{pipeline: primitives.SetStateParDo},
		{pipeline: primitives.SetStateParDoClear},
	}

	for _, test := range tests {
		t.Run(initTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
			}
		})
	}
}

func TestTimers(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.TimersEventTimeBounded},
		{pipeline: primitives.TimersEventTimeUnbounded},
	}

	for _, test := range tests {
		t.Run(initTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
			}
		})
	}
}

func TestTestStream(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
	}{
		{pipeline: primitives.TestStreamBoolSequence},
		{pipeline: primitives.TestStreamByteSliceSequence},
		{pipeline: primitives.TestStreamFloat64Sequence},
		{pipeline: primitives.TestStreamInt64Sequence},
		{pipeline: primitives.TestStreamStrings},
		{pipeline: primitives.TestStreamTwoBoolSequences},
		{pipeline: primitives.TestStreamTwoFloat64Sequences},
		{pipeline: primitives.TestStreamTwoInt64Sequences},
	}

	for _, test := range tests {
		t.Run(initTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			_, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatalf("pipeline failed, but feature should be implemented in Prism: %v", err)
			}
		})
	}
}
