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
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
)

func init() {
	RegisterType(reflect.TypeOf((*AnnotationsFn)(nil)))
}

func TestParDoForSize(t *testing.T) {
	var tests = []struct {
		name      string
		outputDim int
		want      string
	}{
		{"zero outputs", 0, "ParDo0"},
		{"one output", 1, "ParDo"},
		{"two outputs", 2, "ParDo2"},
		{"seven outputs", 7, "ParDo7"},
		{"eight outputs", 8, "ParDoN"},
		{"more than 7 outputs", 10, "ParDoN"},
	}

	for _, tt := range tests {
		testName := tt.name
		t.Run(testName, func(t *testing.T) {
			got := parDoForSize(tt.outputDim)
			if got != tt.want {
				t.Errorf("RecommendParDo(%v) = %v, want %v", tt.outputDim, got, tt.want)
			}
		})
	}
}

// testFunction is used in TestFormatParDoError test to validate that the
// error message returned by the formatParDoError function is correct. This
// function specifically is named and not anonymous to ensure that returned
// error message contains the correct function name which was violating the
// output dimension alignment.
func testFunction() int64 {
	return 42
}

func TestFormatParDoError(t *testing.T) {
	got := formatParDoError(testFunction, 2, 1)
	want := "has 2 outputs, but ParDo requires 1 outputs, use ParDo2 instead."
	if !strings.Contains(got, want) {
		t.Errorf("formatParDoError(testFunction,2,1) = \n%q want =\n%q", got, want)
	}
}

func TestAnnotations(t *testing.T) {
	m := make(map[string][]byte)
	m["privacy_property"] = []byte("differential_privacy")
	doFn := &AnnotationsFn{Annotations: m}

	p := NewPipeline()
	s := p.Root()

	values := [2]int{0, 1}
	col := CreateList(s, values)
	ParDo(s, doFn, col)

	ctx := context.Background()
	envUrn := jobopts.GetEnvironmentUrn(ctx)
	getEnvCfg := jobopts.GetEnvironmentConfig

	environment, err := graphx.CreateEnvironment(ctx, envUrn, getEnvCfg)
	if err != nil {
		t.Fatalf("Couldn't create environment build: %v", err)
	}

	edges, _, err := p.Build()
	if err != nil {
		t.Fatalf("Pipeline couldn't build: %v", err)
	}
	pb, err := graphx.Marshal(edges, &graphx.Options{Environment: environment})
	if err != nil {
		t.Fatalf("Couldn't graphx.Marshal edges: %v", err)
	}
	components := pb.GetComponents()
	transforms := components.GetTransforms()

	foundAnnotationsFn := false
	for _, transform := range transforms {
		if strings.Contains(transform.GetUniqueName(), "AnnotationsFn") {
			foundAnnotationsFn = true
			annotations := transform.GetAnnotations()
			for name, annotation := range annotations {
				if strings.Compare(name, "privacy_property") != 0 {
					t.Errorf("Annotation name: got %v, want %v", name, "privacy_property")
				}
				if got, want := annotation, []byte("differential_privacy"); !bytes.Equal(got, want) {
					t.Errorf("Annotation value: got %v, want %v", got, want)
				}
			}
		}
	}
	if !foundAnnotationsFn {
		t.Errorf("Couldn't find AnnotationsFn in graph %v", transforms)
	}
}

// AnnotationsFn is a dummy DoFn with an annotation.
type AnnotationsFn struct {
	Annotations map[string][]byte
}

func (fn *AnnotationsFn) ProcessElement(v int) int {
	return v
}

func doNothing(_ []byte, _ int) {}
func TestParDoSideInputValidation(t *testing.T) {
	var tests = []struct {
		name      string
		wFn       *window.Fn
		isBounded bool
	}{
		// TODO(https://github.com/apache/beam/issues/21596): Re-enable this test case once proper streaming testing support is finished.
		// {
		// 	"global window unbounded",
		// 	window.NewGlobalWindows(),
		// 	false,
		// },
		{
			"side input session windowed",
			window.NewSessions(1 * time.Minute),
			true,
		},
		{
			"global main, interval side",
			window.NewFixedWindows(10 * time.Second),
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewPipeline()
			s := p.Root()

			strat := &window.WindowingStrategy{Fn: test.wFn, Trigger: trigger.Default(), AccumulationMode: window.Discarding, AllowedLateness: 0}
			sideCol := PCollection{n: graph.New().NewNode(typex.New(reflectx.Int), strat, test.isBounded)}
			outCol, err := TryParDo(s, doNothing, Impulse(s), SideInput{Input: sideCol})
			if outCol != nil {
				t.Errorf("TryParDo() produced an output PCollection when it should have failed, got %v", outCol)
			}
			if err == nil {
				t.Errorf("TryParDo() did not return an error when it should have")
			}
		})
	}
}
