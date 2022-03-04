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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
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
	want := "beam.testFunction has 2 outputs, but ParDo requires 1 outputs, use ParDo2 instead."
	if !strings.Contains(got, want) {
		t.Errorf("formatParDoError(testFunction,2,1) = %v, want = %v", got, want)
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
