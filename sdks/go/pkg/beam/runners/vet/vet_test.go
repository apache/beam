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

package vet

import (
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/vet/testpipeline"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name                string
		c                   func(beam.Scope)
		perf, exp, ref, reg bool
	}{
		{name: "Performant", c: testpipeline.Performant, perf: true},
		{name: "FunctionReg", c: testpipeline.FunctionReg, exp: true, ref: true, reg: true},
		{name: "ShimNeeded", c: testpipeline.ShimNeeded, ref: true},
		{name: "TypeReg", c: testpipeline.TypeReg, ref: true, reg: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.c(s)
			e, err := Evaluate(context.Background(), p)
			if err != nil {
				t.Fatalf("failed to evaluate testpipeline.Pipeline: %v", err)
			}
			if e.Performant() != test.perf {
				t.Fatalf("e.Performant() = %v, want %v", e.Performant(), test.perf)
			}
			// Abort early for performant pipelines.
			if test.perf {
				return
			}
			e.summary()
			if e.AllExported() != test.exp {
				t.Errorf("e.AllExported() = %v, want %v", e.AllExported(), test.exp)
			}
			if e.RequiresRegistrations() != test.reg {
				t.Errorf("e.RequiresRegistrations() = %v, want %v\n%v", e.RequiresRegistrations(), test.reg, string(e.d.Bytes()))
			}
			if e.UsesDefaultReflectionShims() != test.ref {
				t.Errorf("e.UsesDefaultReflectionShims() = %v, want %v", e.UsesDefaultReflectionShims(), test.ref)
			}
		})
	}
}
