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

package passert

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func isA(input string) bool        { return input == "a" }
func isB(input string) bool        { return input == "b" }
func lessThan13(input int) bool    { return input < 13 }
func greaterThan13(input int) bool { return input > 13 }

func init() {
	register.Function1x1(isA)
	register.Function1x1(isB)
	register.Function1x1(lessThan13)
	register.Function1x1(greaterThan13)
}

func TestTrue_string(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a", "a", "a")
	True(s, col, isA)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestTrue_numeric(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 3, 3, 6)
	True(s, col, lessThan13)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestTrue_bad(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a", "a", "b")
	True(s, col, isA)
	err := ptest.Run(p)
	if err == nil {
		t.Fatalf("Pipeline succeeded when it should haved failed, got %v", err)
	}
	if !strings.Contains(err.Error(), "predicate(b) = false, want true") {
		t.Errorf("Pipeline failed but did not produce the expected error, got %v", err)
	}
}

func TestFalse_string(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a", "a", "a")
	False(s, col, isB)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestFalse_numeric(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, 3, 3, 6)
	False(s, col, greaterThan13)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestFalse_bad(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a", "a", "b")
	False(s, col, isB)
	err := ptest.Run(p)
	if err == nil {
		t.Fatalf("Pipeline succeeded when it should haved failed, got %v", err)
	}
	if !strings.Contains(err.Error(), "predicate(b) = true, want false") {
		t.Errorf("Pipeline failed but did not produce the expected error, got %v", err)
	}
}

func TestEmpty_good(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.CreateList(s, []string{})
	Empty(s, col)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestEmpty_bad(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a")
	Empty(s, col)
	err := ptest.Run(p)
	if err == nil {
		t.Fatalf("Pipeline succeeded when it should haved failed, got %v", err)
	}
	if !strings.Contains(err.Error(), "PCollection contains a, want empty collection") {
		t.Errorf("Pipeline failed but did not produce the expected error, got %v", err)
	}
}

func TestNonEmpty(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.CreateList(s, []string{"a", "b", "c"})
	NonEmpty(s, col)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestNonEmpty_Bad(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.CreateList(s, []string{})
	NonEmpty(s, col)
	if err := ptest.Run(p); err == nil {
		t.Error("Pipeline succeeded when it should have failed.")
	}
}
