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

package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type generalMetadata struct {
	urn            string
	characteristic reflect.Type
}

func (m generalMetadata) ConfigURN() string {
	return m.urn
}

func (m generalMetadata) ConfigCharacteristic() reflect.Type {
	return m.characteristic
}

func TestHandlerRegistry(t *testing.T) {
	type testCombine struct {
		Lift bool
	}
	combineMetadata := generalMetadata{"combine", reflect.TypeOf(testCombine{})}
	type testIterables struct {
		StateBackedEnabled  bool
		StateBackedPageSize int64
	}
	iterableMetadata := generalMetadata{"iterable", reflect.TypeOf(testIterables{})}
	type testSdf struct {
		Enabled   bool
		BatchSize int64
	}
	sdfMetadata := generalMetadata{"sdf", reflect.TypeOf(testSdf{})}

	type spotCheck struct {
		v, h string
		want any
	}
	tests := []struct {
		name     string
		handlers []HandlerMetadata
		config   string

		wantVariants, wantHandlers []string
		wantSpots                  []spotCheck
	}{
		{
			name:     "basics",
			handlers: []HandlerMetadata{combineMetadata, iterableMetadata, sdfMetadata},
			config: `
flink:
  combine:
    lift: false
dataflow:
  combine:
    lift: true
  sdf:
    enabled: true
    batchsize: 5
`,
			wantVariants: []string{"dataflow", "flink"},
			wantHandlers: []string{"combine", "sdf"},
			wantSpots: []spotCheck{
				{v: "dataflow", h: "combine", want: testCombine{Lift: true}},
				{v: "flink", h: "combine", want: testCombine{Lift: false}},
				{v: "dataflow", h: "sdf", want: testSdf{Enabled: true, BatchSize: 5}},
				{v: "flink", h: "sdf", want: testSdf{Enabled: false, BatchSize: 0}}, // Unset means 0 value configs.
				{v: "unknown", h: "missing", want: nil},
				{v: "dataflow", h: "missing", want: nil},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			reg.RegisterHandlers(test.handlers...)

			if err := reg.LoadFromYaml([]byte(test.config)); err != nil {
				t.Fatalf("error unmarshalling test config: %v", err)
			}

			if d := cmp.Diff(test.wantVariants, reg.Variants()); d != "" {
				t.Errorf("mismatch in variants (-want, +got):\n%v", d)
			}
			if d := cmp.Diff(test.wantHandlers, reg.UsedHandlers()); d != "" {
				t.Errorf("mismatch in used handlers (-want, +got):\n%v", d)
			}
			for _, spot := range test.wantSpots {
				got := reg.GetVariant(spot.v).GetCharacteristics(spot.h)
				if d := cmp.Diff(spot.want, got); d != "" {
					t.Errorf("mismatch in spot check for (%v, %v) (-want, +got):\n%v", spot.v, spot.h, d)
				}
			}
		})
	}

	t.Run("trying to read a config with an unregistered handler should fail", func(t *testing.T) {
		reg := NewHandlerRegistry()
		reg.RegisterHandlers(combineMetadata)

		config := `
dataflow:
  sdf:
    enabled: true
    batchsize: 5
  combine:
    lift: true`

		err := reg.LoadFromYaml([]byte(config))
		if err == nil {
			t.Fatal("loaded config, got nil; want error")
		}
		if !strings.Contains(err.Error(), "sdf") {
			t.Fatalf("error should contain \"sdf\", but was: %v", err)
		}
	})

	t.Run("duplicate variants", func(t *testing.T) {
		reg := NewHandlerRegistry()
		reg.RegisterHandlers(combineMetadata)

		config := `
dataflow:
  combine:
    lift: true
dataflow:
  combine:
    lift: false
`
		err := reg.LoadFromYaml([]byte(config))
		if err == nil {
			t.Fatal("loaded config, got nil; want error")
		}
	})

	t.Run("duplicate handlers", func(t *testing.T) {
		reg := NewHandlerRegistry()
		reg.RegisterHandlers(combineMetadata)

		config := `
dataflow:
  combine:
    lift: true
  combine:
    lift: false
`
		err := reg.LoadFromYaml([]byte(config))
		if err == nil {
			t.Fatal("loaded config, got nil; want error")
		}
	})

	t.Run("invalid handler config:fieldtype", func(t *testing.T) {
		reg := NewHandlerRegistry()
		reg.RegisterHandlers(combineMetadata)

		config := `
dataflow:
  combine:
    lift: d
`
		err := reg.LoadFromYaml([]byte(config))
		if err == nil {
			t.Fatal("loaded config, got nil; want error")
		}
	})
	t.Run("invalid handler config:extra field", func(t *testing.T) {
		reg := NewHandlerRegistry()
		reg.RegisterHandlers(combineMetadata)

		config := `
dataflow:
  combine:
    lift: no
    lower: foo
`
		err := reg.LoadFromYaml([]byte(config))
		if err == nil {
			t.Fatal("loaded config, got nil; want error")
		}
	})

	t.Run("no variant", func(t *testing.T) {
		reg := NewHandlerRegistry()
		reg.RegisterHandlers(combineMetadata)

		config := `
dataflow:
  combine:
    lift: true
`
		err := reg.LoadFromYaml([]byte(config))
		if err != nil {
			t.Fatalf("error loading config: %v", err)
		}
		if got, want := reg.GetVariant("notpresent"), (*Variant)(nil); got != want {
			t.Errorf("GetVariant('notpresent') = %v, want %v", got, want)
		}
	})
}
