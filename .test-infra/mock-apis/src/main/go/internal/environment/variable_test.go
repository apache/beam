// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package environment

import (
	"errors"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMissing(t *testing.T) {
	type args struct {
		vars   []Variable
		values []string
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{
			name: "{}",
			args: args{},
		},
		{
			name: "{A=}",
			args: args{
				vars: []Variable{
					"A",
				},
				values: []string{
					"",
				},
			},
			want: errors.New("variables empty but expected from environment: A="),
		},
		{
			name: "{A=1}",
			args: args{
				vars: []Variable{
					"A",
				},
				values: []string{
					"1",
				},
			},
			want: nil,
		},
		{
			name: "{A=; B=}",
			args: args{
				vars: []Variable{
					"A",
					"B",
				},
				values: []string{
					"",
					"",
				},
			},
			want: errors.New("variables empty but expected from environment: A=; B="),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got, want string
			clearVars(tt.args.vars...)
			set(t, tt.args.vars, tt.args.values)
			err := Missing(tt.args.vars...)
			if err != nil {
				got = err.Error()
			}
			if tt.want != nil {
				want = tt.want.Error()
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Missing() error returned unexpected difference in error messages (-want +got):\n%s", diff)
			}
		})
	}
}

func TestVariable_Default(t *testing.T) {
	type args struct {
		setValue     string
		defaultValue string
	}
	tests := []struct {
		name string
		v    Variable
		args args
		want string
	}{
		{
			name: "environment variable not set",
			v:    "A",
			args: args{
				defaultValue: "1",
			},
			want: "1",
		},
		{
			name: "environment variable default is overridden by set value",
			v:    "A",
			args: args{
				setValue:     "2",
				defaultValue: "1",
			},
			want: "2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearVars(tt.v)
			if tt.args.setValue != "" {
				set(t, []Variable{tt.v}, []string{tt.args.setValue})
			}
			if err := tt.v.Default(tt.args.defaultValue); err != nil {
				t.Fatalf("could not set default environment variable value during test execution: %v", err)
			}
			got := os.Getenv(tt.v.Key())
			if got != tt.want {
				t.Errorf("Default() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestVariable_KeyValue(t *testing.T) {
	tests := []struct {
		name  string
		v     Variable
		value string
		want  string
	}{
		{
			name: "environment variable not set",
			v:    "A",
			want: "A=",
		},
		{
			name:  "environment variable is set",
			v:     "A",
			value: "1",
			want:  "A=1",
		},
	}
	for _, tt := range tests {
		clearVars(tt.v)
		t.Run(tt.name, func(t *testing.T) {
			set(t, []Variable{tt.v}, []string{tt.value})
			got := tt.v.KeyValue()
			if got != tt.want {
				t.Errorf("KeyValue() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestVariable_Missing(t *testing.T) {
	type args struct {
		setValue     string
		defaultValue string
	}
	tests := []struct {
		name string
		args args
		v    Variable
		want bool
	}{
		{
			name: "no default and not set",
			args: args{},
			v:    "A",
			want: true,
		},
		{
			name: "has default but not set",
			args: args{
				defaultValue: "1",
			},
			v:    "A",
			want: false,
		},
		{
			name: "no default but set",
			args: args{
				setValue: "1",
			},
			v:    "A",
			want: false,
		},
		{
			name: "has default and set",
			args: args{
				setValue:     "2",
				defaultValue: "1",
			},
			v:    "A",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearVars(tt.v)
			if tt.args.defaultValue != "" {
				if err := tt.v.Default(tt.args.defaultValue); err != nil {
					t.Fatalf("could not set default environment variable value during test execution: %v", err)
				}
			}
			if tt.args.setValue != "" {
				set(t, []Variable{tt.v}, []string{tt.args.setValue})
			}
			if got := tt.v.Missing(); got != tt.want {
				t.Errorf("Missing() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVariable_Value(t *testing.T) {
	type args struct {
		setValue     string
		defaultValue string
	}
	tests := []struct {
		name string
		args args
		v    Variable
		want string
	}{
		{
			name: "no default and not set",
			args: args{},
			v:    "A",
			want: "",
		},
		{
			name: "has default but not set",
			args: args{
				defaultValue: "1",
			},
			v:    "A",
			want: "1",
		},
		{
			name: "no default but set",
			args: args{
				setValue: "1",
			},
			v:    "A",
			want: "1",
		},
		{
			name: "has default and set",
			args: args{
				setValue:     "2",
				defaultValue: "1",
			},
			v:    "A",
			want: "2",
		},
	}
	for _, tt := range tests {
		clearVars(tt.v)
		if tt.args.defaultValue != "" {
			if err := tt.v.Default(tt.args.defaultValue); err != nil {
				t.Fatalf("could not set default environment variable value during test execution: %v", err)
			}
		}
		if tt.args.setValue != "" {
			set(t, []Variable{tt.v}, []string{tt.args.setValue})
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.Value(); got != tt.want {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func clearVars(vars ...Variable) {
	for _, k := range vars {
		_ = os.Setenv(k.Key(), "")
	}
}

func set(t *testing.T, vars []Variable, values []string) {
	if len(vars) != len(values) {
		t.Fatalf("test cases should be configured with matching args.vars and args.values: len(tt.args.vars): %v != len(tt.args.values): %v", len(vars), len(values))
	}
	for i := range vars {
		key := vars[i].Key()
		value := values[i]
		_ = os.Setenv(key, value)
	}
}
