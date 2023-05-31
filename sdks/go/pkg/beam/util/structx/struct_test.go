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

package structx

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestInferFieldNames(t *testing.T) {
	type embedded struct {
		Embedded1 string `key:"embedded1"`
		Embedded2 string
	}

	tests := []struct {
		name string
		t    reflect.Type
		key  string
		want []string
	}{
		{
			name: "Return slice of field names from tagged struct fields",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `key:"field2"`
			}{}),
			key:  "key",
			want: []string{"field1", "field2"},
		},
		{
			name: "Return nil slice from struct with no fields",
			t:    reflect.TypeOf(struct{}{}),
			key:  "key",
			want: nil,
		},
		{
			name: "Use struct field name for field without tag",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string
			}{}),
			key:  "key",
			want: []string{"field1", "Field2"},
		},
		{
			name: "Use struct field name for field with empty tag",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `key:""`
			}{}),
			key:  "key",
			want: []string{"field1", "Field2"},
		},
		{
			name: "Use struct field name for field with other tag key",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `other:"field2"`
			}{}),
			key:  "key",
			want: []string{"field1", "Field2"},
		},
		{
			name: "Omit field with '-' tag",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `key:"-"`
			}{}),
			key:  "key",
			want: []string{"field1"},
		},
		{
			name: "Omit unexported field",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				field2 string
			}{}),
			key:  "key",
			want: []string{"field1"},
		},
		{
			name: "Omit unexported field with tag",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				field2 string `key:"field2"`
			}{}),
			key:  "key",
			want: []string{"field1"},
		},
		{
			name: "Extract the first comma separated value",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1,omitempty"`
				Field2 string `key:",omitempty"`
			}{}),
			key:  "key",
			want: []string{"field1", "Field2"},
		},
		{
			name: "Include fields from embedded struct",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				embedded
			}{}),
			key:  "key",
			want: []string{"field1", "embedded1", "Embedded2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InferFieldNames(tt.t, tt.key); !cmp.Equal(got, tt.want) {
				t.Errorf("InferFieldNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInferFieldNamesPanic(t *testing.T) {
	t.Run("Panic for non-struct type", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("InferFieldNames() does not panic")
			}
		}()

		InferFieldNames(reflect.TypeOf(""), "key")
	})
}

func TestFieldIndexByTag(t *testing.T) {
	tests := []struct {
		name  string
		t     reflect.Type
		key   string
		value string
		want  int
	}{
		{
			name: "Return index of field with matching tag key and value",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `key:"field2"`
			}{}),
			key:   "key",
			value: "field2",
			want:  1,
		},
		{
			name: "Return -1 for non-existent tag key",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `key:"field2"`
			}{}),
			key:   "other",
			value: "field1",
			want:  -1,
		},
		{
			name: "Return -1 for non-existent tag value",
			t: reflect.TypeOf(struct {
				Field1 string `key:"field1"`
				Field2 string `key:"field2"`
			}{}),
			key:   "key",
			value: "field3",
			want:  -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FieldIndexByTag(tt.t, tt.key, tt.value); got != tt.want {
				t.Errorf("FieldIndexByTag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFieldIndexByTagPanic(t *testing.T) {
	t.Run("Panic for non-struct type", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("FieldIndexByTag() does not panic")
			}
		}()

		FieldIndexByTag(reflect.TypeOf(""), "key", "field1")
	})
}
