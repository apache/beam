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

package mongodbio

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
)

func Test_inferProjection(t *testing.T) {
	type doc struct {
		Field1 string `bson:"field1"`
		Field2 string `bson:"field2"`
		Field3 string `bson:"-"`
	}

	tests := []struct {
		name   string
		t      reflect.Type
		tagKey string
		want   bson.D
	}{
		{
			name:   "Infer projection from struct bson tags",
			t:      reflect.TypeOf(doc{}),
			tagKey: "bson",
			want: bson.D{
				{Key: "field1", Value: 1},
				{Key: "field2", Value: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inferProjection(tt.t, tt.tagKey); !cmp.Equal(got, tt.want) {
				t.Errorf("inferProjection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inferProjectionPanic(t *testing.T) {
	type doc struct{}

	t.Run("Panic when type has no fields to infer", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("inferProjection() does not panic")
			}
		}()

		inferProjection(reflect.TypeOf(doc{}), "bson")
	})
}
