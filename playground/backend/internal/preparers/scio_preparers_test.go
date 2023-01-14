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

<<<<<<<< HEAD:sdks/go/pkg/beam/util/errorx/guarded_test.go
package errorx
========
package preparers
>>>>>>>> master:playground/backend/internal/preparers/scio_preparers_test.go

import (
	"errors"
	"testing"
)

<<<<<<<< HEAD:sdks/go/pkg/beam/util/errorx/guarded_test.go
func TestTrySetError(t *testing.T) {
	var gu GuardedError
	setErr := errors.New("attempted error")
	success := gu.TrySetError(setErr)
	if !success {
		t.Fatal("got false when trying to set error, want true")
	}
	if got, want := gu.Error(), setErr; got != want {
		t.Errorf("got error %v when checking message, want %v", got, want)
	}
}

func TestTrySetError_bad(t *testing.T) {
	setErr := errors.New("old error")
	gu := &GuardedError{err: setErr}
	success := gu.TrySetError(setErr)
	if success {
		t.Fatal("got true when trying to set error, want false")
	}
	if got, want := gu.Error(), setErr; got != want {
		t.Errorf("got error %v when checking message, want %v", got, want)
========
func TestGetScioPreparers(t *testing.T) {
	type args struct {
		filePath      string
		prepareParams map[string]string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			// Test case with calling GetScioPreparers method.
			// As a result, want to receive slice of preparers with len = 3
			name: "Get scio preparers",
			args: args{"MOCK_FILEPATH", make(map[string]string)},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewPreparersBuilder(tt.args.filePath, tt.args.prepareParams)
			GetScioPreparers(builder)
			if got := builder.Build().GetPreparers(); len(*got) != tt.want {
				t.Errorf("GetScioPreparers() returns %v Preparers, want %v", len(*got), tt.want)
			}
		})
>>>>>>>> master:playground/backend/internal/preparers/scio_preparers_test.go
	}
}
