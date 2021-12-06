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

// Contains tests cases for all custom error. Each method contains 3 test cases:
// - call the method with correct count of arguments.
// - call the method with too few count of arguments.
// - call the method with too many count of arguments.
// For all cases want to receive an error with expected error message.
package errors

import (
	"strings"
	"testing"
)

func TestInternalError(t *testing.T) {
	type args struct {
		title         string
		formatMessage string
		arg           []interface{}
	}
	tests := []struct {
		name     string
		args     args
		expected string
		wantErr  bool
	}{
		{
			name:     "correct count of args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{"TEST_ARG"}},
			expected: "rpc error: code = Internal desc = TEST_TITLE: TEST_FORMAT_MESSAGE TEST_ARG",
			wantErr:  true,
		},
		{
			name:     "too many args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{"TEST_ARG", "TEST_ARG"}},
			expected: "rpc error: code = Internal desc = TEST_TITLE: TEST_FORMAT_MESSAGE TEST_ARG%!(EXTRA string=TEST_ARG)",
			wantErr:  true,
		},
		{
			name:     "too few args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{}},
			expected: "rpc error: code = Internal desc = TEST_TITLE: TEST_FORMAT_MESSAGE %!s(MISSING)",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := InternalError(tt.args.title, tt.args.formatMessage, tt.args.arg...)
			if (err != nil) != tt.wantErr {
				t.Errorf("InternalError() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !strings.EqualFold(err.Error(), tt.expected) {
				t.Errorf("InternalError() error = %v, wantErr %v", err.Error(), tt.expected)
			}
		})
	}
}

func TestInvalidArgumentError(t *testing.T) {
	type args struct {
		title         string
		formatMessage string
		arg           []interface{}
	}
	tests := []struct {
		name     string
		args     args
		expected string
		wantErr  bool
	}{
		{
			name:     "correct count of args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{"TEST_ARG"}},
			expected: "rpc error: code = InvalidArgument desc = TEST_TITLE: TEST_FORMAT_MESSAGE TEST_ARG",
			wantErr:  true,
		},
		{
			name:     "too many args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{"TEST_ARG", "TEST_ARG"}},
			expected: "rpc error: code = InvalidArgument desc = TEST_TITLE: TEST_FORMAT_MESSAGE TEST_ARG%!(EXTRA string=TEST_ARG)",
			wantErr:  true,
		},
		{
			name:     "too few args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{}},
			expected: "rpc error: code = InvalidArgument desc = TEST_TITLE: TEST_FORMAT_MESSAGE %!s(MISSING)",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := InvalidArgumentError(tt.args.title, tt.args.formatMessage, tt.args.arg...)
			if (err != nil) != tt.wantErr {
				t.Errorf("InvalidArgumentError() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !strings.EqualFold(err.Error(), tt.expected) {
				t.Errorf("InvalidArgumentError() error = %v, wantErr %v", err.Error(), tt.expected)
			}
		})
	}
}

func TestNotFoundError(t *testing.T) {
	type args struct {
		title         string
		formatMessage string
		arg           []interface{}
	}
	tests := []struct {
		name     string
		args     args
		expected string
		wantErr  bool
	}{
		{
			name:     "correct count of args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{"TEST_ARG"}},
			expected: "rpc error: code = NotFound desc = TEST_TITLE: TEST_FORMAT_MESSAGE TEST_ARG",
			wantErr:  true,
		},
		{
			name:     "too many args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{"TEST_ARG", "TEST_ARG"}},
			expected: "rpc error: code = NotFound desc = TEST_TITLE: TEST_FORMAT_MESSAGE TEST_ARG%!(EXTRA string=TEST_ARG)",
			wantErr:  true,
		},
		{
			name:     "too few args",
			args:     args{title: "TEST_TITLE", formatMessage: "TEST_FORMAT_MESSAGE %s", arg: []interface{}{}},
			expected: "rpc error: code = NotFound desc = TEST_TITLE: TEST_FORMAT_MESSAGE %!s(MISSING)",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NotFoundError(tt.args.title, tt.args.formatMessage, tt.args.arg...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NotFoundError() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !strings.EqualFold(err.Error(), tt.expected) {
				t.Errorf("NotFoundError() error = %v, wantErr %v", err.Error(), tt.expected)
			}
		})
	}
}
