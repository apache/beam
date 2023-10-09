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

package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	gcplogging "cloud.google.com/go/logging"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var (
	opts = []cmp.Option{
		cmpopts.IgnoreFields(loggingpb.LogEntrySourceLocation{}, "state", "sizeCache", "unknownFields"),
		cmpopts.IgnoreFields(gcplogging.Entry{}, "Timestamp"),
	}
)

func Test_logger_Info(t *testing.T) {
	type args struct {
		message string
		fields  []logging.Field
	}
	tests := []struct {
		name string
		args args
		want gcplogging.Entry
	}{
		{
			name: "message only",
			args: args{
				message: "hello log",
			},
			want: gcplogging.Entry{
				LogName:  "message only",
				Severity: gcplogging.Info,
				Payload: map[string]interface{}{
					"message": "hello log",
				},
			},
		},
		{
			name: "with flat fields",
			args: args{
				message: "message with fields",
				fields: []logging.Field{
					{
						Key:   "string",
						Value: "a string",
					},
					{
						Key:   "int",
						Value: 1,
					},
					{
						Key:   "bool",
						Value: true,
					},
					{
						Key:   "float",
						Value: 1.23456789,
					},
				},
			},
			want: gcplogging.Entry{
				LogName:  "with flat fields",
				Severity: gcplogging.Info,
				Payload: map[string]interface{}{
					"message": "message with fields",
					"string":  "a string",
					"int":     float64(1),
					"bool":    true,
					"float":   1.23456789,
				},
			},
		},
		{
			name: "with struct",
			args: args{
				message: "with struct",
				fields: []logging.Field{
					{
						Key: "a struct",
						Value: logging.Field{
							Key: "a",
							Value: logging.Field{
								Key:   "b",
								Value: "c",
							},
						},
					},
				},
			},
			want: gcplogging.Entry{
				LogName:  "with struct",
				Severity: gcplogging.Info,
				Payload: map[string]interface{}{
					"message": "with struct",
					"a struct": map[string]any{
						"Key": "a",
						"Value": map[string]any{
							"Key":   "b",
							"Value": "c",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			l := logging.New(tt.name, logging.WithWriter(&buf))
			l.Info(context.Background(), tt.args.message, tt.args.fields...)
			_, file, line, _ := runtime.Caller(0)
			tt.want.SourceLocation = &loggingpb.LogEntrySourceLocation{
				File: file,
				Line: int64(line) - 1,
			}
			var got gcplogging.Entry
			if err := json.NewDecoder(&buf).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tt.want, opts...); diff != "" {
				t.Errorf("Info(%s, %v) diff\n%s", tt.args.message, tt.args.fields, diff)
			}
		})
	}
}
func Test_logger_Error(t *testing.T) {
	buf := bytes.Buffer{}
	l := logging.New("test logger error", logging.WithWriter(&buf))
	message := "some error"
	fields := []logging.Field{
		{
			Key:   "observed",
			Value: time.Unix(1000000000, 0),
		},
	}
	l.Error(context.Background(), errors.New(message), fields...)
	_, file, line, _ := runtime.Caller(0)
	var got gcplogging.Entry
	if err := json.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(got, gcplogging.Entry{
		LogName:  "test logger error",
		Severity: gcplogging.Error,
		Payload:  map[string]any{"message": "some error", "observed": "2001-09-08T18:46:40-07:00"},
		SourceLocation: &loggingpb.LogEntrySourceLocation{
			File: file,
			Line: int64(line) - 1,
		},
	}, opts...); diff != "" {
		t.Errorf("Info(%s, %v) diff\n%s", message, fields, diff)
	}
}
