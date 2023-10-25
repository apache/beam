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
	"log/slog"
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
		fields  []slog.Attr
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
				fields: []slog.Attr{
					{
						Key:   "string",
						Value: slog.StringValue("a string"),
					},
					{
						Key:   "int",
						Value: slog.IntValue(1),
					},
					{
						Key:   "bool",
						Value: slog.BoolValue(true),
					},
					{
						Key:   "float",
						Value: slog.Float64Value(1.23456789),
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			l := logging.New(&logging.Options{
				Name:   tt.name,
				Writer: &buf,
			})
			l.LogAttrs(context.Background(), slog.LevelInfo, tt.args.message, tt.args.fields...)
			_, file, line, _ := runtime.Caller(0)
			tt.want.SourceLocation = &loggingpb.LogEntrySourceLocation{
				File: file,
				Line: int64(line) - 1,
			}
			var got gcplogging.Entry
			if err := json.NewDecoder(&buf).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got, opts...); diff != "" {
				t.Errorf("LogAttrs(Info) yielded unexpected difference in log entry (-want, +got):\n%s", diff)
			}
		})
	}
}
func Test_logger_Error(t *testing.T) {
	buf := bytes.Buffer{}
	l := logging.New(&logging.Options{
		Name:   "test logger error",
		Writer: &buf,
	})
	message := "some error"
	fields := []slog.Attr{
		{
			Key:   "observed",
			Value: slog.TimeValue(time.Unix(1000000000, 0)),
		},
	}
	l.LogAttrs(context.Background(), slog.LevelError, message, fields...)
	_, file, line, _ := runtime.Caller(0)
	var got gcplogging.Entry
	if err := json.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(gcplogging.Entry{
		LogName:  "test logger error",
		Severity: gcplogging.Error,
		Payload:  map[string]any{"message": "some error", "observed": "2001-09-09T01:46:40Z"},
		SourceLocation: &loggingpb.LogEntrySourceLocation{
			File: file,
			Line: int64(line) - 1,
		},
	}, got, opts...); diff != "" {
		t.Errorf("LogAttrs(Error) yielded unexpected difference in log entry (-want, +got):\n%s", diff)
	}
}
