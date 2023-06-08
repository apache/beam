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

// Package logging performs structured output of log entries.
package logging

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger outputs structured logs.
type Logger zap.Logger

// Field represents a structured log entry field.
type Field zap.Field

// MustLogger instantiates a Logger, assigning the name; panics on error.
func MustLogger(_ context.Context, name string) *Logger {
	logger, err := zap.NewProduction(zap.WithCaller(false), zap.AddStacktrace(zapcore.ErrorLevel))
	if err != nil {
		panic(err)
	}
	logger = logger.Named(name)
	return (*Logger)(logger)
}

// Any returns a key=value Field choosing the best way to represent the value.
func Any(key string, value interface{}) Field {
	return (Field)(zap.Any(key, value))
}

// String returns a key=value string Field.
func String(key, value string) Field {
	return (Field)(zap.String(key, value))
}

// Strings returns key=value1,value2,... string Field.
func Strings(key string, value []string) Field {
	return (Field)(zap.Strings(key, value))
}

// Uint64 returns a key=value uint64 Field.
func Uint64(key string, value uint64) Field {
	return (Field)(zap.Uint64(key, value))
}

// Int64 returns a key=value int64 Field.
func Int64(key string, value int64) Field {
	return (Field)(zap.Int64(key, value))
}

func zapFields(field []Field) []zap.Field {
	var result []zap.Field
	for _, k := range field {
		result = append(result, (zap.Field)(k))
	}
	return result
}

// Debug emits entries with a debug severity log level.
func (logger *Logger) Debug(_ context.Context, message string, fields ...Field) {
	zl := (*zap.Logger)(logger)
	zl.Debug(message, zapFields(fields)...)
}

// Error emits entries with a error severity log level.
func (logger *Logger) Error(_ context.Context, message string, fields ...Field) {
	zl := (*zap.Logger)(logger)
	zl.Error(message, zapFields(fields)...)
}

// Info emits entries with a info severity log level.
func (logger *Logger) Info(_ context.Context, message string, fields ...Field) {
	zl := (*zap.Logger)(logger)
	zl.Info(message, zapFields(fields)...)
}

// Fatal emits entries with a fatal severity log level.
func (logger *Logger) Fatal(_ context.Context, message string, fields ...Field) {
	zl := (*zap.Logger)(logger)
	zl.Fatal(message, zapFields(fields)...)
}
