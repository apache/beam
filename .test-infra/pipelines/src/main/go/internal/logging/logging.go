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
	"fmt"
	"os"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/environment"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DebugLevel Level = iota - 1

	// InfoLevel is the default logging level.
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

var (
	LevelVariable              environment.Variable = "LOG_LEVEL"
	AllowedLevelVariableValues                      = map[string]Level{
		"debug": DebugLevel,
		"info":  InfoLevel,
		"warn":  WarnLevel,
		"error": ErrorLevel,
		"fatal": FatalLevel,
	}
)

func init() {
	_ = LevelVariable.Default("info")
}

type Level int8

// Logger outputs structured logs.
type Logger struct {
	inner *zap.Logger
	atom  zap.AtomicLevel
}

// Field represents a structured log entry field.
type Field zap.Field

// New instantiates a Logger, assigning the name where its
// Level is derived from the LevelVariable; context is currently a placeholder
// and not used. Panics if the level is missing or is not one of
// AllowedLevelVariableValues.
func New(ctx context.Context, name string, level environment.Variable) *Logger {
	if level.Missing() {
		panic(fmt.Errorf("environment variable: %s is empty but required", level.Key()))
	}
	v, ok := AllowedLevelVariableValues[level.Value()]
	if !ok {
		panic(fmt.Errorf("environment variable: %s with value %s is not allowed", level.Key(), level.Value()))
	}
	return newWithLevel(ctx, name, v)
}

func newWithLevel(_ context.Context, name string, level Level) *Logger {
	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(os.Stdout),
		atom,
	))
	logger = logger.Named(name)
	zl := (zapcore.Level)(level)
	atom.SetLevel(zl)
	return &Logger{
		atom:  atom,
		inner: logger,
	}
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

func zapFields(field []Field) []zap.Field {
	var result []zap.Field
	for _, k := range field {
		result = append(result, (zap.Field)(k))
	}
	return result
}

// Debug emits entries with a debug severity log level.
func (logger *Logger) Debug(_ context.Context, message string, fields ...Field) {
	logger.inner.Debug(message, zapFields(fields)...)
}

// Error emits entries with a error severity log level.
func (logger *Logger) Error(_ context.Context, err error, fields ...Field) {
	logger.inner.Error(err.Error(), zapFields(fields)...)
}

// Info emits entries with a info severity log level.
func (logger *Logger) Info(_ context.Context, message string, fields ...Field) {
	logger.inner.Info(message, zapFields(fields)...)
}

// Fatal emits entries with a fatal severity log level.
func (logger *Logger) Fatal(_ context.Context, err error, fields ...Field) {
	logger.inner.Fatal(err.Error(), zapFields(fields)...)
}

func (logger *Logger) WithLevel(level Level) *Logger {
	zl := (zapcore.Level)(level)
	logger.atom.SetLevel(zl)
	return logger
}
