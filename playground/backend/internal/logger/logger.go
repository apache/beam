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

package logger

import (
	"cloud.google.com/go/logging"
	"context"
	"log"
)

type Severity string

const (
	INFO      Severity = "[INFO]:"
	WARN      Severity = "[WARN]:"
	ERROR     Severity = "[ERROR]:"
	FATAL     Severity = "[FATAL]:"
	DEBUG     Severity = "[DEBUG]:"
	appEngine          = "app_engine"
)

var handlers []Handler

// SetupLogger constructs logger by application environment
// Add handlers in root logger:
//
//	CloudLoggingHandler - if server running on App Engine
//	StdHandler - if server running locally
func SetupLogger(ctx context.Context, launchSite, googleProjectId string) {
	switch launchSite {
	case appEngine:
		client, err := logging.NewClient(ctx, googleProjectId)
		if err != nil {
			log.Fatalf("Failed to create client: %v", err)
		}
		cloudLogger := NewCloudLoggingHandler(client)
		AddHandler(cloudLogger)
	default:
		stdLogger := NewStdHandler()
		AddHandler(stdLogger)
	}
}

// SetHandlers set a new array of logger handlers
func SetHandlers(h []Handler) {
	handlers = h
}

// AddHandler adds a new handler to the array
func AddHandler(h Handler) {
	handlers = append(handlers, h)
}

func Info(args ...interface{}) {
	for _, handler := range handlers {
		handler.Info(args...)
	}
}

func Infof(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Infof(format, args...)
	}
}

func Warn(args ...interface{}) {
	for _, handler := range handlers {
		handler.Warn(args...)
	}
}

func Warnf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Warnf(format, args...)
	}
}

func Error(args ...interface{}) {
	for _, handler := range handlers {
		handler.Error(args...)
	}
}

func Errorf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Errorf(format, args...)
	}
}

func Debug(args ...interface{}) {
	for _, handler := range handlers {
		handler.Debug(args...)
	}
}

func Debugf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Debugf(format, args...)
	}
}

func Fatal(args ...interface{}) {
	for _, handler := range handlers {
		handler.Fatal(args...)
	}
}

func Fatalf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Fatalf(format, args...)
	}
}
