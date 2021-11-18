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
	"fmt"
	"log"
)

type Severity string

const (
	INFO  Severity = "[INFO]:"
	WARN  Severity = "[WARN]:"
	ERROR Severity = "[ERROR]:"
	FATAL Severity = "[FATAL]:"
	DEBUG Severity = "[DEBUG]:"
)

var handlers []Handler

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
	logMessage(INFO, args...)
}

func Infof(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Infof(format, args...)
	}
	logMessage(INFO, fmt.Sprintf(format, args...))
}

func Warn(args ...interface{}) {
	for _, handler := range handlers {
		handler.Warn(args...)
	}
	logMessage(WARN, args...)
}

func Warnf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Warnf(format, args...)
	}
	logMessage(WARN, fmt.Sprintf(format, args...))
}

func Error(args ...interface{}) {
	for _, handler := range handlers {
		handler.Error(args...)
	}
	logMessage(ERROR, args...)
}

func Errorf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Errorf(format, args...)
	}
	logMessage(ERROR, fmt.Sprintf(format, args...))
}

func Debug(args ...interface{}) {
	for _, handler := range handlers {
		handler.Debug(args...)
	}
	logMessage(DEBUG, args...)
}

func Debugf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Debugf(format, args...)
	}
	logMessage(DEBUG, fmt.Sprintf(format, args...))
}

func Fatal(args ...interface{}) {
	for _, handler := range handlers {
		handler.Fatal(args...)
	}
	args = append([]interface{}{FATAL}, args...)
	log.Fatalln(args...)
}

func Fatalf(format string, args ...interface{}) {
	for _, handler := range handlers {
		handler.Fatalf(format, args...)
	}
	args = append([]interface{}{FATAL}, fmt.Sprintf(format, args...))
	log.Fatalln(args...)
}

// logMessage logs a message at level severity.
func logMessage(severity Severity, args ...interface{}) {
	args = append([]interface{}{severity}, args...)
	log.Println(args...)
}
