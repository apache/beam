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

// StdHandler represents standard 'log' package that logs to stderr
type StdHandler struct {
}

// NewStdHandler creates StdHandler
func NewStdHandler() *StdHandler {
	return &StdHandler{}
}

func (h StdHandler) Info(args ...interface{}) {
	h.logMessage(INFO, args...)
}

func (h StdHandler) Infof(format string, args ...interface{}) {
	h.logMessage(INFO, fmt.Sprintf(format, args...))
}

func (h StdHandler) Warn(args ...interface{}) {
	h.logMessage(WARN, args...)
}

func (h StdHandler) Warnf(format string, args ...interface{}) {
	h.logMessage(WARN, fmt.Sprintf(format, args...))
}

func (h StdHandler) Error(args ...interface{}) {
	h.logMessage(ERROR, args...)
}

func (h StdHandler) Errorf(format string, args ...interface{}) {
	h.logMessage(ERROR, fmt.Sprintf(format, args...))
}

func (h StdHandler) Debug(args ...interface{}) {
	h.logMessage(DEBUG, args...)
}

func (h StdHandler) Debugf(format string, args ...interface{}) {
	h.logMessage(DEBUG, fmt.Sprintf(format, args...))
}

func (h StdHandler) Fatal(args ...interface{}) {
	args = append([]interface{}{FATAL}, args...)
	log.Fatalln(args...)
}

func (h StdHandler) Fatalf(format string, args ...interface{}) {
	args = append([]interface{}{FATAL}, fmt.Sprintf(format, args...))
	log.Fatalln(args...)
}

// logMessage logs a message at level severity.
func (h StdHandler) logMessage(severity Severity, args ...interface{}) {
	args = append([]interface{}{severity}, args...)
	log.Println(args...)
}
