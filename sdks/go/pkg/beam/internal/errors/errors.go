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

// Package errors contains functionality for creating and wrapping errors with improved formatting
// compared to the standard Go error functionality.
package errors

import (
	"fmt"
	"io"
	"strings"
)

// New returns an error with the given message.
func New(message string) error {
	return fmt.Errorf("%s", message)
}

// Errorf returns an error with a message formatted according to the format
// specifier.
func Errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

// Wrap returns a new error annotating err with a new message.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return &beamError{
		cause: err,
		msg:   message,
		top:   getTop(err),
	}
}

// Wrapf returns a new error annotating err with a new message according to
// the format specifier.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &beamError{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
		top:   getTop(err),
	}
}

// WithContext returns a new error adding additional context to err.
func WithContext(err error, context string) error {
	if err == nil {
		return nil
	}
	return &beamError{
		cause:   err,
		context: context,
		top:     getTop(err),
	}
}

// WithContextf returns a new error adding additional context to err according
// to the format specifier.
func WithContextf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &beamError{
		cause:   err,
		context: fmt.Sprintf(format, args...),
		top:     getTop(err),
	}
}

// SetTopLevelMsg returns a new error with the given top level message. The top
// level message is the first error message that gets printed when Error()
// is called on the returned error or any error wrapping it.
func SetTopLevelMsg(err error, top string) error {
	if err == nil {
		return nil
	}
	return &beamError{
		cause: err,
		top:   top,
	}
}

// SetTopLevelMsgf returns a new error with the given top level message
// according to the format specifier. The top level message is the first error
// message that gets printed when Error() is called on the returned error or
// any error wrapping it.
func SetTopLevelMsgf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &beamError{
		cause: err,
		top:   fmt.Sprintf(format, args...),
	}
}

func getTop(e error) string {
	if be, ok := e.(*beamError); ok {
		return be.top
	}
	return ""
}

// beamError represents one or more details about an error. They are usually
// nested in the order that additional context was wrapped around the original
// error.
//
// The presence or lack of certain fields implicitly indicates some details
// about the error.
//
// * If no cause is present it indicates that this instance is the original
//   error, and the message is assumed to be present.
// * If both message and context are present, the context describes this error,
//   not the cause of this error.
// * top is always propogated up from the cause. If it's empty that means that
//   it was never set on any error in the sequence.
type beamError struct {
	cause   error  // The error being wrapped. If nil then this is the first error.
	context string // Adds additional context to this error and any following.
	msg     string // Message describing an error.
	top     string // The first error message to display to a user. Propogated upwards.
}

// Error outputs a beamError as a string. The top-level error message is
// displayed first, followed by each error's context and error message in
// sequence. The original error is output last.
func (e *beamError) Error() string {
	var builder strings.Builder

	if e.top != "" {
		builder.WriteString(fmt.Sprintf("%s\nFull error:\n", e.top))
	}

	e.printRecursive(&builder)

	return builder.String()
}

// printRecursive is a helper function for outputting the contexts and messages
// of a sequence of beamErrors.
func (e *beamError) printRecursive(builder *strings.Builder) {
	wraps := e.cause != nil

	if e.context != "" {
		// Increase the indent for multi-line contexts.
		builder.WriteString(fmt.Sprintf("\t%s\n", strings.ReplaceAll(e.context, "\n", "\n\t")))
	}
	if e.msg != "" {
		builder.WriteString(e.msg)
		if wraps {
			builder.WriteString("\n\tcaused by:\n")
		}
	}

	if wraps {
		if be, ok := e.cause.(*beamError); ok {
			be.printRecursive(builder)
		} else {
			builder.WriteString(e.cause.Error())
		}
	}
}

// Format implements the fmt.Formatter interface
func (e *beamError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v', 's':
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

// Unwrap returns the cause of this error if present.
func (e *beamError) Unwrap() error {
	return e.cause
}
