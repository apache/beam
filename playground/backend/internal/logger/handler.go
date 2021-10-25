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

// Handler interface for logger handlers that used to write the log entries to different outputs provided by particular handler.
// When messages are logged via the logger, the messages are eventually forwarded to handlers.
type Handler interface {

	// Info logs a message at level Info.
	Info(args ...interface{})

	// Infof formats according to a format specifier and logs a message at level Info.
	Infof(format string, args ...interface{})

	// Warn logs a message at level Warn.
	Warn(args ...interface{})

	// Warnf formats according to a format specifier and logs a message at level Warn.
	Warnf(format string, args ...interface{})

	// Error logs a message at level Error.
	Error(args ...interface{})

	// Errorf formats according to a format specifier and logs a message at level Error.
	Errorf(format string, args ...interface{})

	// Debug logs a message at level Debug.
	Debug(args ...interface{})

	// Debugf formats according to a format specifier and logs a message at level Debug.
	Debugf(format string, args ...interface{})

	// Fatal logs a message at level Fatal
	// Then the process will exit with status 1
	Fatal(args ...interface{})

	// Fatalf formats according to a format specifier and logs a message at level Fatal.
	// Then the process will exit with status 1
	Fatalf(format string, args ...interface{})
}
