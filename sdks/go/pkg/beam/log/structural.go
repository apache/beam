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

package log

import (
	"context"
	slogger "log/slog"
)

// Structural is a wrapper over slog
type Structural struct{}

var loggerMap = map[Severity]func(string, ...any){
	SevUnspecified: slogger.Info,
	SevDebug:       slogger.Debug,
	SevInfo:        slogger.Info,
	SevWarn:        slogger.Warn,
	SevError:       slogger.Error,
	SevFatal:       slogger.Error,
}

// Log logs the message to the structural Go logger. For Panic, it does not
// perform the os.Exit(1) call, but defers to the log wrapper.
func (s *Structural) Log(ctx context.Context, sev Severity, _ int, msg string) {
	loggerMap[sev](msg)
}
