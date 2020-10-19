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

package task

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"time"
)

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	windowed := beam.WindowInto(s, window.NewFixedWindows(time.Hour), input)
	return beam.ParDo(s, timestampFn, windowed)
}

func timestampFn(iw beam.Window, et beam.EventTime, line string) Commit {
	return Commit{
		MaxTimestampWindow: toTime(iw.MaxTimestamp()),
		EventTimestamp:     toTime(et),
		Line:               line,
	}
}

func toTime(et beam.EventTime) time.Time {
	return time.Unix(0, et.Milliseconds() * int64(time.Millisecond))
}

type Commit struct {
	MaxTimestampWindow time.Time
	EventTimestamp     time.Time
	Line               string
}

func (c Commit) String() string {
	return fmt.Sprintf("Window ending at: %v contains timestamp: %v for commit: \"%s\"",
		c.MaxTimestampWindow.Format(time.Kitchen),
		c.EventTimestamp.Format(time.Kitchen),
		c.Line)
}

