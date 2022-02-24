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

// Package mtime contains a millisecond representation of time. The purpose
// of this representation is alignment with the Beam specification, where we
// need extreme values outside the range of time.Time for windowing.
package mtime

import (
	"fmt"
	"math"
	"time"
)

const (
	// MinTimestamp is the minimum value for any Beam timestamp. Often referred to
	// as "-infinity". This value and MaxTimestamp are chosen so that their
	// microseconds-since-epoch can be safely represented with an int64 and boundary
	// values can be represented correctly with millisecond precision.
	MinTimestamp Time = math.MinInt64 / 1000

	// MaxTimestamp is the maximum value for any Beam timestamp. Often referred to
	// as "+infinity".
	MaxTimestamp Time = math.MaxInt64 / 1000

	// EndOfGlobalWindowTime is the timestamp at the end of the global window. It
	// is a day before the max timestamp.
	// TODO(BEAM-4179) Use GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS from the Runner API constants
	EndOfGlobalWindowTime = MaxTimestamp - 24*60*60*1000

	// ZeroTimestamp is the default zero value time. It corresponds to the unix epoch.
	ZeroTimestamp Time = 0
)

// Time is the number of milli-seconds since the Unix epoch. The valid range of
// times is bounded by what can be represented a _micro_-seconds-since-epoch.
type Time int64

// Now returns the current time.
func Now() Time {
	return FromTime(time.Now())
}

// FromMilliseconds returns a timestamp from a raw milliseconds-since-epoch value.
func FromMilliseconds(unixMilliseconds int64) Time {
	return Normalize(Time(unixMilliseconds))
}

// FromDuration returns a timestamp from a time.Duration-since-epoch value.
func FromDuration(d time.Duration) Time {
	return ZeroTimestamp.Add(d)
}

// FromTime returns a milli-second precision timestamp from a time.Time.
func FromTime(t time.Time) Time {
	// TODO(BEAM-13988): Replace t.UnixNano() with t.UnixMilli() for Go 1.17 or higher.
	return Normalize(Time(t.UnixNano() / 1e6))
}

// Milliseconds returns the number of milli-seconds since the Unix epoch.
func (t Time) Milliseconds() int64 {
	return int64(t)
}

// Add returns the time plus the duration. Input Durations of less than one
// millisecond will not increment the time due to a loss of precision when
// converting to milliseconds.
func (t Time) Add(d time.Duration) Time {
	return Normalize(Time(int64(t) + d.Milliseconds()))
}

// Subtract returns the time minus the duration. Input Durations of less than one
// millisecond will not increment the time due to a loss of precision when
// converting to milliseconds.
func (t Time) Subtract(d time.Duration) Time {
	return Normalize(Time(int64(t) - d.Milliseconds()))
}

func (t Time) String() string {
	switch t {
	case MinTimestamp:
		return "-inf"
	case MaxTimestamp:
		return "+inf"
	case EndOfGlobalWindowTime:
		return "glo"
	default:
		return fmt.Sprintf("%v", t.Milliseconds())
	}
}

// Min returns the smallest (earliest) time.
func Min(a, b Time) Time {
	if int64(a) < int64(b) {
		return a
	}
	return b
}

// Max returns the largest (latest) time.
func Max(a, b Time) Time {
	if int64(a) < int64(b) {
		return b
	}
	return a
}

// Normalize ensures a Time is within [MinTimestamp,MaxTimestamp].
func Normalize(t Time) Time {
	return Min(Max(t, MinTimestamp), MaxTimestamp)
}
