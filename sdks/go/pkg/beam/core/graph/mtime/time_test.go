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

package mtime

import (
	"math"
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	tests := []struct {
		name     string
		baseTime Time
		addition time.Duration
		expOut   Time
	}{
		{
			"insignificant addition small",
			Time(1000),
			1 * time.Nanosecond,
			Time(1000),
		},
		{
			"insignificant addition large",
			Time(1000),
			999999 * time.Nanosecond,
			Time(1000),
		},
		{
			"significant addition small",
			Time(1000),
			1 * time.Millisecond,
			Time(1001),
		},
		{
			"significant addition large",
			Time(1000),
			10 * time.Second,
			Time(11000),
		},
		{
			"add past max timestamp",
			MaxTimestamp,
			1 * time.Minute,
			MaxTimestamp,
		},
		{
			"add across max boundary",
			Time(int64(MaxTimestamp) - 10000),
			10 * time.Minute,
			MaxTimestamp,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.baseTime.Add(test.addition), test.expOut; got != want {
				t.Errorf("(%v).Add(%v), got time %v, want %v", test.baseTime, test.addition, got, want)
			}
		})
	}
}

func TestSubtract(t *testing.T) {
	tests := []struct {
		name        string
		baseTime    Time
		subtraction time.Duration
		expOut      Time
	}{
		{
			"insignificant subtraction small",
			Time(1000),
			1 * time.Nanosecond,
			Time(1000),
		},
		{
			"insignificant subtraction large",
			Time(1000),
			999999 * time.Nanosecond,
			Time(1000),
		},
		{
			"significant subtraction small",
			Time(1000),
			1 * time.Millisecond,
			Time(999),
		},
		{
			"significant subtraction large",
			Time(1000),
			10 * time.Second,
			Time(-9000),
		},
		{
			"subtract past min timestamp",
			MinTimestamp,
			1 * time.Minute,
			MinTimestamp,
		},
		{
			"subtract across min boundary",
			Time(int64(MinTimestamp) + 10000),
			10 * time.Minute,
			MinTimestamp,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.baseTime.Subtract(test.subtraction), test.expOut; got != want {
				t.Errorf("(%v).Subtract(%v), got time %v, want %v", test.baseTime, test.subtraction, got, want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		name   string
		in     Time
		expOut Time
	}{
		{
			"min timestamp",
			MinTimestamp,
			MinTimestamp,
		},
		{
			"max timestamp",
			MaxTimestamp,
			MaxTimestamp,
		},
		{
			"end of global window",
			EndOfGlobalWindowTime,
			EndOfGlobalWindowTime,
		},
		{
			"beyond max timestamp",
			Time(math.MaxInt64),
			MaxTimestamp,
		},
		{
			"below min timestamp",
			Time(math.MinInt64),
			MinTimestamp,
		},
		{
			"normal value",
			Time(int64(20000)),
			Time(int64(20000)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := Normalize(test.in), test.expOut; got != want {
				t.Errorf("Normalize(%v), got Time %v, want %v", test.in, got, want)
			}
		})
	}
}

func TestFromTime(t *testing.T) {
	tests := []struct {
		name   string
		input  time.Time
		expOut Time
	}{
		{
			"zero unix",
			time.Unix(0, 0).UTC(),
			Time(0),
		},
		{
			"behind unix",
			time.Unix(-1, 0).UTC(),
			Time(-1000),
		},
		{
			"ahead of unix",
			time.Unix(1, 0).UTC(),
			Time(1000),
		},
		{
			"insignificant time small",
			time.Unix(0, 1),
			Time(0),
		},
		{
			"insignificant time large",
			time.Unix(0, 999999),
			Time(0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := FromTime(test.input), test.expOut; got != want {
				t.Errorf("FromTime(%v), got %v, want %v", test.input, got, want)
			}
		})
	}
}

func TestToTime(t *testing.T) {
	tests := []struct {
		name   string
		input  Time
		expOut time.Time
	}{
		{
			"zero unix",
			Time(0),
			time.Unix(0, 0).UTC(),
		},
		{
			"behind unix",
			Time(-1000),
			time.Unix(-1, 0).UTC(),
		},
		{
			"ahead of unix",
			Time(1000),
			time.Unix(1, 0).UTC(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.input.ToTime(), test.expOut; !got.Equal(want) {
				t.Errorf("ToTime(%v), got %v, want %v", test.input, got, want)
			}
		})
	}
}

func TestMilliseconds(t *testing.T) {
	tests := []struct {
		name        string
		inputMillis int64
	}{
		{
			"Zero",
			int64(0),
		},
		{
			"End of Global Window",
			EndOfGlobalWindowTime.Milliseconds(),
		},
		{
			"some number",
			int64(1000),
		},
	}
	for _, test := range tests {
		milliTime := FromMilliseconds(test.inputMillis)
		outputMillis := milliTime.Milliseconds()
		if got, want := outputMillis, test.inputMillis; got != want {
			t.Errorf("got %v milliseconds, want %v milliseconds", got, want)
		}
	}
}

func TestFromDuration(t *testing.T) {
	tests := []struct {
		name string
		dur  time.Duration
	}{
		{
			"zero",
			0 * time.Millisecond,
		},
		{
			"End of Global Window",
			time.Duration(EndOfGlobalWindowTime),
		},
		{
			"day",
			24 * time.Hour,
		},
	}
	for _, test := range tests {
		durTime := FromDuration(test.dur)
		timeMillis := durTime.Milliseconds()
		if got, want := timeMillis, test.dur.Milliseconds(); got != want {
			t.Errorf("got %v milliseconds, want %v milliseconds", got, want)
		}
	}
}
