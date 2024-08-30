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

// Package timers contains structs for setting pipeline timers.
package timers

import (
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

var (
	// ProviderType represents the type of timer provider.
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

// TimeDomain represents different time domains to set timer.
type TimeDomain int32

const (
	// UnspecifiedTimeDomain represents unspecified time domain.
	UnspecifiedTimeDomain TimeDomain = 0
	// EventTimeDomain is time from the perspective of the data
	EventTimeDomain TimeDomain = 1
	// ProcessingTimeDomain is time from the perspective of the
	// execution of your pipeline
	ProcessingTimeDomain TimeDomain = 2
)

// TimerMap holds timer information obtained from the pipeline.
//
// For SDK internal use, and subject to change.
type TimerMap struct {
	Family                       string
	Tag                          string
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
}

// timerConfig is used transiently to hold configuration from the functional options.
type timerConfig struct {
	Tag           string
	HoldSet       bool // Whether the HoldTimestamp was set.
	HoldTimestamp mtime.Time
}

type timerOptions func(*timerConfig)

// WithTag sets the tag for the timer.
func WithTag(tag string) timerOptions {
	return func(tm *timerConfig) {
		tm.Tag = tag
	}
}

// WithOutputTimestamp sets the output timestamp for the timer.
func WithOutputTimestamp(outputTimestamp time.Time) timerOptions {
	return func(tm *timerConfig) {
		tm.HoldSet = true
		tm.HoldTimestamp = mtime.FromTime(outputTimestamp)
	}
}

// Context is a parameter for OnTimer methods to receive the fired Timer.
type Context struct {
	Family string
	Tag    string
}

// Provider represents a timer provider interface.
//
// The methods are not intended for end user use, and is subject to change.
type Provider interface {
	Set(t TimerMap)
}

// PipelineTimer interface represents valid timer type.
type PipelineTimer interface {
	Timers() map[string]TimeDomain
}

// EventTime represents the event time timer.
type EventTime struct {
	Family string
}

// Timers returns mapping of timer family ID and its time domain.
func (et EventTime) Timers() map[string]TimeDomain {
	return map[string]TimeDomain{et.Family: EventTimeDomain}
}

// Set sets the timer for a event-time timestamp. Calling this method repeatedly for the same key
// will cause it overwrite previously set timer.
func (et EventTime) Set(p Provider, FiringTimestamp time.Time, opts ...timerOptions) {
	tc := timerConfig{}
	for _, opt := range opts {
		opt(&tc)
	}
	tm := TimerMap{Family: et.Family, Tag: tc.Tag, FireTimestamp: mtime.FromTime(FiringTimestamp), HoldTimestamp: mtime.FromTime(FiringTimestamp)}
	if tc.HoldSet {
		tm.HoldTimestamp = tc.HoldTimestamp
	}
	p.Set(tm)
}

// Clear clears this timer.
func (et EventTime) Clear(p Provider) {
	p.Set(TimerMap{Family: et.Family, Clear: true})
}

// ClearTag clears this timer for the given tag.
func (et EventTime) ClearTag(p Provider, tag string) {
	p.Set(TimerMap{Family: et.Family, Clear: true, Tag: tag})
}

// ProcessingTime represents the processing time timer.
type ProcessingTime struct {
	Family string
}

// Timers returns mapping of timer family ID and its time domain.
func (pt ProcessingTime) Timers() map[string]TimeDomain {
	return map[string]TimeDomain{pt.Family: ProcessingTimeDomain}
}

// Set sets the timer for processing time domain. Calling this method repeatedly for the same key
// will cause it overwrite previously set timer.
func (pt ProcessingTime) Set(p Provider, FiringTimestamp time.Time, opts ...timerOptions) {
	tc := timerConfig{}
	for _, opt := range opts {
		opt(&tc)
	}
	tm := TimerMap{Family: pt.Family, Tag: tc.Tag, FireTimestamp: mtime.FromTime(FiringTimestamp), HoldTimestamp: mtime.FromTime(FiringTimestamp)}
	if tc.HoldSet {
		tm.HoldTimestamp = tc.HoldTimestamp
	}

	p.Set(tm)
}

// Clear clears this timer.
func (pt ProcessingTime) Clear(p Provider) {
	p.Set(TimerMap{Family: pt.Family, Clear: true})
}

// ClearTag clears this timer for the given tag.
func (pt ProcessingTime) ClearTag(p Provider, tag string) {
	p.Set(TimerMap{Family: pt.Family, Clear: true, Tag: tag})
}

// InEventTime creates and returns a new EventTime timer object.
func InEventTime(family string) EventTime {
	return EventTime{Family: family}
}

// InProcessingTime creates and returns a new ProcessingTime timer object.
func InProcessingTime(family string) ProcessingTime {
	return ProcessingTime{Family: family}
}
