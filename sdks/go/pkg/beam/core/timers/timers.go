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
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

type TimeDomainEnum int32

const (
	TimeDomainUnspecified    TimeDomainEnum = 0
	TimeDomainEventTime      TimeDomainEnum = 1
	TimeDomainProcessingTime TimeDomainEnum = 2
)

type TimerMap struct {
	Family                       string
	Tag                          string
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
}

type timerConfig struct {
	Tag           string
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
		tm.HoldTimestamp = mtime.FromTime(outputTimestamp)
	}
}

type Provider interface {
	Set(t TimerMap)
}

// EventTime represents the event time timer.
type EventTime struct {
	Family string
}

func (et EventTime) TimerFamily() string {
	return et.Family
}

func (et EventTime) TimerDomain() TimeDomainEnum {
	return TimeDomainEventTime
}

// Set sets the timer for a event-time timestamp. Calling this method repeatedly for the same key
// will cause it overwrite previously set timer.
func (et *EventTime) Set(p Provider, FiringTimestamp time.Time, opts ...timerOptions) {
	tc := timerConfig{}
	for _, opt := range opts {
		opt(&tc)
	}
	tm := TimerMap{Family: et.Family, Tag: tc.Tag, FireTimestamp: mtime.FromTime(FiringTimestamp), HoldTimestamp: mtime.FromTime(FiringTimestamp)}
	if !tc.HoldTimestamp.ToTime().IsZero() {
		tm.HoldTimestamp = tc.HoldTimestamp
	}
	p.Set(tm)
}

// Clear clears this timer.
func (et *EventTime) Clear(p Provider) {
	p.Set(TimerMap{Family: et.Family, Clear: true})
}

// ProcessingTime represents the processing time timer.
type ProcessingTime struct {
	Family string
}

func (pt ProcessingTime) TimerFamily() string {
	return pt.Family
}

func (pt ProcessingTime) TimerDomain() TimeDomainEnum {
	return TimeDomainProcessingTime
}

// Set sets the timer for processing time domain. Calling this method repeatedly for the same key
// will cause it overwrite previously set timer.
func (pt *ProcessingTime) Set(p Provider, FiringTimestamp time.Time, opts ...timerOptions) {
	tc := timerConfig{}
	for _, opt := range opts {
		opt(&tc)
	}
	tm := TimerMap{Family: pt.Family, Tag: tc.Tag, FireTimestamp: mtime.FromTime(FiringTimestamp), HoldTimestamp: mtime.FromTime(FiringTimestamp)}
	if !tc.HoldTimestamp.ToTime().IsZero() {
		tm.HoldTimestamp = tc.HoldTimestamp
	}

	p.Set(tm)
}

// Clear clears this timer.
func (pt ProcessingTime) Clear(p Provider) {
	p.Set(TimerMap{Family: pt.Family, Clear: true})
}

func InEventTime(Key string) EventTime {
	return EventTime{Family: Key}
}

func InProcessingTime(Key string) ProcessingTime {
	return ProcessingTime{Family: Key}
}
