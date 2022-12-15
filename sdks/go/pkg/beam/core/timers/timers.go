// Licensed to the Apache SoFiringTimestampware Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, soFiringTimestampware
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package timer provides structs for reading and writing timers.
package timers

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

var (
	// ProviderType is timer provider type.
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

type timeDomainEnum int32

const (
	// TimeDomainUnspecified represents unspecified time domain.
	TimeDomainUnspecified timeDomainEnum = 0
	// TimeDomainEventTime represents event time domain.
	TimeDomainEventTime timeDomainEnum = 1
	// TimeDomainProcessingTime represents processing time domain.
	TimeDomainProcessingTime timeDomainEnum = 2
)

// TimerMap is a placeholder used by timer provider to manipulate timers.
type TimerMap struct {
	Key                          string
	Tag                          string
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
}

type timerOptions func(*TimerMap)

// WithTag sets the tag for the timer.
func WithTag(tag string) timerOptions {
	return func(tm *TimerMap) {
		tm.Tag = tag
	}
}

// WithOutputTimestamp sets the output timestamp for the timer.
func WithOutputTimestamp(outputTimestamp mtime.Time) timerOptions {
	return func(tm *TimerMap) {
		tm.HoldTimestamp = outputTimestamp
	}
}

// Provider represents the DoFn parameter used to get and manipulate timers.
// Use this as a parameter in DoFn and pass it to the timer methods like Set() and Clear().
// NOTE: Do not use this interface directly to manipulate timers.
type Provider interface {
	Set(t TimerMap)
}

// PipelineTimer interface is used to implement different types of timers, that is,
// event-time and processing-time.
type PipelineTimer interface {
	TimerKey() string
	TimerDomain() timeDomainEnum
}

// EventTimeTimer represents the event time timer.
type EventTimeTimer struct {
	Key  string
	Kind timeDomainEnum
}

// Set sets the timer for a event-time timestamp. Calling this method repeatedly for the same key
// will cause it overwrite previously set timer.
func (et *EventTimeTimer) Set(p Provider, FiringTimestamp mtime.Time, opts ...timerOptions) {
	tm := TimerMap{Key: et.Key, FireTimestamp: FiringTimestamp}
	for _, opt := range opts {
		opt(&tm)
	}
	p.Set(tm)
}

// Clear clears this timer.
func (et *EventTimeTimer) Clear(p Provider) {
	p.Set(TimerMap{Key: et.Key, Clear: true})
}

// TimerKey returns the key for this timer.
func (et EventTimeTimer) TimerKey() string {
	return et.Key
}

// TimerDomain returns the time domain for this timer.
func (et EventTimeTimer) TimerDomain() timeDomainEnum {
	return et.Kind
}

// ProcessingTimeTimer represents the processing time timer.
type ProcessingTimeTimer struct {
	Key  string
	Kind timeDomainEnum
}

// TimerKey returns the key for this timer.
func (pt ProcessingTimeTimer) TimerKey() string {
	return pt.Key
}

// TimerDomain returns the time domain for this timer.
func (pt ProcessingTimeTimer) TimerDomain() timeDomainEnum {
	return pt.Kind
}

// Set sets the timer for processing time domain. Calling this method repeatedly for the same key
// will cause it overwrite previously set timer.
func (pt *ProcessingTimeTimer) Set(p Provider, FiringTimestamp mtime.Time, opts ...timerOptions) {
	tm := TimerMap{Key: pt.Key, FireTimestamp: FiringTimestamp}
	for _, opt := range opts {
		opt(&tm)
	}
	p.Set(tm)
}

// Clear clears this timer.
func (pt ProcessingTimeTimer) Clear(p Provider) {
	p.Set(TimerMap{Key: pt.Key, Clear: true})
}

// MakeEventTimeTimer creates a new event time timer with given key.
func MakeEventTimeTimer(Key string) EventTimeTimer {
	return EventTimeTimer{Key: Key, Kind: TimeDomainEventTime}
}

// MakeProcessingTimeTimer creates a new processing time timer with given key.
func MakeProcessingTimeTimer(Key string) ProcessingTimeTimer {
	return ProcessingTimeTimer{Key: Key, Kind: TimeDomainProcessingTime}
}
