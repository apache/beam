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
	"context"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
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

type Provider interface {
	Set(t TimerMap)
}

type EventTime struct {
	// need to export them otherwise the key comes out empty in execution?
	Family string
}

func (t EventTime) Set(p Provider, firingTimestamp time.Time) {
	fire := mtime.FromTime(firingTimestamp)
	// Hold timestamp must match fireing timestamp if not otherwise set.
	p.Set(TimerMap{Family: t.Family, FireTimestamp: fire, HoldTimestamp: fire})
}

type Opts struct {
	Tag  string
	Hold time.Time
}

func (t *EventTime) SetWithOpts(p Provider, firingTimestamp time.Time, opts Opts) {
	fire := mtime.FromTime(firingTimestamp)
	// Hold timestamp must match fireing timestamp if not otherwise set.
	tm := TimerMap{Family: t.Family, Tag: opts.Tag, FireTimestamp: fire, HoldTimestamp: fire}
	if !opts.Hold.IsZero() {
		tm.HoldTimestamp = mtime.FromTime(opts.Hold)
	}
	p.Set(tm)
}

func (e EventTime) TimerFamily() string {
	return e.Family
}

func (e EventTime) TimerDomain() TimeDomainEnum {
	return TimeDomainEventTime
}

type ProcessingTime struct {
	Family string
}

func (e ProcessingTime) TimerFamily() string {
	return e.Family
}

func (e ProcessingTime) TimerDomain() TimeDomainEnum {
	return TimeDomainProcessingTime
}

func (t ProcessingTime) Set(p Provider, firingTimestamp time.Time) {
	log.Infof(context.Background(), "setting timer in core/timer: %+v", t)
	fire := mtime.FromTime(firingTimestamp)
	p.Set(TimerMap{Family: t.Family, FireTimestamp: fire, HoldTimestamp: fire})
}

func (t ProcessingTime) SetWithOpts(p Provider, firingTimestamp time.Time, opts Opts) {
	fire := mtime.FromTime(firingTimestamp)
	// Hold timestamp must match input element timestamp if not otherwise set.
	tm := TimerMap{Family: t.Family, Tag: opts.Tag, FireTimestamp: fire, HoldTimestamp: fire}
	if !opts.Hold.IsZero() {
		tm.HoldTimestamp = mtime.FromTime(opts.Hold)
	}
	p.Set(tm)
}

func InEventTime(Key string) EventTime {
	return EventTime{Family: Key}
}

func InProcessingTime(Key string) ProcessingTime {
	return ProcessingTime{Family: Key}
}
