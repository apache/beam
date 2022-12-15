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

package timers

import (
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/google/go-cmp/cmp"
)

type fakeTimerProvider struct {
	timerDetails TimerMap
}

func (t *fakeTimerProvider) Set(tm TimerMap) {
	t.timerDetails = tm
}

func (t *fakeTimerProvider) GetMap() TimerMap {
	return t.timerDetails
}

func TestProcessingTimeTimer_Set(t *testing.T) {
	type fields struct {
		Key  string
		Kind timeDomainEnum
	}
	type args struct {
		FiringTimestamp mtime.Time
		opts            []timerOptions
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		out    TimerMap
	}{
		{
			name:   "correct set no options",
			fields: fields{Key: "Window", Kind: TimeDomainProcessingTime},
			args: args{
				FiringTimestamp: mtime.FromTime(time.Now()),
			},
			out: TimerMap{
				Key:           "Window",
				FireTimestamp: mtime.FromTime(time.Now()),
			},
		},
		{
			name:   "correct set tag option",
			fields: fields{Key: "Window", Kind: TimeDomainProcessingTime},
			args: args{
				FiringTimestamp: mtime.FromTime(time.Now()),
				opts:            []timerOptions{WithTag("windowTag")},
			},
			out: TimerMap{
				Key:           "Window",
				FireTimestamp: mtime.FromTime(time.Now()),
				Tag:           "windowTag",
			},
		},
		{
			name:   "correct set tag and timestamp option",
			fields: fields{Key: "Window", Kind: TimeDomainProcessingTime},
			args: args{
				FiringTimestamp: mtime.FromTime(time.Now()),
				opts:            []timerOptions{WithTag("windowTag"), WithOutputTimestamp(mtime.FromTime(time.Now().Add(2 * time.Second)))},
			},
			out: TimerMap{
				Key:           "Window",
				FireTimestamp: mtime.FromTime(time.Now()),
				Tag:           "windowTag",
				HoldTimestamp: mtime.FromTime(time.Now().Add(2 * time.Second)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := MakeProcessingTimeTimer(tt.fields.Key)
			provider := &fakeTimerProvider{}
			pt.Set(provider, tt.args.FiringTimestamp, tt.args.opts...)
			if pt.TimerKey() != tt.fields.Key {
				t.Errorf("processing-time timer key mismatch, got: %v, want: %v", pt.TimerKey(), tt.fields.Key)
			}
			if pt.TimerDomain() != tt.fields.Kind {
				t.Errorf("processing-time timer domain mismatch, got: %v, want: %v", pt.TimerDomain(), tt.fields.Kind)
			}
			if !cmp.Equal(provider.GetMap(), tt.out) {
				t.Errorf("processing-time timer not set correctly, got: %v, want: %v", provider.GetMap(), tt.out)
			}
			pt.Clear(provider)
			if provider.GetMap().Clear != true {
				t.Error("failed to clear processing-time timer")
			}
		})
	}

}

func TestEventTimeTimer_Set(t *testing.T) {
	type fields struct {
		Key  string
		Kind timeDomainEnum
	}
	type args struct {
		FiringTimestamp mtime.Time
		opts            []timerOptions
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		out    TimerMap
	}{
		{
			name:   "correct set no options",
			fields: fields{Key: "Window", Kind: TimeDomainEventTime},
			args: args{
				FiringTimestamp: mtime.FromTime(time.Now()),
			},
			out: TimerMap{
				Key:           "Window",
				FireTimestamp: mtime.FromTime(time.Now()),
			},
		},
		{
			name:   "correct set tag option",
			fields: fields{Key: "Window", Kind: TimeDomainEventTime},
			args: args{
				FiringTimestamp: mtime.FromTime(time.Now()),
				opts:            []timerOptions{WithTag("windowTag")},
			},
			out: TimerMap{
				Key:           "Window",
				FireTimestamp: mtime.FromTime(time.Now()),
				Tag:           "windowTag",
			},
		},
		{
			name:   "correct set tag and timestamp option",
			fields: fields{Key: "Window", Kind: TimeDomainEventTime},
			args: args{
				FiringTimestamp: mtime.FromTime(time.Now()),
				opts:            []timerOptions{WithTag("windowTag"), WithOutputTimestamp(mtime.FromTime(time.Now().Add(2 * time.Second)))},
			},
			out: TimerMap{
				Key:           "Window",
				FireTimestamp: mtime.FromTime(time.Now()),
				Tag:           "windowTag",
				HoldTimestamp: mtime.FromTime(time.Now().Add(2 * time.Second)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := MakeEventTimeTimer(tt.fields.Key)
			provider := &fakeTimerProvider{}
			pt.Set(provider, tt.args.FiringTimestamp, tt.args.opts...)
			if pt.TimerKey() != tt.fields.Key {
				t.Errorf("event-time timer key mismatch, got: %v, want: %v", pt.TimerKey(), tt.fields.Key)
			}
			if pt.TimerDomain() != tt.fields.Kind {
				t.Errorf("event-time timer domain mismatch, got: %v, want: %v", pt.TimerDomain(), tt.fields.Kind)
			}
			if !cmp.Equal(provider.GetMap(), tt.out) {
				t.Errorf("event-time timer not set correctly, got: %v, want: %v", provider.GetMap(), tt.out)
			}
			pt.Clear(provider)
			if provider.GetMap().Clear != true {
				t.Error("failed to clear event-time timer")
			}
		})
	}
}
