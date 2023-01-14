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

package sdf

import "time"

// WallTimeWatermarkEstimator is a watermark estimator that advances the
// current DoFn's output watermark to the current wallclock time on splits
// or checkpoints.
type WallTimeWatermarkEstimator struct{}

// CurrentWatermark returns the current time. It is used by the Sdk harness
// to set the current DoFn's output watermark on splits and checkpoints.
func (e *WallTimeWatermarkEstimator) CurrentWatermark() time.Time {
	return time.Now()
}

// TimestampObservingWatermarkEstimator is a watermark estimator that advances the
// current DoFn's output watermark to the timestamp of the most recently emitted
// element.
type TimestampObservingWatermarkEstimator struct {
	State time.Time
}

// CurrentWatermark returns the current watermark. It is used by the Sdk harness
// to set the current DoFn's output watermark on splits and checkpoints.
func (e *TimestampObservingWatermarkEstimator) CurrentWatermark() time.Time {
	return e.State
}

// ObserveTimestamp returns updates the watermark to the timestamp of the most
// recently emitted element. It is invoked by the Sdk after each emit. The updated
// watermark will not be reflected until a split or checkpoint occurs.
func (e *TimestampObservingWatermarkEstimator) ObserveTimestamp(t time.Time) {
	e.State = t
}

// ManualWatermarkEstimator is a watermark estimator that advances the
// current DoFn's output watermark when a user calls UpdateWatermark
// from within ProcessElement.
type ManualWatermarkEstimator struct {
	State time.Time
}

// CurrentWatermark returns the most recent timestamp set from
// ProcessElement. It is used by the Sdk harness to set the
// current DoFn's output watermark on splits and checkpoints.
func (e *ManualWatermarkEstimator) CurrentWatermark() time.Time {
	return e.State
}

// UpdateWatermark is a convenience function that can be used
// to update the current watermark from inside ProcessElement.
func (e *ManualWatermarkEstimator) UpdateWatermark(t time.Time) {
	e.State = t
}
