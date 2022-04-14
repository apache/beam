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
