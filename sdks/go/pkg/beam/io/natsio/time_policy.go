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

package natsio

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

type timePolicy int

const (
	processingTimePolicy timePolicy = iota
	publishingTimePolicy
)

type timestampFn func(time.Time) mtime.Time

func processingTime(_ time.Time) mtime.Time {
	return mtime.Now()
}

func publishingTime(t time.Time) mtime.Time {
	return mtime.FromTime(t)
}

func (p timePolicy) TimestampFn() timestampFn {
	switch p {
	case processingTimePolicy:
		return processingTime
	case publishingTimePolicy:
		return publishingTime
	default:
		panic("unsupported time policy")
	}
}
