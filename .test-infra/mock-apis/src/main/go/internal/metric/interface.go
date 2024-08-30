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

package metric

import (
	"context"
	"time"
)

// Writer writes to a metric sink.
type Writer interface {

	// Write to a metric sink.
	Write(ctx context.Context, name string, unit string, points ...*Point) error
}

// Point models a metric data point.
type Point struct {

	// Timestamp of the metric data point.
	Timestamp time.Time

	// Value of the metric data point.
	Value int64
}
