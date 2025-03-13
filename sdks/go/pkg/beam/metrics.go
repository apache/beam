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

package beam

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
)

// Implementation Note: The wrapping of the embedded methods
// is to allow better GoDocs for the methods on the proxy types.

// Counter is a metric that can be incremented and decremented,
// and is aggregated by the sum.
//
// Counters are safe to use in multiple bundles simultaneously, but
// not generally threadsafe. Your DoFn needs to manage the thread
// safety of Beam metrics for any additional concurrency it uses.
type Counter struct {
	*metrics.Counter
}

// Inc increments the counter within by the given amount. The context must be
// provided by the framework, or the value will not be recorded.
func (c Counter) Inc(ctx context.Context, v int64) {
	c.Counter.Inc(ctx, v)
}

// Dec decrements the counter within by the given amount. The context must be
// provided by the framework, or the value will not be recorded.
func (c Counter) Dec(ctx context.Context, v int64) {
	c.Counter.Dec(ctx, v)
}

// NewCounter returns the Counter with the given namespace and name.
func NewCounter(namespace, name string) Counter {
	return Counter{metrics.NewCounter(namespace, name)}
}

// Distribution is a metric that records various statistics about the distribution
// of reported values.
//
// Distributions are safe to use in multiple bundles simultaneously, but
// not generally threadsafe. Your DoFn needs to manage the thread
// safety of Beam metrics for any additional concurrency it uses.
type Distribution struct {
	*metrics.Distribution
}

// Update adds an observation to this distribution. The context must be
// provided by the framework, or the value will not be recorded.
func (c Distribution) Update(ctx context.Context, v int64) {
	c.Distribution.Update(ctx, v)
}

// NewDistribution returns the Distribution with the given namespace and name.
func NewDistribution(namespace, name string) Distribution {
	return Distribution{metrics.NewDistribution(namespace, name)}
}

// Gauge is a metric that can have its new value set, and is aggregated by taking
// the last reported value.
//
// Gauge are safe to use in multiple bundles simultaneously, but
// not generally threadsafe. Your DoFn needs to manage the thread
// safety of Beam metrics for any additional concurrency it uses.
type Gauge struct {
	*metrics.Gauge
}

// Set sets the current value for this gauge. The context must be
// provided by the framework, or the value will not be recorded.
func (c Gauge) Set(ctx context.Context, v int64) {
	c.Gauge.Set(ctx, v)
}

// NewGauge returns the Gauge with the given namespace and name.
func NewGauge(namespace, name string) Gauge {
	return Gauge{metrics.NewGauge(namespace, name)}
}
