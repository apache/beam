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

// Package metrics implements the Beam metrics API, described at
// http://s.apache.org/beam-metrics-api
//
// Metrics in the Beam model are uniquely identified by a namespace, a name,
// and the PTransform context in which they are used. Further, they are
// reported as a delta against the bundle being processed, so that overcounting
// doesn't occur if a bundle needs to be retried. Each metric is scoped to
// their bundle, and ptransform.
//
// Cells (or metric cells) are defined for each Beam model metric
// type, and the serve as concurrency safe storage of a given metric's values.
// Proxys are exported values representing the metric, for use in user
// ptransform code. They don't retain their cells, since they don't have
// the context to be able to store them for export back to the pipeline runner.
//
// Metric cells aren't initialized until their first mutation, which
// follows from the Beam model design, where metrics are only sent for a bundle
// if they have changed. This is particularly convenient for distributions which
// means their min and max fields can be set to the first value on creation
// rather than have some marker of uninitialized state, which would otherwise
// need to be checked for on every update.
//
// Metric values are implemented as lightweight proxies of the user provided
// namespace and name. This allows them to be declared globally, and used in
// any ParDo. Further, as per the design, they can be declared dynamically
// at runtime.
//
// To handle reporting deltas on the metrics by bundle, metrics
// are keyed by bundleID,PTransformID,namespace, and name, so metrics that
// are identical except for bundles are treated as distinct, effectively
// providing per bundle deltas, since a new value cell is used per bundle.
package metrics

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/ptypes"
)

// Metric cells are named and scoped by ptransform, and bundle,
// the latter two of which are only known at runtime. We propagate
// the PTransformID and BundleID via a context.Context. Consequently
// using metrics requires the PTransform have a context.Context
// argument.

type ctxKey string

const bundleKey ctxKey = "beam:bundle"
const ptransformKey ctxKey = "beam:ptransform"

// beamCtx is a caching context for IDs necessary to place metric updates.
//  Allocating contexts and searching for PTransformIDs for every element
// is expensive, so we avoid it if possible.
type beamCtx struct {
	context.Context
	bundleID, ptransformID string
}

// Value lifts the beam contLift the keys value for faster lookups when not available.
func (ctx *beamCtx) Value(key interface{}) interface{} {
	switch key {
	case bundleKey:
		if ctx.bundleID == "" {
			if id := ctx.Context.Value(key); id != nil {
				ctx.bundleID = id.(string)
			} else {
				return nil
			}
		}
		return ctx.bundleID
	case ptransformKey:
		if ctx.ptransformID == "" {
			if id := ctx.Context.Value(key); id != nil {
				ctx.ptransformID = id.(string)
			} else {
				return nil
			}
		}
		return ctx.ptransformID
	}
	return ctx.Context.Value(key)
}

// SetBundleID sets the id of the current Bundle.
func SetBundleID(ctx context.Context, id string) context.Context {
	// Checking for *beamCtx is an optimization, so we don't dig deeply
	// for ids if not necessary.
	if bctx, ok := ctx.(*beamCtx); ok {
		return &beamCtx{Context: bctx.Context, bundleID: id, ptransformID: bctx.ptransformID}
	}
	return &beamCtx{Context: ctx, bundleID: id}
}

// SetPTransformID sets the id of the current PTransform.
func SetPTransformID(ctx context.Context, id string) context.Context {
	// Checking for *beamCtx is an optimization, so we don't dig deeply
	// for ids if not necessary.
	if bctx, ok := ctx.(*beamCtx); ok {
		return &beamCtx{Context: bctx.Context, bundleID: bctx.bundleID, ptransformID: id}
	}
	return &beamCtx{Context: ctx, ptransformID: id}
}

const (
	bundleIDUnset     = "(bundle id unset)"
	ptransformIDUnset = "(ptransform id unset)"
)

func getContextKey(ctx context.Context, n name) key {
	key := key{name: n, bundle: bundleIDUnset, ptransform: ptransformIDUnset}
	if id := ctx.Value(bundleKey); id != nil {
		key.bundle = id.(string)
	}
	if id := ctx.Value(ptransformKey); id != nil {
		key.ptransform = id.(string)
	}
	return key
}

// userMetric knows how to convert it's value to a Metrics_User proto.
type userMetric interface {
	toProto() *fnexecution_v1.Metrics_User
}

// name is a pair of strings identifying a specific metric.
type name struct {
	namespace, name string
}

func (n name) String() string {
	return fmt.Sprintf("%s.%s", n.namespace, n.name)
}

func newName(ns, n string) name {
	if len(n) == 0 || len(ns) == 0 {
		panic(fmt.Sprintf("namespace and name are required to be non-empty, got %q and %q", ns, n))
	}
	return name{namespace: ns, name: n}
}

type key struct {
	name               name
	bundle, ptransform string
}

var (
	// mu protects access to store
	mu sync.RWMutex
	// store is a map of BundleIDs to PtransformIDs to userMetrics.
	// it permits us to extract metric protos for runners per data Bundle, and
	// per PTransform.
	store = make(map[string]map[string]map[name]userMetric)

	// We store the user path access to the cells in metric type segregated
	// sync.Maps. Using sync.Maps lets metrics with disjoint keys have concurrent
	// access to the cells, and using separate sync.Map per metric type
	// simplifies code understanding, since each only contains a single type of
	// cell.

	countersMu      sync.RWMutex
	counters        = make(map[key]*counter)
	distributionsMu sync.RWMutex
	distributions   = make(map[key]*distribution)
	gaugesMu        sync.RWMutex
	gauges          = make(map[key]*gauge)
)

// TODO(lostluck): 2018/03/05 Use a common internal beam now() instead, once that exists.
var now = time.Now

// Counter is a simple counter for incrementing and decrementing a value.
type Counter struct {
	name name
	// The following are a fast cache of the key and storage
	mu sync.Mutex
	k  key
	c  *counter
}

func (m *Counter) String() string {
	return fmt.Sprintf("Counter metric %s", m.name)
}

// NewCounter returns the Counter with the given namespace and name.
func NewCounter(ns, n string) *Counter {
	mn := newName(ns, n)
	return &Counter{
		name: mn,
	}
}

// Inc increments the counter within the given PTransform context by v.
func (m *Counter) Inc(ctx context.Context, v int64) {
	key := getContextKey(ctx, m.name)
	cs := &counter{
		value: v,
	}
	m.mu.Lock()
	if m.k == key {
		m.c.inc(v)
		m.mu.Unlock()
		return
	}
	m.k = key
	countersMu.Lock()
	if c, loaded := counters[key]; loaded {
		m.c = c
		countersMu.Unlock()
		m.mu.Unlock()
		c.inc(v)
		return
	}
	m.c = cs
	counters[key] = cs
	countersMu.Unlock()
	m.mu.Unlock()
	storeMetric(key, cs)
}

// Dec decrements the counter within the given PTransform context by v.
func (m *Counter) Dec(ctx context.Context, v int64) {
	m.Inc(ctx, -v)
}

// counter is a metric cell for counter values.
type counter struct {
	value int64
	mu    sync.Mutex
}

func (m *counter) inc(v int64) {
	m.mu.Lock()
	m.value += v
	m.mu.Unlock()
}

func (m *counter) String() string {
	return fmt.Sprintf("value: %d", m.value)
}

// toProto returns a Metrics_User populated with the Data messages, but not the name. The
// caller needs to populate with the metric's name.
func (m *counter) toProto() *fnexecution_v1.Metrics_User {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &fnexecution_v1.Metrics_User{
		Data: &fnexecution_v1.Metrics_User_CounterData_{
			CounterData: &fnexecution_v1.Metrics_User_CounterData{
				Value: m.value,
			},
		},
	}
}

// Distribution is a simple distribution of values.
type Distribution struct {
	name name

	// The following are a fast cache of the key and storage
	mu sync.Mutex
	k  key
	d  *distribution
}

func (m *Distribution) String() string {
	return fmt.Sprintf("Distribution metric %s", m.name)
}

// NewDistribution returns the Distribution with the given namespace and name.
func NewDistribution(ns, n string) *Distribution {
	mn := newName(ns, n)
	return &Distribution{
		name: mn,
	}
}

// Update updates the distribution within the given PTransform context with v.
func (m *Distribution) Update(ctx context.Context, v int64) {
	key := getContextKey(ctx, m.name)
	ds := &distribution{
		count: 1,
		sum:   v,
		min:   v,
		max:   v,
	}
	m.mu.Lock()
	if m.k == key {
		m.d.update(v)
		m.mu.Unlock()
		return
	}
	m.k = key
	distributionsMu.Lock()
	if d, loaded := distributions[key]; loaded {
		m.d = d
		distributionsMu.Unlock()
		m.mu.Unlock()
		d.update(v)
		return
	}
	m.d = ds
	distributions[key] = ds
	distributionsMu.Unlock()
	m.mu.Unlock()
	storeMetric(key, ds)
}

// distribution is a metric cell for distribution values.
type distribution struct {
	count, sum, min, max int64
	mu                   sync.Mutex
}

func (m *distribution) update(v int64) {
	m.mu.Lock()
	if v < m.min {
		m.min = v
	}
	if v > m.max {
		m.max = v
	}
	m.count++
	m.sum += v
	m.mu.Unlock()
}

func (m *distribution) String() string {
	return fmt.Sprintf("count: %d sum: %d min: %d max: %d", m.count, m.sum, m.min, m.max)
}

// toProto returns a Metrics_User populated with the Data messages, but not the name. The
// caller needs to populate with the metric's name.
func (m *distribution) toProto() *fnexecution_v1.Metrics_User {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &fnexecution_v1.Metrics_User{
		Data: &fnexecution_v1.Metrics_User_DistributionData_{
			DistributionData: &fnexecution_v1.Metrics_User_DistributionData{
				Count: m.count,
				Sum:   m.sum,
				Min:   m.min,
				Max:   m.max,
			},
		},
	}
}

// Gauge is a time, value pair metric.
type Gauge struct {
	name name

	// The following are a fast cache of the key and storage
	mu sync.Mutex
	k  key
	g  *gauge
}

func (m *Gauge) String() string {
	return fmt.Sprintf("Guage metric %s", m.name)
}

// NewGauge returns the Gauge with the given namespace and name.
func NewGauge(ns, n string) *Gauge {
	mn := newName(ns, n)
	return &Gauge{
		name: mn,
	}
}

// Set sets the gauge to the given value, and associates it with the current time on the clock.
func (m *Gauge) Set(ctx context.Context, v int64) {
	key := getContextKey(ctx, m.name)
	gs := &gauge{
		t: now(),
		v: v,
	}
	m.mu.Lock()
	if m.k == key {
		m.g.set(v)
		m.mu.Unlock()
		return
	}
	m.k = key
	gaugesMu.Lock()
	if g, loaded := gauges[key]; loaded {
		m.g = g
		gaugesMu.Unlock()
		m.mu.Unlock()
		g.set(v)
		return
	}
	m.g = gs
	gauges[key] = gs
	gaugesMu.Unlock()
	m.mu.Unlock()
	storeMetric(key, gs)
}

// storeMetric stores a metric away on its first use so it may be retrieved later on.
// In the event of a name collision, storeMetric can panic, so it's prudent to release
// locks if they are no longer required.
func storeMetric(key key, m userMetric) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := store[key.bundle]; !ok {
		store[key.bundle] = make(map[string]map[name]userMetric)
	}
	if _, ok := store[key.bundle][key.ptransform]; !ok {
		store[key.bundle][key.ptransform] = make(map[name]userMetric)
	}
	if _, ok := store[key.bundle][key.ptransform][key.name]; ok {
		panic(fmt.Sprintf("metric name %s being reused for a second metric in a single PTransform", key.name))
	}
	store[key.bundle][key.ptransform][key.name] = m
}

// gauge is a metric cell for gauge values.
type gauge struct {
	mu sync.Mutex
	t  time.Time
	v  int64
}

func (m *gauge) set(v int64) {
	m.mu.Lock()
	m.t = now()
	m.v = v
	m.mu.Unlock()
}

func (m *gauge) toProto() *fnexecution_v1.Metrics_User {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, err := ptypes.TimestampProto(m.t)
	if err != nil {
		panic(err)
	}
	return &fnexecution_v1.Metrics_User{
		Data: &fnexecution_v1.Metrics_User_GaugeData_{
			GaugeData: &fnexecution_v1.Metrics_User_GaugeData{
				Value:     m.v,
				Timestamp: ts,
			},
		},
	}
}

func (m *gauge) String() string {
	return fmt.Sprintf("time: %s value: %d", m.t, m.v)
}

// ToProto exports all collected metrics for the given BundleID and PTransform ID pair.
func ToProto(b, pt string) []*fnexecution_v1.Metrics_User {
	mu.RLock()
	defer mu.RUnlock()
	ps := store[b]
	s := ps[pt]
	var ret []*fnexecution_v1.Metrics_User
	for n, m := range s {
		p := m.toProto()
		p.MetricName = &fnexecution_v1.Metrics_User_MetricName{
			Name:      n.name,
			Namespace: n.namespace,
		}
		ret = append(ret, p)
	}
	return ret
}

// DumpToLog is a debugging function that outputs all metrics available locally to beam.Log.
func DumpToLog(ctx context.Context) {
	dumpTo(func(format string, args ...interface{}) {
		log.Errorf(ctx, format, args...)
	})
}

// DumpToOut is a debugging function that outputs all metrics available locally to std out.
func DumpToOut() {
	dumpTo(func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	})
}

func dumpTo(p func(format string, args ...interface{})) {
	mu.RLock()
	defer mu.RUnlock()
	countersMu.RLock()
	defer countersMu.RUnlock()
	distributionsMu.RLock()
	defer distributionsMu.RUnlock()
	gaugesMu.RLock()
	defer gaugesMu.RUnlock()
	var bs []string
	for b := range store {
		bs = append(bs, b)
	}
	sort.Strings(bs)
	for _, b := range bs {
		var pts []string
		for pt := range store[b] {
			pts = append(pts, pt)
		}
		sort.Strings(pts)
		for _, pt := range pts {
			var ns []name
			for n := range store[b][pt] {
				ns = append(ns, n)
			}
			sort.Slice(ns, func(i, j int) bool {
				if ns[i].namespace < ns[j].namespace {
					return true
				}
				if ns[i].namespace == ns[j].namespace && ns[i].name < ns[j].name {
					return true
				}
				return false
			})
			p("Bundle: %q - PTransformID: %q", b, pt)
			for _, n := range ns {
				key := key{name: n, bundle: b, ptransform: pt}
				if m, ok := counters[key]; ok {
					p("\t%s - %s", key.name, m)
				}
				if m, ok := distributions[key]; ok {
					p("\t%s - %s", key.name, m)
				}
				if m, ok := gauges[key]; ok {
					p("\t%s - %s", key.name, m)
				}
			}
		}
	}
}

// Clear resets all storage associated with metrics for tests.
// Calling this in pipeline code leads to inaccurate metrics.
func Clear() {
	mu.Lock()
	store = make(map[string]map[string]map[name]userMetric)
	counters = make(map[key]*counter)
	distributions = make(map[key]*distribution)
	gauges = make(map[key]*gauge)
	mu.Unlock()
}

// ClearBundleData removes stored references associated with a given bundle,
// so it can be garbage collected.
func ClearBundleData(b string) {
	// No concurrency races since mu guards all access to store,
	// and the metric cell sync.Maps are goroutine safe.
	mu.Lock()
	pts := store[b]
	countersMu.Lock()
	distributionsMu.Lock()
	gaugesMu.Lock()
	for pt, m := range pts {
		for n := range m {
			key := key{name: n, bundle: b, ptransform: pt}
			delete(counters, key)
			delete(distributions, key)
			delete(gauges, key)
		}
	}
	countersMu.Unlock()
	distributionsMu.Unlock()
	gaugesMu.Unlock()
	delete(store, b)
	mu.Unlock()
}
