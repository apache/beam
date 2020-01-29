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
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/ptypes"
)

// Metric cells are named and scoped by ptransform, and bundle,
// the latter two of which are only known at runtime. We propagate
// the PTransformID and BundleID via a context.Context. Consequently
// using metrics requires the PTransform have a context.Context
// argument.

// perBundle is a struct that retains per transform countersets.
// TODO(lostluck): Migrate the exec package to use these to extract
// metric data for export to runner, rather than the global store.
type perBundle struct {
	mu  sync.Mutex
	css []*ptCounterSet
}

type nameHash uint64

// ptCounterSet is the internal tracking struct for a single ptransform
// in a single bundle for all counter types.
type ptCounterSet struct {
	// We store the user path access to the cells in metric type segregated
	// maps. At present, caching the name hash,
	counters      map[nameHash]*counter
	distributions map[nameHash]*distribution
	gauges        map[nameHash]*gauge
}

type ctxKey string

const (
	bundleKey     ctxKey = "beam:bundle"
	ptransformKey ctxKey = "beam:ptransform"
	counterSetKey ctxKey = "beam:counterset"
)

// beamCtx is a caching context for IDs necessary to place metric updates.
// Allocating contexts and searching for PTransformIDs for every element
// is expensive, so we avoid it if possible.
type beamCtx struct {
	context.Context
	bundleID, ptransformID string
	bs                     *perBundle
	cs                     *ptCounterSet
}

// Value implements context.Value for beamCtx, and lifts the
// values for metrics keys for value for faster lookups.
func (ctx *beamCtx) Value(key interface{}) interface{} {
	switch key {
	case counterSetKey:
		if ctx.cs == nil {
			if cs := ctx.Context.Value(key); cs != nil {
				ctx.cs = cs.(*ptCounterSet)
			} else {
				return nil
			}
		}
		return ctx.cs
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

func (ctx *beamCtx) String() string {
	return fmt.Sprintf("beamCtx[%s;%s]", ctx.bundleID, ctx.ptransformID)
}

// SetBundleID sets the id of the current Bundle.
func SetBundleID(ctx context.Context, id string) context.Context {
	// Checking for *beamCtx is an optimization, so we don't dig deeply
	// for ids if not necessary.
	if bctx, ok := ctx.(*beamCtx); ok {
		return &beamCtx{Context: bctx.Context, bundleID: id, bs: &perBundle{}, ptransformID: bctx.ptransformID}
	}
	return &beamCtx{Context: ctx, bundleID: id, bs: &perBundle{}}
}

// SetPTransformID sets the id of the current PTransform.
// Must only be called on a context returened by SetBundleID.
func SetPTransformID(ctx context.Context, id string) context.Context {
	// Checking for *beamCtx is an optimization, so we don't dig deeply
	// for ids if not necessary.
	if bctx, ok := ctx.(*beamCtx); ok {
		return &beamCtx{Context: bctx.Context, bundleID: bctx.bundleID, bs: bctx.bs, ptransformID: id}
	}
	panic(fmt.Sprintf("SetPTransformID called before SetBundleID for %v", id))
	return nil // never runs.
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

func getCounterSet(ctx context.Context) *ptCounterSet {
	if id := ctx.Value(counterSetKey); id != nil {
		return id.(*ptCounterSet)
	}
	// It's not set anywhere and wasn't hoisted, so create it.
	if bctx, ok := ctx.(*beamCtx); ok {
		bctx.bs.mu.Lock()
		cs := &ptCounterSet{
			counters:      make(map[nameHash]*counter),
			distributions: make(map[nameHash]*distribution),
			gauges:        make(map[nameHash]*gauge),
		}
		bctx.bs.css = append(bctx.bs.css, cs)
		bctx.cs = cs
		bctx.bs.mu.Unlock()
		return cs
	}
	panic("counterSet missing, beam isn't set up properly.")
	return nil // never runs.
}

type kind uint8

const (
	kindUnknown kind = iota
	kindSumCounter
	kindDistribution
	kindGauge
)

func (t kind) String() string {
	switch t {
	case kindSumCounter:
		return "Counter"
	case kindDistribution:
		return "Distribution"
	case kindGauge:
		return "Gauge"
	default:
		panic(fmt.Sprintf("Unknown metric type value: %v", uint8(t)))
	}
}

// userMetric knows how to convert it's value to a Metrics_User proto.
// TODO(lostluck): Move proto translation to the harness package to
// avoid the proto dependency outside of the harness.
type userMetric interface {
	toProto() *fnexecution_v1.Metrics_User
	kind() kind
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

// We hash the name to a uint64 so we avoid using go's native string hashing for
// every use of a metrics. uint64s have faster lookup than strings as a result.
// Collisions are possible, but statistically unlikely as namespaces and names
// are usually short enough to avoid this.
var (
	hasherMu sync.Mutex
	hasher   = fnv.New64a()
)

func hashName(ns, n string) nameHash {
	hasherMu.Lock()
	hasher.Reset()
	var buf [64]byte
	b := buf[:]
	hashString(ns, b)
	hashString(n, b)
	h := hasher.Sum64()
	hasherMu.Unlock()
	return nameHash(h)
}

// hashString hashes a string with the package level hasher
// and requires posession of the hasherMu lock. The byte
// slice is assumed to be backed by a [64]byte.
func hashString(s string, b []byte) {
	l := len(s)
	i := 0
	for len(s)-i > 64 {
		n := i + 64
		copy(b, s[i:n])
		ioutilx.WriteUnsafe(hasher, b)
		i = n
	}
	n := l - i
	copy(b, s[i:])
	ioutilx.WriteUnsafe(hasher, b[:n])
}

type key struct {
	name               name
	bundle, ptransform string
}

var (
	// mu protects access to the global store
	mu sync.RWMutex
	// store is a map of BundleIDs to PtransformIDs to userMetrics.
	// it permits us to extract metric protos for runners per data Bundle, and
	// per PTransform.
	// TODO(lostluck): Migrate exec/plan.go to manage it's own perBundle counter stores.
	// Note: This is safe to use to read the metrics concurrently with user modification
	// in bundles, as only initial use modifies this map, and only contains the resulting
	// metrics.
	store = make(map[string]map[string]map[name]userMetric)
)

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
	if ms, ok := store[key.bundle][key.ptransform][key.name]; ok {
		if ms.kind() != m.kind() {
			panic(fmt.Sprintf("metric name %s being reused for a second metric in a single PTransform", key.name))
		}
		return
	}
	store[key.bundle][key.ptransform][key.name] = m
}

// Counter is a simple counter for incrementing and decrementing a value.
type Counter struct {
	name name
	hash nameHash
}

func (m *Counter) String() string {
	return fmt.Sprintf("Counter metric %s", m.name)
}

// NewCounter returns the Counter with the given namespace and name.
func NewCounter(ns, n string) *Counter {
	return &Counter{
		name: newName(ns, n),
		hash: hashName(ns, n),
	}
}

// Inc increments the counter within the given PTransform context by v.
func (m *Counter) Inc(ctx context.Context, v int64) {
	cs := getCounterSet(ctx)
	if c, ok := cs.counters[m.hash]; ok {
		c.inc(v)
		return
	}
	// We're the first to create this metric!
	c := &counter{
		value: v,
	}
	cs.counters[m.hash] = c
	key := getContextKey(ctx, m.name)
	storeMetric(key, c)
}

// Dec decrements the counter within the given PTransform context by v.
func (m *Counter) Dec(ctx context.Context, v int64) {
	m.Inc(ctx, -v)
}

// counter is a metric cell for counter values.
type counter struct {
	value int64
}

func (m *counter) inc(v int64) {
	atomic.AddInt64(&m.value, v)
}

func (m *counter) String() string {
	return fmt.Sprintf("value: %d", m.value)
}

// toProto returns a Metrics_User populated with the Data messages, but not the name. The
// caller needs to populate with the metric's name.
func (m *counter) toProto() *fnexecution_v1.Metrics_User {
	return &fnexecution_v1.Metrics_User{
		Data: &fnexecution_v1.Metrics_User_CounterData_{
			CounterData: &fnexecution_v1.Metrics_User_CounterData{
				Value: atomic.LoadInt64(&m.value),
			},
		},
	}
}

func (m *counter) kind() kind {
	return kindSumCounter
}

// Distribution is a simple distribution of values.
type Distribution struct {
	name name
	hash nameHash
}

func (m *Distribution) String() string {
	return fmt.Sprintf("Distribution metric %s", m.name)
}

// NewDistribution returns the Distribution with the given namespace and name.
func NewDistribution(ns, n string) *Distribution {
	return &Distribution{
		name: newName(ns, n),
		hash: hashName(ns, n),
	}
}

// Update updates the distribution within the given PTransform context with v.
func (m *Distribution) Update(ctx context.Context, v int64) {
	cs := getCounterSet(ctx)
	if d, ok := cs.distributions[m.hash]; ok {
		d.update(v)
		return
	}
	// We're the first to create this metric!
	d := &distribution{
		count: 1,
		sum:   v,
		min:   v,
		max:   v,
	}
	cs.distributions[m.hash] = d
	key := getContextKey(ctx, m.name)
	storeMetric(key, d)
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

func (m *distribution) kind() kind {
	return kindDistribution
}

// Gauge is a time, value pair metric.
type Gauge struct {
	name name
	hash nameHash
}

func (m *Gauge) String() string {
	return fmt.Sprintf("Guage metric %s", m.name)
}

// NewGauge returns the Gauge with the given namespace and name.
func NewGauge(ns, n string) *Gauge {
	return &Gauge{
		name: newName(ns, n),
		hash: hashName(ns, n),
	}
}

// TODO(lostluck): 2018/03/05 Use a common internal beam now() instead, once that exists.
var now = time.Now

// Set sets the gauge to the given value, and associates it with the current time on the clock.
func (m *Gauge) Set(ctx context.Context, v int64) {
	cs := getCounterSet(ctx)
	if g, ok := cs.gauges[m.hash]; ok {
		g.set(v)
		return
	}
	// We're the first to create this metric!
	g := &gauge{
		t: now(),
		v: v,
	}
	cs.gauges[m.hash] = g
	key := getContextKey(ctx, m.name)
	storeMetric(key, g)
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

func (m *gauge) kind() kind {
	return kindGauge
}

func (m *gauge) String() string {
	return fmt.Sprintf("%v time: %s value: %d", m.kind(), m.t, m.v)
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
				m := store[b][pt][n]
				p("\t%s - %s", n, m)
			}
		}
	}
}

// Clear resets all storage associated with metrics for tests.
// Calling this in pipeline code leads to inaccurate metrics.
func Clear() {
	mu.Lock()
	store = make(map[string]map[string]map[name]userMetric)
	mu.Unlock()
}

// ClearBundleData removes stored references associated with a given bundle,
// so it can be garbage collected.
func ClearBundleData(b string) {
	// No concurrency races since mu guards all access to store,
	// and the metric cell sync.Maps are goroutine safe.
	mu.Lock()
	delete(store, b)
	mu.Unlock()
}
