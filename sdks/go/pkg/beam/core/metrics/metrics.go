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
	"hash"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
)

// Metric cells are named and scoped by ptransform, and bundle,
// the latter two of which are only known at runtime. We propagate
// the PTransformID and BundleID via a context.Context. Consequently
// using metrics requires the PTransform have a context.Context
// argument.

type ctxKey string

const (
	counterSetKey   ctxKey = "beam:counterset"
	storeKey        ctxKey = "beam:bundlestore"
	bundleIDKey     ctxKey = "beam:instructionID"
	ptransformIDKey ctxKey = "beam:transformID"
)

// beamCtx is a caching context for IDs necessary to place metric updates.
// Allocating contexts and searching for PTransformIDs for every element
// is expensive, so we avoid it if possible.
type beamCtx struct {
	context.Context
	bundleID, ptransformID string
	store                  *Store
	cs                     *ptCounterSet
}

// Value implements the Context interface Value method for beamCtx.
// The implementation lifts the stored values for metrics keys to the
// top level beamCtx for faster lookups.
func (ctx *beamCtx) Value(key any) any {
	switch key {
	case counterSetKey:
		if ctx.cs == nil {
			if cs := ctx.Context.Value(key); cs != nil {
				ctx.cs = cs.(*ptCounterSet)
			} else {
				// It's not created previously
				ctx.store.mu.Lock()
				cs := &ptCounterSet{
					pid:           ctx.ptransformID,
					counters:      make(map[nameHash]*counter),
					distributions: make(map[nameHash]*distribution),
					gauges:        make(map[nameHash]*gauge),
				}
				ctx.store.css = append(ctx.store.css, cs)
				ctx.cs = cs
				ctx.store.mu.Unlock()
			}
		}
		return ctx.cs
	case storeKey:
		if ctx.store == nil {
			if store := ctx.Context.Value(key); store != nil {
				ctx.store = store.(*Store)
			}
		}
		return ctx.store
	case bundleIDKey:
		return ctx.bundleID
	case ptransformIDKey:
		return ctx.ptransformID
	}
	return ctx.Context.Value(key)
}

func (ctx *beamCtx) String() string {
	return fmt.Sprintf("beamCtx[%s;%s]", ctx.bundleID, ctx.ptransformID)
}

// SetBundleID sets the id of the current Bundle, and populates the store.
func SetBundleID(ctx context.Context, id string) context.Context {
	// Checking for *beamCtx is an optimization, so we don't dig deeply
	// for ids if not necessary.
	if bctx, ok := ctx.(*beamCtx); ok {
		return &beamCtx{Context: bctx.Context, bundleID: id, store: newStore(), ptransformID: bctx.ptransformID}
	}
	return &beamCtx{Context: ctx, bundleID: id, store: newStore()}
}

// SetPTransformID sets the id of the current PTransform.
// Must only be called on a context returned by SetBundleID.
func SetPTransformID(ctx context.Context, id string) context.Context {
	// Checking for *beamCtx is an optimization, so we don't dig deeply
	// for ids if not necessary.
	if bctx, ok := ctx.(*beamCtx); ok {
		bctx.store.mu.Lock()
		if _, ok := bctx.store.stateRegistry[id]; !ok {
			bctx.store.stateRegistry[id] = &[4]ExecutionState{}
		}
		bctx.store.mu.Unlock()
		return &beamCtx{Context: bctx.Context, bundleID: bctx.bundleID, store: bctx.store, ptransformID: id}
	}
	// Avoid breaking if the bundle is unset in testing.
	return &beamCtx{Context: ctx, bundleID: bundleIDUnset, store: newStore(), ptransformID: id}
}

// GetTransformID sources the TransformID from a context, if available.
//
// For Beam internal use only. Subject to change.
func GetTransformID(ctx context.Context) string {
	ret := ctx.Value(ptransformIDKey)
	if id, ok := ret.(string); ok {
		return id
	}
	return ""
}

// GetBundleID sources the Bundle's instruction ID from a context, if available.
//
// For Beam internal use only. Subject to change.
func GetBundleID(ctx context.Context) string {
	ret := ctx.Value(bundleIDKey)
	if id, ok := ret.(string); ok {
		return id
	}
	return ""
}

// GetStore extracts the metrics Store for the given context for a bundle.
//
// Returns nil if the context doesn't contain a metric Store.
func GetStore(ctx context.Context) *Store {
	if bctx, ok := ctx.(*beamCtx); ok {
		return bctx.store
	}
	if v := ctx.Value(storeKey); v != nil {
		return v.(*Store)
	}
	return nil
}

const (
	bundleIDUnset     = "(bundle id unset)"
	ptransformIDUnset = "(ptransform id unset)"
)

func getCounterSet(ctx context.Context) *ptCounterSet {
	if bctx, ok := ctx.(*beamCtx); ok && bctx.cs != nil {
		return bctx.cs
	}
	if set := ctx.Value(counterSetKey); set != nil {
		return set.(*ptCounterSet)
	}
	// This isn't a beam context, so we don't have a
	// useful counterset to return.
	return nil
}

type kind uint8

const (
	kindUnknown kind = iota
	kindSumCounter
	kindDistribution
	kindGauge
	kindDoFnMsec
)

func (t kind) String() string {
	switch t {
	case kindSumCounter:
		return "Counter"
	case kindDistribution:
		return "Distribution"
	case kindGauge:
		return "Gauge"
	case kindDoFnMsec:
		return "DoFnMsec"
	default:
		panic(fmt.Sprintf("Unknown metric type value: %v", uint8(t)))
	}
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
// are usually short enough to avoid this. A sync.Pool is used  because it can provide
// goroutine-local values that reduce contention and profiling shows hashName from NewCounter
// can be a contention hotspot. See parallel benches metrics_test.go:BenchmarkMetrics/*
var (
	hashPool = sync.Pool{
		New: func() interface{} {
			return fnv.New64a()
		},
	}
)

func hashName(ns, n string) nameHash {
	hasher := hashPool.Get().(hash.Hash64)
	hasher.Reset()
	var buf [64]byte
	b := buf[:]
	hashString(hasher, ns, b)
	hashString(hasher, n, b)
	h := hasher.Sum64()
	hashPool.Put(hasher)
	return nameHash(h)
}

// hashString hashes a string with the package level hasher
// and requires posession of the hasherMu lock. The byte
// slice is assumed to be backed by a [64]byte.
func hashString(hasher hash.Hash64, s string, b []byte) {
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
	if cs == nil {
		return
	}
	if c, ok := cs.counters[m.hash]; ok {
		c.inc(v)
		return
	}
	// We're the first to create this metric!
	c := &counter{
		value: v,
	}
	cs.counters[m.hash] = c
	GetStore(ctx).storeMetric(cs.pid, m.name, c)
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

func (m *counter) kind() kind {
	return kindSumCounter
}

func (m *counter) get() int64 {
	return atomic.LoadInt64(&m.value)
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
	if cs == nil {
		return
	}
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
	GetStore(ctx).storeMetric(cs.pid, m.name, d)
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

func (m *distribution) kind() kind {
	return kindDistribution
}

func (m *distribution) get() (count, sum, min, max int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count, m.sum, m.min, m.max
}

// DistributionValue is the value of a Distribution metric.
type DistributionValue struct {
	Count, Sum, Min, Max int64
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
	if cs == nil {
		return
	}
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
	GetStore(ctx).storeMetric(cs.pid, m.name, g)
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

func (m *gauge) kind() kind {
	return kindGauge
}

func (m *gauge) String() string {
	return fmt.Sprintf("%v time: %s value: %d", m.kind(), m.t, m.v)
}

func (m *gauge) get() (int64, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v, m.t
}

// GaugeValue is the value of a Gauge metric.
type GaugeValue struct {
	Value     int64
	Timestamp time.Time
}

type executionState struct {
	state *[4]ExecutionState
}

func (m *executionState) String() string {
	return fmt.Sprintf("value: {%v}", m.state)
}

func (m *executionState) kind() kind {
	return kindDoFnMsec
}

// MsecValue is the value of a single msec metric.
type MsecValue struct {
	Start, Process, Finish, Total time.Duration
}

// PColValue is the value of a single PCollection metric.
type PColValue struct {
	ElementCount    int64
	SampledByteSize DistributionValue
}

// Results represents all metrics gathered during the job's execution.
// It allows for querying metrics using a provided filter.
type Results struct {
	counters      []CounterResult
	distributions []DistributionResult
	gauges        []GaugeResult
	msecs         []MsecResult
	pCols         []PColResult
}

// NewResults creates a new Results.
func NewResults(
	counters []CounterResult,
	distributions []DistributionResult,
	gauges []GaugeResult,
	msecs []MsecResult,
	pCols []PColResult) *Results {
	return &Results{counters, distributions, gauges, msecs, pCols}
}

// AllMetrics returns all metrics from a Results instance.
func (mr Results) AllMetrics() QueryResults {
	return QueryResults(mr)
}

// SingleResult interface facilitates metrics query filtering methods.
type SingleResult interface {
	Name() string
	Namespace() string
	Transform() string
}

// Query allows metrics querying with filter. The filter takes the form of predicate function. Example:
//
//	qr = pr.Metrics().Query(func(mr beam.MetricResult) bool {
//	    return sr.Namespace() == test.namespace
//	})
func (mr Results) Query(f func(SingleResult) bool) QueryResults {
	counters := []CounterResult{}
	distributions := []DistributionResult{}
	gauges := []GaugeResult{}
	msecs := []MsecResult{}
	pCols := []PColResult{}

	for _, counter := range mr.counters {
		if f(counter) {
			counters = append(counters, counter)
		}
	}
	for _, distribution := range mr.distributions {
		if f(distribution) {
			distributions = append(distributions, distribution)
		}
	}
	for _, gauge := range mr.gauges {
		if f(gauge) {
			gauges = append(gauges, gauge)
		}
	}
	for _, msec := range mr.msecs {
		if f(msec) {
			msecs = append(msecs, msec)
		}
	}
	for _, pCol := range mr.pCols {
		if f(pCol) {
			pCols = append(pCols, pCol)
		}
	}
	return QueryResults{counters: counters, distributions: distributions, gauges: gauges, msecs: msecs, pCols: pCols}
}

// QueryResults is the result of a query. Allows accessing all of the
// metrics that matched the filter.
type QueryResults struct {
	counters      []CounterResult
	distributions []DistributionResult
	gauges        []GaugeResult
	msecs         []MsecResult
	pCols         []PColResult
}

// Counters returns a slice of counter metrics.
func (qr QueryResults) Counters() []CounterResult {
	out := make([]CounterResult, len(qr.counters))
	copy(out, qr.counters)
	return out
}

// Distributions returns a slice of distribution metrics.
func (qr QueryResults) Distributions() []DistributionResult {
	out := make([]DistributionResult, len(qr.distributions))
	copy(out, qr.distributions)
	return out
}

// Gauges returns a slice of gauge metrics.
func (qr QueryResults) Gauges() []GaugeResult {
	out := make([]GaugeResult, len(qr.gauges))
	copy(out, qr.gauges)
	return out
}

// Msecs returns a slice of DoFn metrics
func (qr QueryResults) Msecs() []MsecResult {
	out := make([]MsecResult, len(qr.msecs))
	copy(out, qr.msecs)
	return out
}

// PCols returns a slice of PCollection metrics.
func (qr QueryResults) PCols() []PColResult {
	out := make([]PColResult, len(qr.pCols))
	copy(out, qr.pCols)
	return out
}

// CounterResult is an attempted and a commited value of a counter metric plus
// key.
type CounterResult struct {
	Attempted, Committed int64
	Key                  StepKey
}

// Result returns committed metrics. Falls back to attempted metrics if committed
// are not populated (e.g. due to not being supported on a given runner).
func (r CounterResult) Result() int64 {
	if r.Committed != 0 {
		return r.Committed
	}
	return r.Attempted
}

// Name returns the Name of this Counter.
func (r CounterResult) Name() string {
	return r.Key.Name
}

// Namespace returns the Namespace of this Counter.
func (r CounterResult) Namespace() string {
	return r.Key.Namespace
}

// Transform returns the Transform step for this CounterResult.
func (r CounterResult) Transform() string { return r.Key.Step }

// MergeCounters combines counter metrics that share a common key.
func MergeCounters(
	attempted map[StepKey]int64,
	committed map[StepKey]int64) []CounterResult {
	res := make([]CounterResult, 0)
	merged := map[StepKey]CounterResult{}

	for k, v := range attempted {
		merged[k] = CounterResult{Attempted: v, Key: k}
	}
	for k, v := range committed {
		m, ok := merged[k]
		if ok {
			merged[k] = CounterResult{Attempted: m.Attempted, Committed: v, Key: k}
		} else {
			merged[k] = CounterResult{Committed: v, Key: k}
		}
	}

	for _, v := range merged {
		res = append(res, v)
	}
	return res
}

// DistributionResult is an attempted and a commited value of a distribution
// metric plus key.
type DistributionResult struct {
	Attempted, Committed DistributionValue
	Key                  StepKey
}

// Result returns committed metrics. Falls back to attempted metrics if committed
// are not populated (e.g. due to not being supported on a given runner).
func (r DistributionResult) Result() DistributionValue {
	empty := DistributionValue{}
	if r.Committed != empty {
		return r.Committed
	}
	return r.Attempted
}

// Name returns the Name of this Distribution.
func (r DistributionResult) Name() string {
	return r.Key.Name
}

// Namespace returns the Namespace of this Distribution.
func (r DistributionResult) Namespace() string {
	return r.Key.Namespace
}

// Transform returns the Transform step for this DistributionResult.
func (r DistributionResult) Transform() string { return r.Key.Step }

// MergeDistributions combines distribution metrics that share a common key.
func MergeDistributions(
	attempted map[StepKey]DistributionValue,
	committed map[StepKey]DistributionValue) []DistributionResult {
	res := make([]DistributionResult, 0)
	merged := map[StepKey]DistributionResult{}

	for k, v := range attempted {
		merged[k] = DistributionResult{Attempted: v, Key: k}
	}
	for k, v := range committed {
		m, ok := merged[k]
		if ok {
			merged[k] = DistributionResult{Attempted: m.Attempted, Committed: v, Key: k}
		} else {
			merged[k] = DistributionResult{Committed: v, Key: k}
		}
	}

	for _, v := range merged {
		res = append(res, v)
	}
	return res
}

// GaugeResult is an attempted and a commited value of a gauge metric plus
// key.
type GaugeResult struct {
	Attempted, Committed GaugeValue
	Key                  StepKey
}

// Result returns committed metrics. Falls back to attempted metrics if committed
// are not populated (e.g. due to not being supported on a given runner).
func (r GaugeResult) Result() GaugeValue {
	empty := GaugeValue{}
	if r.Committed != empty {
		return r.Committed
	}
	return r.Attempted
}

// Name returns the Name of this Gauge.
func (r GaugeResult) Name() string {
	return r.Key.Name
}

// Namespace returns the Namespace of this Gauge.
func (r GaugeResult) Namespace() string {
	return r.Key.Namespace
}

// Transform returns the Transform step for this GaugeResult.
func (r GaugeResult) Transform() string { return r.Key.Step }

// PColResult is an attempted and a commited value of a pcollection
// metric plus key.
type PColResult struct {
	Attempted, Committed PColValue
	Key                  StepKey
}

// Result returns committed metrics. Falls back to attempted metrics if committed
// are not populated (e.g. due to not being supported on a given runner).
func (r PColResult) Result() PColValue {
	empty := PColValue{}
	if r.Committed != empty {
		return r.Committed
	}
	return r.Attempted
}

// Name returns the Name of this Pcollection Result.
func (r PColResult) Name() string {
	return ""
}

// Namespace returns the Namespace of this Pcollection Result.
func (r PColResult) Namespace() string {
	return ""
}

// Transform returns the Transform step for this Pcollection Result.
func (r PColResult) Transform() string { return r.Key.Step }

// MergePCols combines pcollection metrics that share a common key.
func MergePCols(
	attempted map[StepKey]PColValue,
	committed map[StepKey]PColValue) []PColResult {
	res := make([]PColResult, 0)
	merged := map[StepKey]PColResult{}

	for k, v := range attempted {
		merged[k] = PColResult{Attempted: v, Key: k}
	}
	for k, v := range committed {
		m, ok := merged[k]
		if ok {
			merged[k] = PColResult{Attempted: m.Attempted, Committed: v, Key: k}
		} else {
			merged[k] = PColResult{Committed: v, Key: k}
		}
	}

	for _, v := range merged {
		res = append(res, v)
	}
	return res
}

// StepKey uniquely identifies a metric within a pipeline graph.
type StepKey struct {
	Step, Name, Namespace string
}

// MergeGauges combines gauge metrics that share a common key.
func MergeGauges(
	attempted map[StepKey]GaugeValue,
	committed map[StepKey]GaugeValue) []GaugeResult {
	res := make([]GaugeResult, 0)
	merged := map[StepKey]GaugeResult{}

	for k, v := range attempted {
		merged[k] = GaugeResult{Attempted: v, Key: k}
	}
	for k, v := range committed {
		m, ok := merged[k]
		if ok {
			merged[k] = GaugeResult{Attempted: m.Attempted, Committed: v, Key: k}
		} else {
			merged[k] = GaugeResult{Committed: v, Key: k}
		}
	}

	for _, v := range merged {
		res = append(res, v)
	}
	return res
}

// MsecResult is an attempted and a commited value of a counter metric plus key.
type MsecResult struct {
	Attempted, Committed MsecValue
	Key                  StepKey
}

// Result returns committed metrics. Falls back to attempted metrics if committed
// are not populated (e.g. due to not being supported on a given runner).
func (r MsecResult) Result() MsecValue {
	if r.Committed != (MsecValue{}) {
		return r.Committed
	}
	return r.Attempted
}

// Name returns the Name of this MsecResult.
func (r MsecResult) Name() string {
	return ""
}

// Namespace returns the Namespace of this MsecResult.
func (r MsecResult) Namespace() string {
	return ""
}

// Transform returns the Transform step for this MsecResult.
func (r MsecResult) Transform() string { return r.Key.Step }

// MergeMsecs combines counter metrics that share a common key.
func MergeMsecs(
	attempted map[StepKey]MsecValue,
	committed map[StepKey]MsecValue) []MsecResult {
	res := make([]MsecResult, 0)
	merged := map[StepKey]MsecResult{}

	for k, v := range attempted {
		merged[k] = MsecResult{Attempted: v, Key: k}
	}
	for k, v := range committed {
		m, ok := merged[k]
		if ok {
			merged[k] = MsecResult{Attempted: m.Attempted, Committed: v, Key: k}
		} else {
			merged[k] = MsecResult{Committed: v, Key: k}
		}
	}

	for _, v := range merged {
		res = append(res, v)
	}
	return res
}

// ResultsExtractor extracts the metrics.Results from Store using ctx.
// This is same as what metrics.dumperExtractor and metrics.dumpTo would do together.
func ResultsExtractor(ctx context.Context) Results {
	store := GetStore(ctx)
	m := make(map[Labels]any)
	e := &Extractor{
		SumInt64: func(l Labels, v int64) {
			m[l] = &counter{value: v}
		},
		DistributionInt64: func(l Labels, count, sum, min, max int64) {
			m[l] = &distribution{count: count, sum: sum, min: min, max: max}
		},
		GaugeInt64: func(l Labels, v int64, t time.Time) {
			m[l] = &gauge{v: v, t: t}
		},
		MsecsInt64: func(labels string, e *[4]ExecutionState) {
			m[PTransformLabels(labels)] = &executionState{state: e}
		},
	}
	e.ExtractFrom(store)

	var ls []Labels
	for l := range m {
		ls = append(ls, l)
	}

	sort.Slice(ls, func(i, j int) bool {
		if ls[i].transform < ls[j].transform {
			return true
		}
		tEq := ls[i].transform == ls[j].transform
		if tEq && ls[i].namespace < ls[j].namespace {
			return true
		}
		nsEq := ls[i].namespace == ls[j].namespace
		if tEq && nsEq && ls[i].name < ls[j].name {
			return true
		}
		return false
	})

	r := Results{counters: []CounterResult{}, distributions: []DistributionResult{}, gauges: []GaugeResult{}, msecs: []MsecResult{}}
	for _, l := range ls {
		key := StepKey{Step: l.transform, Name: l.name, Namespace: l.namespace}
		switch opt := m[l]; opt.(type) {
		case *counter:
			attempted := make(map[StepKey]int64)
			committed := make(map[StepKey]int64)
			attempted[key] = 0
			committed[key] = opt.(*counter).value
			r.counters = append(r.counters, MergeCounters(attempted, committed)...)
		case *distribution:
			attempted := make(map[StepKey]DistributionValue)
			committed := make(map[StepKey]DistributionValue)
			attempted[key] = DistributionValue{}
			committed[key] = DistributionValue{opt.(*distribution).count, opt.(*distribution).sum, opt.(*distribution).min, opt.(*distribution).max}
			r.distributions = append(r.distributions, MergeDistributions(attempted, committed)...)
		case *gauge:
			attempted := make(map[StepKey]GaugeValue)
			committed := make(map[StepKey]GaugeValue)
			attempted[key] = GaugeValue{}
			committed[key] = GaugeValue{opt.(*gauge).v, opt.(*gauge).t}
			r.gauges = append(r.gauges, MergeGauges(attempted, committed)...)
		case *executionState:
			attempted := make(map[StepKey]MsecValue)
			committed := make(map[StepKey]MsecValue)
			attempted[key] = MsecValue{}
			es := opt.(*executionState).state
			committed[key] = MsecValue{Start: es[0].TotalTime, Process: es[1].TotalTime, Finish: es[2].TotalTime, Total: es[3].TotalTime}
			r.msecs = append(r.msecs, MergeMsecs(attempted, committed)...)
		}
	}
	return r
}
