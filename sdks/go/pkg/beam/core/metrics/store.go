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

package metrics

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Implementation note: We avoid depending on the FnAPI protos here
// so we can provide a clean abstraction break for users, and avoid
// problems if the FnAPI metrics protos need to change.

// Labels provide the context for the given metric.
type Labels struct {
	transform, namespace, name string
	pcollection                string
}

// Transform returns the transform context for this metric, if available.
func (l Labels) Transform() string { return l.transform }

// Namespace returns the namespace context for this metric.
func (l Labels) Namespace() string { return l.namespace }

// Name returns the name for this metric.
func (l Labels) Name() string { return l.name }

// UserLabels builds a Labels for user metrics.
// Intended for framework use.
func UserLabels(transform, namespace, name string) Labels {
	return Labels{transform: transform, namespace: namespace, name: name}
}

// PCollectionLabels builds a Labels for pcollection metrics.
// Intended for framework use.
func PCollectionLabels(pcollection string) Labels {
	return Labels{pcollection: pcollection}
}

// PCollection returns the PCollection id for this metric.
func (l Labels) PCollection() string { return l.pcollection }

// PTransformLabels builds a Labels for transform metrics.
// Intended for framework use.
func PTransformLabels(transform string) Labels {
	return Labels{transform: transform}
}

// Map produces a map of present labels to their values.
//
// Returns nil map if invalid.
func (l Labels) Map() map[string]string {
	if l.transform != "" {
		return map[string]string{
			"PTRANSFORM": l.transform,
			"NAMESPACE":  l.namespace,
			"NAME":       l.name,
		}
	}
	if l.pcollection != "" {
		return map[string]string{
			"PCOLLECTION": l.pcollection,
		}
	}
	return nil
}

// Extractor allows users to access metrics programatically after
// pipeline completion. Users assign functions to fields that
// interest them, and that function is called for each metric
// of the associated kind.
type Extractor struct {
	// SumInt64 extracts data from Sum Int64 counters.
	SumInt64 func(labels Labels, v int64)
	// DistributionInt64 extracts data from Distribution Int64 counters.
	DistributionInt64 func(labels Labels, count, sum, min, max int64)
	// GaugeInt64 extracts data from Gauge Int64 counters.
	GaugeInt64 func(labels Labels, v int64, t time.Time)
	// MsecsInt64 extracts data from StateRegistry of ExecutionState
	MsecsInt64 func(labels string, e [4]*ExecutionState)
}

// ExtractFrom the given metrics Store all the metrics for
// populated function fields.
// Returns an error if no fields were set.
func (e Extractor) ExtractFrom(store *Store) error {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if e.SumInt64 == nil && e.DistributionInt64 == nil && e.GaugeInt64 == nil {
		return fmt.Errorf("no Extractor fields were set")
	}

	for l, um := range store.store {
		switch um.kind() {
		case kindSumCounter:
			if e.SumInt64 != nil {
				data := um.(*counter).get()
				e.SumInt64(l, data)
			}
		case kindDistribution:
			if e.DistributionInt64 != nil {
				count, sum, min, max := um.(*distribution).get()
				e.DistributionInt64(l, count, sum, min, max)
			}
		case kindGauge:
			if e.GaugeInt64 != nil {
				v, t := um.(*gauge).get()
				e.GaugeInt64(l, v, t)
			}
		}
	}
	for l, um := range store.stateRegistry {
		e.MsecsInt64(l, um)
	}
	return nil
}

// userMetric knows what kind it is.
type userMetric interface {
	kind() kind
}

type nameHash uint64

// ptCounterSet is the internal tracking struct for a single ptransform
// in a single bundle for all counter types.
type ptCounterSet struct {
	pid string
	// We store the user path access to the cells in metric type segregated
	// maps. At present, caching the name hash, with the name in each proxy
	// avoids the expense of re-hashing on every use.
	counters      map[nameHash]*counter
	distributions map[nameHash]*distribution
	gauges        map[nameHash]*gauge
}

// Bundle processing state (START_BUNDLE, PROCESS_BUNDLE, FINISH_BUNDLE)
type bundleProcState int

const (
	StartBundle   bundleProcState = 0
	ProcessBundle bundleProcState = 1
	FinishBundle  bundleProcState = 2
	TotalBundle   bundleProcState = 3
)

// ExecutionState stores the information about a bundle in a particular state.
type ExecutionState struct {
	State        bundleProcState
	IsProcessing bool // set to true when sent as a response to ProcessBundleProgress Request
	TotalTime    time.Duration
}

// BundleState stores information about a PTransform for execution time metrics.
type BundleState struct {
	pid          string
	currentState bundleProcState
}

// SetPTransformState stores the state of PTransform in its bundle.
func SetPTransformState(ctx context.Context, state bundleProcState) {
	if bctx, ok := ctx.(*beamCtx); ok {
		pid := bctx.ptransformID
		bctx.store.states[state].pid = pid
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&bctx.store.bundleState)), unsafe.Pointer(&bctx.store.states[state]))
		atomic.AddInt64(&bctx.store.transitions, 1)
	}
}

// currentStateVal exports the current state of a bundle wrt PTransform.
type currentStateVal struct {
	pid         string
	state       bundleProcState
	transitions int64
}

// Store retains per transform countersets, intended for per bundle use.
type Store struct {
	mu            sync.RWMutex
	css           []*ptCounterSet
	stateRegistry map[string][4]*ExecutionState

	store map[Labels]userMetric

	transitions int64
	bundleState BundleState
	states      [3]BundleState
}

func newStore() *Store {
	return &Store{store: make(map[Labels]userMetric), stateRegistry: make(map[string][4]*ExecutionState)}
}

// storeMetric stores a metric away on its first use so it may be retrieved later on.
// In the event of a name collision, storeMetric can panic, so it's prudent to release
// locks if they are no longer required.
func (b *Store) storeMetric(pid string, n name, m userMetric) {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := Labels{transform: pid, namespace: n.namespace, name: n.name}
	if ms, ok := b.store[l]; ok {
		if ms.kind() != m.kind() {
			panic(fmt.Sprintf("metric name %s being reused for a different metric type in a single PTransform", n))
		}
		return
	}
	b.store[l] = m
}
