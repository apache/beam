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
	"fmt"
	"strings"
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

	// MsecsInt64 extracts data from StateRegistry of ExecutionState.
	// Extraction of Msec counters is experimental and subject to change.
	MsecsInt64 func(labels string, e *[4]ExecutionState)
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
	if e.MsecsInt64 != nil {
		for l, es := range store.stateRegistry {
			e.MsecsInt64(l, es)
		}
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

type bundleProcState int

// String implements the Stringer interface.
func (b bundleProcState) String() string {
	switch b {
	case StartBundle:
		return "START_BUNDLE"
	case ProcessBundle:
		return "PROCESS_BUNDLE"
	case FinishBundle:
		return "FINISH_BUNDLE"
	case TotalBundle:
		return "TOTAL_BUNDLE"
	default:
		return "unknown process bundle state!"
	}
}

const (
	// StartBundle indicates starting state of a bundle
	StartBundle bundleProcState = 0
	// ProcessBundle indicates processing state of a bundle
	ProcessBundle bundleProcState = 1
	// FinishBundle indicates finishing state of a bundle
	FinishBundle bundleProcState = 2
	// TotalBundle (not a state) used for aggregating above states of a bundle
	TotalBundle bundleProcState = 3
)

// ExecutionState stores the information about a bundle in a particular state.
type ExecutionState struct {
	State        bundleProcState
	IsProcessing bool // set to true when sent as a response to ProcessBundleProgress Request
	TotalTime    time.Duration
}

// String implements the Stringer interface.
func (e ExecutionState) String() string {
	return fmt.Sprintf("Execution State:\n\t State: %s\n\t IsProcessing: %v\n\t Total time: %v\n", e.State, e.IsProcessing, e.TotalTime)
}

// BundleState stores information about a PTransform for execution time metrics.
type BundleState struct {
	pid          string
	currentState bundleProcState
}

// String implements the Stringer interface.
func (b BundleState) String() string {
	return fmt.Sprintf("Bundle State:\n\t PTransform ID: %s\n\t Current state: %s", b.pid, b.currentState)
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
	stateRegistry map[string]*[4]ExecutionState

	store map[Labels]userMetric

	transitions *int64
	bundleState *BundleState
}

func newStore() *Store {
	return &Store{store: make(map[Labels]userMetric), stateRegistry: make(map[string]*[4]ExecutionState), transitions: new(int64), bundleState: &BundleState{}}
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

// BundleState returns the bundle state.
func (b *Store) BundleState() string {
	bs := *(*BundleState)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&b.bundleState))))
	return bs.String()
}

// StateRegistry returns the state registry that stores bundleID to executions states mapping.
func (b *Store) StateRegistry() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	builder := &strings.Builder{}
	builder.WriteString("\n | All Bundle Process States | \n")
	for bundleID, state := range b.stateRegistry {
		builder.WriteString(fmt.Sprintf("\tBundle ID: %s\n", bundleID))
		for i := 0; i < 4; i++ {
			builder.WriteString(fmt.Sprintf("\t%s\n", state[i]))
		}
	}
	return builder.String()
}
