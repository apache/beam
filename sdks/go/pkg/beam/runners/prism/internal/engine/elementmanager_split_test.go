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

package engine

import (
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// splitInsideKey returns a split point that keeps the first keep elements of
// key in the primary and returns the remaining elements of key, and every
// element after them, as the residual.
func splitInsideKey(key string, keep int) func(t *testing.T, bundleKeys []string) int {
	return func(t *testing.T, bundleKeys []string) int {
		t.Helper()
		for i, k := range bundleKeys {
			if k == key {
				return i + keep
			}
		}
		t.Fatalf("key %q not in bundle %v", key, bundleKeys)
		return 0
	}
}

// splitAfterFirstKey returns a split point that keeps exactly the elements of
// the first key of the bundle in the primary.
func splitAfterFirstKey(t *testing.T, bundleKeys []string) int {
	t.Helper()
	i := 0
	for i < len(bundleKeys) && bundleKeys[i] == bundleKeys[0] {
		i++
	}
	if i == len(bundleKeys) {
		t.Fatalf("bundle %v has a single key, want at least two", bundleKeys)
	}
	return i
}

// TestElementManager_SplitStatefulBundle checks that a channel split of a
// stateful stage's bundle keeps every key held by a primary element assigned
// to the primary bundle, so no other bundle processes that key concurrently.
//
// Prism gives each bundle a copy of the key's state at bundle start and
// replaces the key's state with the bundle's copy at commit. If two bundles
// process the same key at the same time, the last commit discards the state
// changes of the other.
func TestElementManager_SplitStatefulBundle(t *testing.T) {
	tests := []struct {
		name string
		// keys has one entry per input element, all in the global window.
		keys []string
		// firstResidual picks the split point from the bundle's key order,
		// which depends on map iteration order.
		firstResidual func(t *testing.T, bundleKeys []string) int
	}{
		{
			name:          "split inside the only key",
			keys:          []string{"K", "K", "K"},
			firstResidual: splitInsideKey("K", 1),
		}, {
			name:          "split inside one of two keys",
			keys:          []string{"A", "B", "B"},
			firstResidual: splitInsideKey("B", 1),
		}, {
			name:          "split between keys",
			keys:          []string{"A", "A", "B", "B"},
			firstResidual: splitAfterFirstKey,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			em := NewElementManager(Config{})
			em.AddStage("impulse", nil, []string{"input"}, nil)
			em.AddStage("stateful", []string{"input"}, nil, nil)
			em.StageStateful("stateful", nil)
			ss := em.stages["stateful"]

			var es []element
			for i, k := range test.keys {
				es = append(es, element{
					window:    window.GlobalWindow{},
					timestamp: mtime.Time(i),
					pane:      typex.NoFiringPane(),
					elmBytes:  []byte(fmt.Sprintf("%s%d", k, i)),
					keyBytes:  []byte(k),
					sequence:  i,
				})
			}
			em.addPending(ss.AddPending(em, es))

			var nextID int
			genBundID := func() string {
				defer func() { nextID++ }()
				return fmt.Sprintf("inst%03d", nextID)
			}
			// startBundle builds the next event time bundle, if any, and
			// returns it with the key of each of its elements.
			startBundle := func() (RunBundle, []string, bool) {
				bundID, ok, _, _ := ss.startEventTimeBundle(mtime.MaxTimestamp, genBundID)
				if !ok {
					return RunBundle{}, nil, false
				}
				var keys []string
				for _, e := range ss.inprogress[bundID].es {
					keys = append(keys, string(e.keyBytes))
				}
				return RunBundle{StageID: ss.ID, BundleID: bundID, Watermark: mtime.MaxTimestamp}, keys, true
			}

			primary, bundleKeys, ok := startBundle()
			if !ok {
				t.Fatal("startEventTimeBundle() built no bundle, want one")
			}
			if got, want := len(bundleKeys), len(test.keys); got != want {
				t.Fatalf("bundle %v has %v elements, want %v", bundleKeys, got, want)
			}
			split := test.firstResidual(t, bundleKeys)
			wantHeld := set[string]{}
			for _, k := range bundleKeys[:split] {
				wantHeld.insert(k)
			}

			em.ReturnResiduals(primary, split, PColInfo{}, Residuals{})

			checked := set[string]{}
			for _, k := range bundleKeys {
				if checked.present(k) {
					continue
				}
				checked.insert(k)
				want := wantHeld.present(k)
				if got := ss.inprogressKeys.present(k); got != want {
					t.Errorf("after split of %v at %v: key %v in inprogressKeys = %v, want %v", bundleKeys, split, k, got, want)
				}
				if got := ss.inprogressKeysByBundle[primary.BundleID].present(k); got != want {
					t.Errorf("after split of %v at %v: key %v in inprogressKeysByBundle[%v] = %v, want %v", bundleKeys, split, k, primary.BundleID, got, want)
				}
			}

			// While the primary runs, only residuals of released keys may
			// be scheduled.
			processed := split
			for {
				rb, keys, ok := startBundle()
				if !ok {
					break
				}
				for _, k := range keys {
					if wantHeld.present(k) {
						t.Fatalf("bundle %v has key %v while primary bundle %v still processes it", rb.BundleID, k, primary.BundleID)
					}
				}
				processed += len(keys)
				em.PersistBundle(rb, nil, TentativeData{}, PColInfo{}, Residuals{})
			}

			// Once the primary commits, the residuals of its keys run.
			em.PersistBundle(primary, nil, TentativeData{}, PColInfo{}, Residuals{})
			for {
				rb, keys, ok := startBundle()
				if !ok {
					break
				}
				processed += len(keys)
				em.PersistBundle(rb, nil, TentativeData{}, PColInfo{}, Residuals{})
			}

			if got, want := processed, len(test.keys); got != want {
				t.Errorf("processed %v elements, want %v", got, want)
			}
			if got := em.livePending.Load(); got != 0 {
				t.Errorf("live pending = %v after all bundles persisted, want 0", got)
			}
		})
	}
}
