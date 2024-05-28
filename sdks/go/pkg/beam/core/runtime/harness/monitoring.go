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

package harness

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/metricsx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

type shortKey struct {
	metrics.Labels
	Urn metricsx.Urn // Urns fully specify their type.
}

// shortIDCache retains lookup caches for short ids to the full monitoring
// info metadata.
//
// TODO: 2020/03/26 - measure mutex overhead vs sync.Map for this case.
// sync.Map might have lower contention for this read heavy load.
type shortIDCache struct {
	mu              sync.Mutex
	labels2ShortIds map[shortKey]string
	shortIds2Infos  map[string]*pipepb.MonitoringInfo

	lastShortID int64
}

func newShortIDCache() *shortIDCache {
	return &shortIDCache{
		labels2ShortIds: make(map[shortKey]string),
		shortIds2Infos:  make(map[string]*pipepb.MonitoringInfo),
	}
}

func (c *shortIDCache) getNextShortID() string {
	id := atomic.AddInt64(&c.lastShortID, 1)
	// No reason not to use the smallest string short ids possible.
	return strconv.FormatInt(id, 36)
}

// getShortID returns the short id for the given metric, and if
// it doesn't exist yet, stores the metadata.
// Assumes c.mu lock is held.
func (c *shortIDCache) getShortID(l metrics.Labels, urn metricsx.Urn) string {
	k := shortKey{l, urn}
	s, ok := c.labels2ShortIds[k]
	if ok {
		return s
	}
	s = c.getNextShortID()
	c.labels2ShortIds[k] = s
	c.shortIds2Infos[s] = &pipepb.MonitoringInfo{
		Urn:    metricsx.UrnToString(urn),
		Type:   metricsx.UrnToType(urn),
		Labels: l.Map(),
	}
	return s
}

func (c *shortIDCache) shortIdsToInfos(shortids []string) map[string]*pipepb.MonitoringInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := make(map[string]*pipepb.MonitoringInfo, len(shortids))
	for _, s := range shortids {
		m[s] = c.shortIds2Infos[s]
	}
	return m
}

// Convenience package functions for production.
var defaultShortIDCache *shortIDCache

func init() {
	defaultShortIDCache = newShortIDCache()
}

func getShortID(l metrics.Labels, urn metricsx.Urn) string {
	return defaultShortIDCache.getShortID(l, urn)
}

func shortIdsToInfos(shortids []string) map[string]*pipepb.MonitoringInfo {
	return defaultShortIDCache.shortIdsToInfos(shortids)
}

func monitoring(p *exec.Plan, store *metrics.Store, supportShortID bool) ([]*pipepb.MonitoringInfo, map[string][]byte, bool) {
	if store == nil {
		return nil, nil, false
	}
	defaultShortIDCache.mu.Lock()
	defer defaultShortIDCache.mu.Unlock()

	var monitoringInfo []*pipepb.MonitoringInfo
	payloads := make(map[string][]byte)
	metrics.Extractor{
		SumInt64: func(l metrics.Labels, v int64) {
			payload, err := metricsx.Int64Counter(v)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(l, metricsx.UrnUserSumInt64)] = payload
			if !supportShortID {
				monitoringInfo = append(monitoringInfo,
					&pipepb.MonitoringInfo{
						Urn:     metricsx.UrnToString(metricsx.UrnUserSumInt64),
						Type:    metricsx.UrnToType(metricsx.UrnUserSumInt64),
						Labels:  l.Map(),
						Payload: payload,
					})
			}
		},
		DistributionInt64: func(l metrics.Labels, count, sum, min, max int64) {
			payload, err := metricsx.Int64Distribution(count, sum, min, max)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(l, metricsx.UrnUserDistInt64)] = payload
			if !supportShortID {
				monitoringInfo = append(monitoringInfo,
					&pipepb.MonitoringInfo{
						Urn:     metricsx.UrnToString(metricsx.UrnUserDistInt64),
						Type:    metricsx.UrnToType(metricsx.UrnUserDistInt64),
						Labels:  l.Map(),
						Payload: payload,
					})
			}
		},
		GaugeInt64: func(l metrics.Labels, v int64, t time.Time) {
			payload, err := metricsx.Int64Latest(t, v)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(l, metricsx.UrnUserLatestMsInt64)] = payload
			if !supportShortID {
				monitoringInfo = append(monitoringInfo,
					&pipepb.MonitoringInfo{
						Urn:     metricsx.UrnToString(metricsx.UrnUserLatestMsInt64),
						Type:    metricsx.UrnToType(metricsx.UrnUserLatestMsInt64),
						Labels:  l.Map(),
						Payload: payload,
					})
			}
		},
		MsecsInt64: func(l string, states *[4]metrics.ExecutionState) {
			label := map[string]string{"PTRANSFORM": l}
			for i, v := range states {
				payload, err := metricsx.Int64Counter(int64(v.TotalTime) / int64(time.Millisecond))
				if err != nil {
					panic(err)
				}
				ul := metricsx.ExecutionMsecUrn(i)
				payloads[getShortID(metrics.PTransformLabels(l), ul)] = payload
				if !supportShortID {
					monitoringInfo = append(monitoringInfo,
						&pipepb.MonitoringInfo{
							Urn:     metricsx.UrnToString(ul),
							Type:    metricsx.UrnToType(ul),
							Labels:  label,
							Payload: payload,
						})
				}
			}
		},
	}.ExtractFrom(store)

	// Get the execution monitoring information from the bundle plan.

	snapshot, ok := p.Progress()
	if !ok {
		return monitoringInfo, payloads, false
	}
	for _, pcol := range snapshot.PCols {
		payload, err := metricsx.Int64Counter(pcol.ElementCount)
		if err != nil {
			panic(err)
		}

		// TODO(https://github.com/apache/beam/issues/20204): This metric should account for elements in multiple windows.
		payloads[getShortID(metrics.PCollectionLabels(pcol.ID), metricsx.UrnElementCount)] = payload

		if !supportShortID {
			monitoringInfo = append(monitoringInfo,
				&pipepb.MonitoringInfo{
					Urn:  metricsx.UrnToString(metricsx.UrnElementCount),
					Type: metricsx.UrnToType(metricsx.UrnElementCount),
					Labels: map[string]string{
						"PCOLLECTION": pcol.ID,
					},
					Payload: payload,
				})
		}
		// Skip pcollections without size
		if pcol.SizeCount != 0 {
			payload, err := metricsx.Int64Distribution(pcol.SizeCount, pcol.SizeSum, pcol.SizeMin, pcol.SizeMax)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(metrics.PCollectionLabels(pcol.ID), metricsx.UrnSampledByteSize)] = payload

			if !supportShortID {
				monitoringInfo = append(monitoringInfo,
					&pipepb.MonitoringInfo{
						Urn:  metricsx.UrnToString(metricsx.UrnSampledByteSize),
						Type: metricsx.UrnToType(metricsx.UrnSampledByteSize),
						Labels: map[string]string{
							"PCOLLECTION": pcol.ID,
						},
						Payload: payload,
					})
			}
		}
	}

	payload, err := metricsx.Int64Counter(snapshot.Source.Count)
	if err != nil {
		panic(err)
	}

	payloads[getShortID(metrics.PTransformLabels(snapshot.Source.ID), metricsx.UrnDataChannelReadIndex)] = payload
	if !supportShortID {
		monitoringInfo = append(monitoringInfo,
			&pipepb.MonitoringInfo{
				Urn:  metricsx.UrnToString(metricsx.UrnDataChannelReadIndex),
				Type: metricsx.UrnToType(metricsx.UrnDataChannelReadIndex),
				Labels: map[string]string{
					"PTRANSFORM": snapshot.Source.ID,
				},
				Payload: payload,
			})
	}
	return monitoringInfo, payloads, snapshot.Source.ConsumingReceivedData
}
