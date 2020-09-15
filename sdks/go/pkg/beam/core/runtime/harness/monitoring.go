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
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

type mUrn uint32

// TODO: Pull these from the protos.
var sUrns = [...]string{
	"beam:metric:user:sum_int64:v1",
	"beam:metric:user:sum_double:v1",
	"beam:metric:user:distribution_int64:v1",
	"beam:metric:user:distribution_double:v1",
	"beam:metric:user:latest_int64:v1",
	"beam:metric:user:latest_double:v1",
	"beam:metric:user:top_n_int64:v1",
	"beam:metric:user:top_n_double:v1",
	"beam:metric:user:bottom_n_int64:v1",
	"beam:metric:user:bottom_n_double:v1",

	"beam:metric:element_count:v1",
	"beam:metric:sampled_byte_size:v1",

	"beam:metric:pardo_execution_time:start_bundle_msecs:v1",
	"beam:metric:pardo_execution_time:process_bundle_msecs:v1",
	"beam:metric:pardo_execution_time:finish_bundle_msecs:v1",
	"beam:metric:ptransform_execution_time:total_msecs:v1",

	"beam:metric:ptransform_progress:remaining:v1",
	"beam:metric:ptransform_progress:completed:v1",
	"beam:metric:data_channel:read_index:v1",

	"TestingSentinelUrn", // Must remain last.
}

const (
	urnUserSumInt64 mUrn = iota
	urnUserSumFloat64
	urnUserDistInt64
	urnUserDistFloat64
	urnUserLatestMsInt64
	urnUserLatestMsFloat64
	urnUserTopNInt64
	urnUserTopNFloat64
	urnUserBottomNInt64
	urnUserBottomNFloat64

	urnElementCount
	urnSampledByteSize

	urnStartBundle
	urnProcessBundle
	urnFinishBundle
	urnTransformTotalTime

	urnProgressRemaining
	urnProgressCompleted
	urnDataChannelReadIndex

	urnTestSentinel // Must remain last.
)

// urnToType maps the urn to it's encoding type.
// This function is written to be inlinable by the compiler.
func urnToType(u mUrn) string {
	switch u {
	case urnUserSumInt64, urnElementCount, urnStartBundle, urnProcessBundle, urnFinishBundle, urnTransformTotalTime:
		return "beam:metrics:sum_int64:v1"
	case urnUserSumFloat64:
		return "beam:metrics:sum_double:v1"
	case urnUserDistInt64, urnSampledByteSize:
		return "beam:metrics:distribution_int64:v1"
	case urnUserDistFloat64:
		return "beam:metrics:distribution_double:v1"
	case urnUserLatestMsInt64:
		return "beam:metrics:latest_int64:v1"
	case urnUserLatestMsFloat64:
		return "beam:metrics:latest_double:v1"
	case urnUserTopNInt64:
		return "beam:metrics:top_n_int64:v1"
	case urnUserTopNFloat64:
		return "beam:metrics:top_n_double:v1"
	case urnUserBottomNInt64:
		return "beam:metrics:bottom_n_int64:v1"
	case urnUserBottomNFloat64:
		return "beam:metrics:bottom_n_double:v1"

	case urnProgressRemaining, urnProgressCompleted:
		return "beam:metrics:progress:v1"
	case urnDataChannelReadIndex:
		return "beam:metrics:sum_int64:v1"

	// Monitoring Table isn't currently in the protos.
	// case ???:
	//	return "beam:metrics:monitoring_table:v1"

	case urnTestSentinel:
		return "TestingSentinelType"

	default:
		panic("metric urn without specified type" + sUrns[u])
	}
}

type shortKey struct {
	metrics.Labels
	Urn mUrn // Urns fully specify their type.
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
func (c *shortIDCache) getShortID(l metrics.Labels, urn mUrn) string {
	k := shortKey{l, urn}
	s, ok := c.labels2ShortIds[k]
	if ok {
		return s
	}
	s = c.getNextShortID()
	c.labels2ShortIds[k] = s
	c.shortIds2Infos[s] = &pipepb.MonitoringInfo{
		Urn:    sUrns[urn],
		Type:   urnToType(urn),
		Labels: userLabels(l),
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

func getShortID(l metrics.Labels, urn mUrn) string {
	return defaultShortIDCache.getShortID(l, urn)
}

func shortIdsToInfos(shortids []string) map[string]*pipepb.MonitoringInfo {
	return defaultShortIDCache.shortIdsToInfos(shortids)
}

func monitoring(p *exec.Plan) ([]*pipepb.MonitoringInfo, map[string][]byte) {
	store := p.Store()
	if store == nil {
		return nil, nil
	}

	defaultShortIDCache.mu.Lock()
	defer defaultShortIDCache.mu.Unlock()

	var monitoringInfo []*pipepb.MonitoringInfo
	payloads := make(map[string][]byte)
	metrics.Extractor{
		SumInt64: func(l metrics.Labels, v int64) {
			payload, err := int64Counter(v)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(l, urnUserSumInt64)] = payload

			monitoringInfo = append(monitoringInfo,
				&pipepb.MonitoringInfo{
					Urn:     sUrns[urnUserSumInt64],
					Type:    urnToType(urnUserSumInt64),
					Labels:  userLabels(l),
					Payload: payload,
				})
		},
		DistributionInt64: func(l metrics.Labels, count, sum, min, max int64) {
			payload, err := int64Distribution(count, sum, min, max)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(l, urnUserDistInt64)] = payload

			monitoringInfo = append(monitoringInfo,
				&pipepb.MonitoringInfo{
					Urn:     sUrns[urnUserDistInt64],
					Type:    urnToType(urnUserDistInt64),
					Labels:  userLabels(l),
					Payload: payload,
				})
		},
		GaugeInt64: func(l metrics.Labels, v int64, t time.Time) {
			payload, err := int64Latest(t, v)
			if err != nil {
				panic(err)
			}
			payloads[getShortID(l, urnUserLatestMsInt64)] = payload

			monitoringInfo = append(monitoringInfo,
				&pipepb.MonitoringInfo{
					Urn:     sUrns[urnUserLatestMsInt64],
					Type:    urnToType(urnUserLatestMsInt64),
					Labels:  userLabels(l),
					Payload: payload,
				})

		},
	}.ExtractFrom(store)

	// Get the execution monitoring information from the bundle plan.
	if snapshot, ok := p.Progress(); ok {
		payload, err := int64Counter(snapshot.Count)
		if err != nil {
			panic(err)
		}

		// TODO(BEAM-9934): This metric should account for elements in multiple windows.
		payloads[getShortID(metrics.PCollectionLabels(snapshot.PID), urnElementCount)] = payload
		monitoringInfo = append(monitoringInfo,
			&pipepb.MonitoringInfo{
				Urn:  sUrns[urnElementCount],
				Type: urnToType(urnElementCount),
				Labels: map[string]string{
					"PCOLLECTION": snapshot.PID,
				},
				Payload: payload,
			})

		payloads[getShortID(metrics.PTransformLabels(snapshot.ID), urnDataChannelReadIndex)] = payload
		monitoringInfo = append(monitoringInfo,
			&pipepb.MonitoringInfo{
				Urn:  sUrns[urnDataChannelReadIndex],
				Type: urnToType(urnDataChannelReadIndex),
				Labels: map[string]string{
					"PTRANSFORM": snapshot.ID,
				},
				Payload: payload,
			})
	}

	return monitoringInfo,
		payloads
}

func userLabels(l metrics.Labels) map[string]string {
	return map[string]string{
		"PTRANSFORM": l.Transform(),
		"NAMESPACE":  l.Namespace(),
		"NAME":       l.Name(),
	}
}

func int64Counter(v int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func int64Latest(t time.Time, v int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(mtime.FromTime(t).Milliseconds(), &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func int64Distribution(count, sum, min, max int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(count, &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(sum, &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(min, &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(max, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
