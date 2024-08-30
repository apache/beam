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

package jobservices

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"math"
	"sort"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type labelsToKeyFunc func(string, map[string]string) metricKey

type urnOps struct {
	// keyFn produces the key for this metric from the labels.
	// based on the required label set for the metric from it's spec.
	keyFn labelsToKeyFunc
	// newAccum produces an accumulator assuming we don't have an accumulator for it already.
	// based on the type urn of the metric from it's spec.
	newAccum accumFactory
}

var (
	mUrn2Ops = map[string]urnOps{}
)

func init() {
	mUrn2Spec := map[string]*pipepb.MonitoringInfoSpec{}
	specs := (pipepb.MonitoringInfoSpecs_Enum)(0).Descriptor().Values()
	for i := 0; i < specs.Len(); i++ {
		enum := specs.ByNumber(protoreflect.EnumNumber(i))
		spec := proto.GetExtension(enum.Options(), pipepb.E_MonitoringInfoSpec).(*pipepb.MonitoringInfoSpec)
		mUrn2Spec[spec.GetUrn()] = spec
	}
	mUrn2Ops = buildUrnToOpsMap(mUrn2Spec)
}

// Should probably just construct a slice or map to get the urns out
// since we'll ultimately be using them a lot.
var metTyps = (pipepb.MonitoringInfoTypeUrns_Enum)(0).Descriptor().Values()

func getMetTyp(t pipepb.MonitoringInfoTypeUrns_Enum) string {
	return proto.GetExtension(metTyps.ByNumber(protoreflect.EnumNumber(t)).Options(), pipepb.E_BeamUrn).(string)
}

func buildUrnToOpsMap(mUrn2Spec map[string]*pipepb.MonitoringInfoSpec) map[string]urnOps {
	var hasher maphash.Hash

	props := (pipepb.MonitoringInfo_MonitoringInfoLabels)(0).Descriptor().Values()
	getProp := func(l pipepb.MonitoringInfo_MonitoringInfoLabels) string {
		return proto.GetExtension(props.ByNumber(protoreflect.EnumNumber(l)).Options(), pipepb.E_LabelProps).(*pipepb.MonitoringInfoLabelProps).GetName()
	}

	l2func := make(map[uint64]labelsToKeyFunc)
	labelsToKey := func(required []pipepb.MonitoringInfo_MonitoringInfoLabels, fn labelsToKeyFunc) {
		hasher.Reset()
		// We need the string versions of things to sort against
		// for consistent hashing.
		var req []string
		for _, l := range required {
			v := getProp(l)
			req = append(req, v)
		}
		sort.Strings(req)
		for _, v := range req {
			hasher.WriteString(v)
		}
		key := hasher.Sum64()
		l2func[key] = fn
	}
	ls := func(ls ...pipepb.MonitoringInfo_MonitoringInfoLabels) []pipepb.MonitoringInfo_MonitoringInfoLabels {
		return ls
	}

	ptransformLabel := getProp(pipepb.MonitoringInfo_TRANSFORM)
	namespaceLabel := getProp(pipepb.MonitoringInfo_NAMESPACE)
	nameLabel := getProp(pipepb.MonitoringInfo_NAME)
	pcollectionLabel := getProp(pipepb.MonitoringInfo_PCOLLECTION)
	statusLabel := getProp(pipepb.MonitoringInfo_STATUS)
	serviceLabel := getProp(pipepb.MonitoringInfo_SERVICE)
	resourceLabel := getProp(pipepb.MonitoringInfo_RESOURCE)
	methodLabel := getProp(pipepb.MonitoringInfo_METHOD)

	// Here's where we build the raw map from kinds of labels to the actual functions.
	labelsToKey(ls(pipepb.MonitoringInfo_TRANSFORM,
		pipepb.MonitoringInfo_NAMESPACE,
		pipepb.MonitoringInfo_NAME),
		func(urn string, labels map[string]string) metricKey {
			return userMetricKey{
				urn:        urn,
				ptransform: labels[ptransformLabel],
				namespace:  labels[namespaceLabel],
				name:       labels[nameLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_TRANSFORM),
		func(urn string, labels map[string]string) metricKey {
			return ptransformKey{
				urn:        urn,
				ptransform: labels[ptransformLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_PCOLLECTION),
		func(urn string, labels map[string]string) metricKey {
			return pcollectionKey{
				urn:         urn,
				pcollection: labels[pcollectionLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_SERVICE,
		pipepb.MonitoringInfo_METHOD,
		pipepb.MonitoringInfo_RESOURCE,
		pipepb.MonitoringInfo_TRANSFORM,
		pipepb.MonitoringInfo_STATUS),
		func(urn string, labels map[string]string) metricKey {
			return apiRequestKey{
				urn:        urn,
				service:    labels[serviceLabel],
				method:     labels[methodLabel],
				resource:   labels[resourceLabel],
				ptransform: labels[ptransformLabel],
				status:     labels[statusLabel],
			}
		})
	labelsToKey(ls(pipepb.MonitoringInfo_SERVICE,
		pipepb.MonitoringInfo_METHOD,
		pipepb.MonitoringInfo_RESOURCE,
		pipepb.MonitoringInfo_TRANSFORM),
		func(urn string, labels map[string]string) metricKey {
			return apiRequestLatenciesKey{
				urn:        urn,
				service:    labels[serviceLabel],
				method:     labels[methodLabel],
				resource:   labels[resourceLabel],
				ptransform: labels[ptransformLabel],
			}
		})

	// Specify accumulator decoders for all the metric types.
	// These are a combination of the decoder (accepting the payload bytes)
	// and represent how we hold onto them. Ultimately, these will also be
	// able to extract back out to the protos.

	typ2accumFac := map[string]accumFactory{
		getMetTyp(pipepb.MonitoringInfoTypeUrns_SUM_INT64_TYPE):  func() metricAccumulator { return &sumInt64{} },
		getMetTyp(pipepb.MonitoringInfoTypeUrns_SUM_DOUBLE_TYPE): func() metricAccumulator { return &sumFloat64{} },
		getMetTyp(pipepb.MonitoringInfoTypeUrns_DISTRIBUTION_INT64_TYPE): func() metricAccumulator {
			// Defaults should be safe since the metric only exists if we get any values at all.
			return &distributionInt64{dist: metrics.DistributionValue{Min: math.MaxInt64, Max: math.MinInt64}}
		},
		getMetTyp(pipepb.MonitoringInfoTypeUrns_LATEST_INT64_TYPE): func() metricAccumulator {
			// Initializes the gauge so any new value will override it.
			return &gaugeInt64{millisSinceEpoch: math.MinInt64}
		},
		getMetTyp(pipepb.MonitoringInfoTypeUrns_LATEST_DOUBLE_TYPE): func() metricAccumulator {
			// Initializes the gauge so any new value will override it.
			return &gaugeFloat64{millisSinceEpoch: math.MinInt64}
		},
		getMetTyp(pipepb.MonitoringInfoTypeUrns_SET_STRING_TYPE): func() metricAccumulator {
			return &stringSet{set: map[string]struct{}{}}
		},
		getMetTyp(pipepb.MonitoringInfoTypeUrns_PROGRESS_TYPE): func() metricAccumulator { return &progress{} },
	}

	ret := make(map[string]urnOps)
	for urn, spec := range mUrn2Spec {
		hasher.Reset()
		sorted := spec.GetRequiredLabels()
		sort.Strings(sorted)
		for _, l := range sorted {
			hasher.WriteString(l)
		}
		key := hasher.Sum64()
		fn, ok := l2func[key]
		if !ok {
			slog.Debug("unknown MonitoringSpec required Labels",
				slog.String("urn", spec.GetType()),
				slog.String("key", spec.GetType()),
				slog.Any("sortedlabels", sorted))
			continue
		}
		fac, ok := typ2accumFac[spec.GetType()]
		if !ok {
			slog.Debug("unknown MonitoringSpec type")
			continue
		}
		ret[urn] = urnOps{
			keyFn:    fn,
			newAccum: fac,
		}
	}
	return ret
}

type sumInt64 struct {
	sum int64
}

func (m *sumInt64) accumulate(pyld []byte) error {
	v, err := coder.DecodeVarInt(bytes.NewBuffer(pyld))
	if err != nil {
		return err
	}
	m.sum += v
	return nil
}

func (m *sumInt64) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeVarInt(m.sum, &buf)
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_SUM_INT64_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

type sumFloat64 struct {
	sum float64
}

func (m *sumFloat64) accumulate(pyld []byte) error {
	v, err := coder.DecodeDouble(bytes.NewBuffer(pyld))
	if err != nil {
		return err
	}
	m.sum += v
	return nil
}

func (m *sumFloat64) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeDouble(m.sum, &buf)
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_SUM_DOUBLE_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

type progress struct {
	snap []float64
}

func (m *progress) accumulate(pyld []byte) error {
	buf := bytes.NewBuffer(pyld)
	// Assuming known length iterable
	n, err := coder.DecodeInt32(buf)
	if err != nil {
		return err
	}
	progs := make([]float64, 0, n)
	for i := int32(0); i < n; i++ {
		v, err := coder.DecodeDouble(buf)
		if err != nil {
			return err
		}
		progs = append(progs, v)
	}
	m.snap = progs
	return nil
}

func (m *progress) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeInt32(int32(len(m.snap)), &buf)
	for _, v := range m.snap {
		coder.EncodeDouble(v, &buf)
	}
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_PROGRESS_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

func ordMin[T constraints.Ordered](a T, b T) T {
	if a < b {
		return a
	}
	return b
}

func ordMax[T constraints.Ordered](a T, b T) T {
	if a > b {
		return a
	}
	return b
}

type distributionInt64 struct {
	dist metrics.DistributionValue
}

func (m *distributionInt64) accumulate(pyld []byte) error {
	buf := bytes.NewBuffer(pyld)
	var dist metrics.DistributionValue
	var err error
	if dist.Count, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	if dist.Sum, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	if dist.Min, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	if dist.Max, err = coder.DecodeVarInt(buf); err != nil {
		return err
	}
	m.dist = metrics.DistributionValue{
		Count: m.dist.Count + dist.Count,
		Sum:   m.dist.Sum + dist.Sum,
		Min:   ordMin(m.dist.Min, dist.Min),
		Max:   ordMax(m.dist.Max, dist.Max),
	}
	return nil
}

func (m *distributionInt64) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeVarInt(m.dist.Count, &buf)
	coder.EncodeVarInt(m.dist.Sum, &buf)
	coder.EncodeVarInt(m.dist.Min, &buf)
	coder.EncodeVarInt(m.dist.Max, &buf)
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_DISTRIBUTION_INT64_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

type gaugeInt64 struct {
	millisSinceEpoch int64
	val              int64
}

func (m *gaugeInt64) accumulate(pyld []byte) error {
	buf := bytes.NewBuffer(pyld)

	timestamp, err := coder.DecodeVarInt(buf)
	if err != nil {
		return err
	}
	if m.millisSinceEpoch > timestamp {
		// Drop values that are older than what we have already.
		return nil
	}
	val, err := coder.DecodeVarInt(buf)
	if err != nil {
		return err
	}
	m.millisSinceEpoch = timestamp
	m.val = val
	return nil
}

func (m *gaugeInt64) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeVarInt(m.millisSinceEpoch, &buf)
	coder.EncodeVarInt(m.val, &buf)
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_LATEST_INT64_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

type gaugeFloat64 struct {
	millisSinceEpoch int64
	val              float64
}

func (m *gaugeFloat64) accumulate(pyld []byte) error {
	buf := bytes.NewBuffer(pyld)

	timestamp, err := coder.DecodeVarInt(buf)
	if err != nil {
		return err
	}
	if m.millisSinceEpoch > timestamp {
		// Drop values that are older than what we have already.
		return nil
	}
	val, err := coder.DecodeDouble(buf)
	if err != nil {
		return err
	}
	m.millisSinceEpoch = timestamp
	m.val = val
	return nil
}

func (m *gaugeFloat64) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeVarInt(m.millisSinceEpoch, &buf)
	coder.EncodeDouble(m.val, &buf)
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_LATEST_DOUBLE_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

type stringSet struct {
	set map[string]struct{}
}

func (m *stringSet) accumulate(pyld []byte) error {
	buf := bytes.NewBuffer(pyld)

	n, err := coder.DecodeInt32(buf)
	if err != nil {
		return err
	}
	// Assume it's a fixed iterator size.
	for i := int32(0); i < n; i++ {
		val, err := coder.DecodeStringUTF8(buf)
		if err != nil {
			return err
		}
		m.set[val] = struct{}{}
	}
	return nil
}

func (m *stringSet) toProto(key metricKey) *pipepb.MonitoringInfo {
	var buf bytes.Buffer
	coder.EncodeInt32(int32(len(m.set)), &buf)
	for k := range m.set {
		coder.EncodeStringUTF8(k, &buf)
	}
	return &pipepb.MonitoringInfo{
		Urn:     key.Urn(),
		Type:    getMetTyp(pipepb.MonitoringInfoTypeUrns_SET_STRING_TYPE),
		Payload: buf.Bytes(),
		Labels:  key.Labels(),
	}
}

type durability int

const (
	tentative = durability(iota)
	committed
)

type metricAccumulator interface {
	accumulate([]byte) error
	// TODO, maybe just the payload, and another method for its type urn,
	// Since they're all the same except for the payloads and type urn.
	toProto(key metricKey) *pipepb.MonitoringInfo
}

type accumFactory func() metricAccumulator

type metricKey interface {
	Urn() string
	Labels() map[string]string
}

type userMetricKey struct {
	urn, ptransform, namespace, name string
}

func (k userMetricKey) Urn() string {
	return k.urn
}

func (k userMetricKey) Labels() map[string]string {
	return map[string]string{
		"PTRANSFORM": k.ptransform,
		"NAMESPACE":  k.namespace,
		"NAME":       k.name,
	}
}

type pcollectionKey struct {
	urn, pcollection string
}

func (k pcollectionKey) Urn() string {
	return k.urn
}

func (k pcollectionKey) Labels() map[string]string {
	return map[string]string{
		"PCOLLECTION": k.pcollection,
	}
}

type ptransformKey struct {
	urn, ptransform string
}

func (k ptransformKey) Urn() string {
	return k.urn
}

func (k ptransformKey) Labels() map[string]string {
	return map[string]string{
		"PTRANSFORM": k.ptransform,
	}
}

type apiRequestKey struct {
	urn, service, method, resource, ptransform, status string
}

func (k apiRequestKey) Urn() string {
	return k.urn
}

func (k apiRequestKey) Labels() map[string]string {
	return map[string]string{
		"PTRANSFORM": k.ptransform,
		"SERVICE":    k.service,
		"METHOD":     k.method,
		"RESOURCE":   k.resource,
		"STATUS":     k.status,
	}
}

type apiRequestLatenciesKey struct {
	urn, service, method, resource, ptransform string
}

func (k apiRequestLatenciesKey) Urn() string {
	return k.urn
}

func (k apiRequestLatenciesKey) Labels() map[string]string {
	return map[string]string{
		"PTRANSFORM": k.ptransform,
		"SERVICE":    k.service,
		"METHOD":     k.method,
		"RESOURCE":   k.resource,
	}
}

type metricsStore struct {
	mu     sync.Mutex
	accums [2]map[metricKey]metricAccumulator

	shortIDsToKeys      map[string]metricKey
	unprocessedPayloads [2]map[string][]byte
}

func (m *metricsStore) AddShortIDs(resp *fnpb.MonitoringInfosMetadataResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shortIDsToKeys == nil {
		m.shortIDsToKeys = map[string]metricKey{}
	}

	mis := resp.GetMonitoringInfo()
	for short, mi := range mis {
		urn := mi.GetUrn()
		ops, ok := mUrn2Ops[urn]
		if !ok {
			slog.Debug("unknown metrics urn", slog.String("urn", urn))
			continue
		}
		key := ops.keyFn(urn, mi.GetLabels())
		m.shortIDsToKeys[short] = key
	}
	for d, payloads := range m.unprocessedPayloads {
		m.unprocessedPayloads[d] = nil
		m.contributeMetrics(durability(d), payloads)
	}
}

func (m *metricsStore) contributeMetrics(d durability, mdata map[string][]byte) (map[string]int64, []string) {
	var index, totalCount int64
	if m.accums[d] == nil {
		m.accums[d] = map[metricKey]metricAccumulator{}
	}
	if m.unprocessedPayloads[d] == nil {
		m.unprocessedPayloads[d] = map[string][]byte{}
	}
	accums := m.accums[d]
	var missingShortIDs []string
	for short, payload := range mdata {
		key, ok := m.shortIDsToKeys[short]
		if !ok {
			missingShortIDs = append(missingShortIDs, short)
			m.unprocessedPayloads[d][short] = payload
			continue
		}
		a, ok := accums[key]
		if !ok || d == tentative {
			ops, ok := mUrn2Ops[key.Urn()]
			if !ok {
				slog.Debug("unknown metrics urn", slog.String("urn", key.Urn()))
				continue
			}
			a = ops.newAccum()
		}
		if err := a.accumulate(payload); err != nil {
			panic(fmt.Sprintf("error decoding metrics %v: %+v\n\t%+v :%v", key.Urn(), key, a, err))
		}
		accums[key] = a
		switch u := key.Urn(); u {
		case "beam:metric:data_channel:read_index:v1":
			index = a.(*sumInt64).sum // There should only be one of these per progress response.
		case "beam:metric:element_count:v1":
			totalCount += a.(*sumInt64).sum
		}
	}
	return map[string]int64{
		"index":      index,
		"totalCount": totalCount,
	}, missingShortIDs
}

func (m *metricsStore) ContributeTentativeMetrics(payloads *fnpb.ProcessBundleProgressResponse) (map[string]int64, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.contributeMetrics(tentative, payloads.GetMonitoringData())
}

func (m *metricsStore) ContributeFinalMetrics(payloads *fnpb.ProcessBundleResponse) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, unknownIDs := m.contributeMetrics(committed, payloads.GetMonitoringData())
	return unknownIDs
}

func (m *metricsStore) Results(d durability) []*pipepb.MonitoringInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	infos := make([]*pipepb.MonitoringInfo, 0, len(m.accums))
	for key, accum := range m.accums[d] {
		infos = append(infos, accum.toProto(key))
	}
	return infos
}
