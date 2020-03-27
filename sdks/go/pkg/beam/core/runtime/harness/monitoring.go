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
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	ppb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/ptypes"
)

func monitoring(p *exec.Plan) (*fnpb.Metrics, []*ppb.MonitoringInfo) {
	store := p.Store()
	if store == nil {
		return nil, nil
	}

	// Get the legacy style metrics.
	transforms := make(map[string]*fnpb.Metrics_PTransform)
	metrics.Extractor{
		SumInt64: func(l metrics.Labels, v int64) {
			pb := getTransform(transforms, l)
			pb.User = append(pb.User, &fnpb.Metrics_User{
				MetricName: toName(l),
				Data: &fnpb.Metrics_User_CounterData_{
					CounterData: &fnpb.Metrics_User_CounterData{
						Value: v,
					},
				},
			})
		},
		DistributionInt64: func(l metrics.Labels, count, sum, min, max int64) {
			pb := getTransform(transforms, l)
			pb.User = append(pb.User, &fnpb.Metrics_User{
				MetricName: toName(l),
				Data: &fnpb.Metrics_User_DistributionData_{
					DistributionData: &fnpb.Metrics_User_DistributionData{
						Count: count,
						Sum:   sum,
						Min:   min,
						Max:   max,
					},
				},
			})
		},
		GaugeInt64: func(l metrics.Labels, v int64, t time.Time) {
			ts, err := ptypes.TimestampProto(t)
			if err != nil {
				panic(err)
			}
			pb := getTransform(transforms, l)
			pb.User = append(pb.User, &fnpb.Metrics_User{
				MetricName: toName(l),
				Data: &fnpb.Metrics_User_GaugeData_{
					GaugeData: &fnpb.Metrics_User_GaugeData{
						Value:     v,
						Timestamp: ts,
					},
				},
			})
		},
	}.ExtractFrom(store)

	// Get the MonitoringInfo versions.
	var monitoringInfo []*ppb.MonitoringInfo
	metrics.Extractor{
		SumInt64: func(l metrics.Labels, v int64) {
			payload, err := int64Counter(v)
			if err != nil {
				panic(err)
			}
			monitoringInfo = append(monitoringInfo,
				&ppb.MonitoringInfo{
					Urn:     "beam:metric:user:sum_int64:v1",
					Type:    "beam:metrics:sum_int64:v1",
					Labels:  userLabels(l),
					Payload: payload,
				})
		},
		DistributionInt64: func(l metrics.Labels, count, sum, min, max int64) {
			payload, err := int64Distribution(count, sum, min, max)
			if err != nil {
				panic(err)
			}
			monitoringInfo = append(monitoringInfo,
				&ppb.MonitoringInfo{
					Urn:     "beam:metric:user:distribution_int64:v1",
					Type:    "beam:metrics:distribution_int64:v1",
					Labels:  userLabels(l),
					Payload: payload,
				})
		},
		GaugeInt64: func(l metrics.Labels, v int64, t time.Time) {
			payload, err := int64Latest(t, v)
			if err != nil {
				panic(err)
			}
			monitoringInfo = append(monitoringInfo,
				&ppb.MonitoringInfo{
					Urn:     "beam:metric:user:latest_int64:v1",
					Type:    "beam:metrics:latest_int64:v1",
					Labels:  userLabels(l),
					Payload: payload,
				})
		},
	}.ExtractFrom(store)

	// Get the execution monitoring information from the bundle plan.
	if snapshot, ok := p.Progress(); ok {
		// Legacy version.
		transforms[snapshot.ID] = &fnpb.Metrics_PTransform{
			ProcessedElements: &fnpb.Metrics_PTransform_ProcessedElements{
				Measured: &fnpb.Metrics_PTransform_Measured{
					OutputElementCounts: map[string]int64{
						snapshot.Name: snapshot.Count,
					},
				},
			},
		}
		// Monitoring info version.
		payload, err := int64Counter(snapshot.Count)
		if err != nil {
			panic(err)
		}
		monitoringInfo = append(monitoringInfo,
			&ppb.MonitoringInfo{
				Urn:  "beam:metric:element_count:v1",
				Type: "beam:metrics:sum_int64:v1",
				Labels: map[string]string{
					"PCOLLECTION": snapshot.PID,
				},
				Payload: payload,
			})
	}

	return &fnpb.Metrics{
		Ptransforms: transforms,
	}, monitoringInfo
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

func getTransform(transforms map[string]*fnpb.Metrics_PTransform, l metrics.Labels) *fnpb.Metrics_PTransform {
	if pb, ok := transforms[l.Transform()]; ok {
		return pb
	}
	pb := &fnpb.Metrics_PTransform{}
	transforms[l.Transform()] = pb
	return pb
}

func toName(l metrics.Labels) *fnpb.Metrics_User_MetricName {
	return &fnpb.Metrics_User_MetricName{
		Name:      l.Name(),
		Namespace: l.Namespace(),
	}
}
