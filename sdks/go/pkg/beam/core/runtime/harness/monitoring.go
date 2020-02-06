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
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	ppb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/ptypes"
)

func monitoring(p *exec.Plan) (*fnpb.Metrics, []*ppb.MonitoringInfo) {
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
	}.ExtractFrom(p.Store)

	// Get the MonitoringInfo versions.
	var monitoringInfo []*ppb.MonitoringInfo
	metrics.Extractor{
		SumInt64: func(l metrics.Labels, v int64) {
			monitoringInfo = append(monitoringInfo,
				&ppb.MonitoringInfo{
					Urn:    "beam:metric:user",
					Type:   "beam:metrics:sum_int_64",
					Labels: userLabels(l),
					Data:   int64Counter(v),
				})
		},
		DistributionInt64: func(l metrics.Labels, count, sum, min, max int64) {
			monitoringInfo = append(monitoringInfo,
				&ppb.MonitoringInfo{
					Urn:    "beam:metric:user_distribution",
					Type:   "beam:metrics:distribution_int_64",
					Labels: userLabels(l),
					Data:   int64Distribution(count, sum, min, max),
				})
		},
		GaugeInt64: func(l metrics.Labels, v int64, t time.Time) {
			ts, err := ptypes.TimestampProto(t)
			if err != nil {
				panic(err)
			}
			monitoringInfo = append(monitoringInfo,
				&ppb.MonitoringInfo{
					Urn:       "beam:metric:user",
					Type:      "beam:metrics:latest_int_64",
					Labels:    userLabels(l),
					Data:      int64Counter(v),
					Timestamp: ts,
				})
		},
	}.ExtractFrom(p.Store)

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
		monitoringInfo = append(monitoringInfo,
			&ppb.MonitoringInfo{
				Urn:  "beam:metric:element_count:v1",
				Type: "beam:metrics:sum_int_64",
				Labels: map[string]string{
					"PCOLLECTION": snapshot.PID,
				},
				Data: int64Counter(snapshot.Count),
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

func int64Counter(v int64) *ppb.MonitoringInfo_Metric {
	return &ppb.MonitoringInfo_Metric{
		Metric: &ppb.Metric{
			Data: &ppb.Metric_CounterData{
				CounterData: &ppb.CounterData{
					Value: &ppb.CounterData_Int64Value{
						Int64Value: v,
					},
				},
			},
		},
	}
}

func int64Distribution(count, sum, min, max int64) *ppb.MonitoringInfo_Metric {
	return &ppb.MonitoringInfo_Metric{
		Metric: &ppb.Metric{
			Data: &ppb.Metric_DistributionData{
				DistributionData: &ppb.DistributionData{
					Distribution: &ppb.DistributionData_IntDistributionData{
						IntDistributionData: &ppb.IntDistributionData{
							Count: count,
							Sum:   sum,
							Min:   min,
							Max:   max,
						},
					},
				},
			},
		},
	}
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
