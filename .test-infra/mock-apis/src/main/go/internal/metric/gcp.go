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

package metric

import (
	"context"
	"path"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	metricTypePrefix      = "custom.googleapis.com"
	monitoredResourceType = "generic_task"
)

// GcpGauge implements a Writer for a Google Cloud gauge.
// See https://cloud.google.com/monitoring/api/v3/kinds-and-types#metric-kinds
type GcpGauge monitoring.MetricClient

// Write to a Google Cloud monitoring gauge.
func (writer *GcpGauge) Write(ctx context.Context, name string, unit string, points ...*Point) error {
	var mPts []*monitoringpb.Point
	for _, p := range points {
		t := timestamppb.New(p.Timestamp)
		mPts = append(mPts, &monitoringpb.Point{
			Interval: &monitoringpb.TimeInterval{
				StartTime: t,
				EndTime:   t,
			},
			Value: &monitoringpb.TypedValue{
				Value: &monitoringpb.TypedValue_Int64Value{
					Int64Value: p.Value,
				},
			},
		})
	}
	ts := timeseries(name, unit, metric.MetricDescriptor_GAUGE, mPts)

	client := (*monitoring.MetricClient)(writer)
	return client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name:       name,
		TimeSeries: []*monitoringpb.TimeSeries{ts},
	})
}

func timeseries(name string, unit string, kind metric.MetricDescriptor_MetricKind, points []*monitoringpb.Point) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type: path.Join(metricTypePrefix, name),
		},
		Resource: &monitoredres.MonitoredResource{
			Type: monitoredResourceType,
		},
		MetricKind: kind,
		ValueType:  metric.MetricDescriptor_INT64,
		Unit:       unit,
		Points:     points,
	}
}
