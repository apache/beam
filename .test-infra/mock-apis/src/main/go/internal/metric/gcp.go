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

type GcpGauge monitoring.MetricClient

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
