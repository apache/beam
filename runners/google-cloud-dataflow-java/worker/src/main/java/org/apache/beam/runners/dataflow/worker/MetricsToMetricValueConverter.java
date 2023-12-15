/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsToMetricValueConverter {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsToMetricValueConverter.class);

  public static Windmill.PerStepNamespaceMetrics convertCountersToPerStepNamespaceMetrics(
    String namespace, Map<MetricName, Long> counterUpdates) {
      if (!namespace.equals(BigQuerySinkMetrics.METRICS_NAMESPACE)) {
        return null;
      }
      List<Windmill.MetricValue> metricUpdates = new ArrayList<Windmill.MetricValue>();
      for (Map.Entry<MetricName, Long> entry : counterUpdates.entrySet()) {
        if (entry.getValue() == 0) {
          continue;
        }
  
        if (!entry.getKey().getNamespace().equals(BigQuerySinkMetrics.METRICS_NAMESPACE)) {
          continue;
        }
  
        BigQuerySinkMetrics.LabeledMetricName labeledName =
            BigQuerySinkMetrics.parseLabeledMetricName(entry.getKey().getName());
        if (labeledName.getBaseName().isEmpty()) {
          continue;
        }
  
        Windmill.MetricValue val =
            Windmill.MetricValue.newBuilder()
                .setMetricName(labeledName.getBaseName())
                .putAllMetricLabels(labeledName.getMetricLabels())
                .setValueInt64(entry.getValue())
                .build();
        metricUpdates.add(val);
      }
  
    }

  public static List<Windmill.MetricValue> convertCountersToMetricValueUpdates(
      Map<MetricName, Long> counterUpdates) {
    List<Windmill.MetricValue> metricUpdates = new ArrayList<Windmill.MetricValue>();

    for (Map.Entry<MetricName, Long> entry : counterUpdates.entrySet()) {
      if (entry.getValue() == 0) {
        continue;
      }

      if (!entry.getKey().getNamespace().equals(BigQuerySinkMetrics.METRICS_NAMESPACE)) {
        continue;
      }

      BigQuerySinkMetrics.LabeledMetricName labeledName =
          BigQuerySinkMetrics.parseLabeledMetricName(entry.getKey().getName());
      if (labeledName.getBaseName().isEmpty()) {
        continue;
      }

      Windmill.MetricValue val =
          Windmill.MetricValue.newBuilder()
              .setMetricName(labeledName.getBaseName())
              .putAllMetricLabels(labeledName.getMetricLabels())
              .setValueInt64(entry.getValue())
              .build();
      metricUpdates.add(val);
    }

    return metricUpdates;
  }

  public static List<Windmill.MetricValue> convertHistogramsToMetricValueUpdates(Map<MetricName, HistogramData> histogramUpdates) {
    List<Windmill.MetricValue> metricUpdates = new ArrayList<Windmill.MetricValue>();

    for (Map.Entry<MetricName, HistogramData> entry : histogramUpdates.entrySet()) {
      BigQuerySinkMetrics.LabeledMetricName labeledName =
      BigQuerySinkMetrics.parseLabeledMetricName(entry.getKey().getName());
      if (labeledName.getBaseName().isEmpty()) {
        continue;
      }

      HistogramData data = entry.getValue();
      if (data.getTotalCount() == 0L) {
        continue;
      }
      Windmill.Histogram.Builder histogramBuilder = Windmill.Histogram.newBuilder();
      int numberOfBuckets = data.getBucketType().getNumBuckets() + 2;

      if (data.getBucketType() instanceof HistogramData.LinearBuckets) {
        HistogramData.LinearBuckets buckets = (HistogramData.LinearBuckets) data.getBucketType();
        histogramBuilder.getBucketOptionsBuilder().getLinearBuilder().setNumberOfBuckets(numberOfBuckets).setWidth(buckets.getWidth()).setStart(buckets.getStart());
        LOG.warn("Successfully converted to linear buckets.");
      } else if (data.getBucketType() instanceof HistogramData.ExponentialBuckets) {
        HistogramData.ExponentialBuckets buckets = (HistogramData.ExponentialBuckets) data.getBucketType();
        histogramBuilder.getBucketOptionsBuilder().getExponentialBuilder().setNumberOfBuckets(numberOfBuckets).setScale(buckets.getScale());
        LOG.warn("Successfully converted to exponential buckets.");
      } else {
        LOG.warn("Unable to convert types, bucket class: " + data.getBucketType().getClass().getName() + " Expected name: " + HistogramData.LinearBuckets.class.getName());
        continue;
      }

      histogramBuilder.setMean(data.getMean()).setSumOfSquaredDeviations(data.getSumOfSquaredDeviations()).setCount(data.getTotalCount());
      histogramBuilder.addBucketCounts(data.getBottomBucketCount());
      for (int i = 0; i < data.getBucketType().getNumBuckets(); i ++) {
        histogramBuilder.addBucketCounts(data.getCount(i));
      }
      histogramBuilder.addBucketCounts(data.getTopBucketCount());
      Windmill.MetricValue val =
          Windmill.MetricValue.newBuilder()
              .setMetricName(labeledName.getBaseName())
              .putAllMetricLabels(labeledName.getMetricLabels())
              .setValueHistogram(histogramBuilder.build())
              .build();
      metricUpdates.add(val);
    }
    return metricUpdates;
  }

}
