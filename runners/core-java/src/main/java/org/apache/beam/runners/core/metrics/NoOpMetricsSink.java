package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsSink;

public class NoOpMetricsSink implements MetricsSink<Void>{

  @Override public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
  }
}
