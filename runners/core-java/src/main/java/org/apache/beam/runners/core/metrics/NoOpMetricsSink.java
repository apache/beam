package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * This is the default metrics sink that does nothing. When it is set,
 * MetricsPusher does not start pushing thread.
 */
public class NoOpMetricsSink implements MetricsSink<Void>{

  public NoOpMetricsSink(PipelineOptions pipelineOptions) {}

  @Override public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
  }
}
