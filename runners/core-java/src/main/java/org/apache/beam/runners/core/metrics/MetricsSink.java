package org.apache.beam.runners.core.metrics;

import java.io.Serializable;
import org.apache.beam.sdk.metrics.MetricQueryResults;

/**
 * Abstract base class for all metric sinks it can use different serializers.
 */

public abstract class MetricsSink<OutputT> implements Serializable{

  private MetricsSerializer<OutputT> metricsSerializer = provideSerializer();

  void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    OutputT serializedMetrics = metricsSerializer.serializeMetrics(metricQueryResults);
    writeSerializedMetrics(serializedMetrics);
  }
  protected abstract MetricsSerializer<OutputT> provideSerializer();
  protected abstract void writeSerializedMetrics(OutputT metrics) throws Exception;
}

