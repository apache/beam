package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.MetricQueryResults;

/**
 * This is the default Metrics Sink that just store in a static field the first counter (if it
 * exists) attempted value. This is usefull for tests.
 */
public class DummyMetricsSink extends MetricsSink<Long> {
  private static long counterValue;

  @Override protected MetricsSerializer<Long> provideSerializer() {
    return new MetricsSerializer<Long>() {

      @Override
      public Long serializeMetrics(MetricQueryResults metricQueryResults) throws Exception {
        return metricQueryResults.counters().iterator().hasNext()
            ? metricQueryResults.counters().iterator().next().attempted()
            : null;
      }
    };
  }

  @Override protected void writeSerializedMetrics(Long metrics) throws Exception {
    counterValue = metrics != null ? metrics : 0L;
  }
  public static long getCounterValue(){
    return counterValue;
  }

  public static void clear(){
    counterValue = 0L;
  }
}
