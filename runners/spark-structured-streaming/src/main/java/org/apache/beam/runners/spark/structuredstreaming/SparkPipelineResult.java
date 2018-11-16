package org.apache.beam.runners.spark.structuredstreaming;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

public class SparkPipelineResult implements PipelineResult {

  @Override public State getState() {
    return null;
  }

  @Override public State cancel() throws IOException {
    return null;
  }

  @Override public State waitUntilFinish(Duration duration) {
    return null;
  }

  @Override public State waitUntilFinish() {
    return null;
  }

  @Override public MetricResults metrics() {
    return null;
  }
}
