package org.apache.beam.runners.spark;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;


/**
 * A {@link SparkPipelineOptions} for tests.
 */
public interface TestSparkPipelineOptions extends SparkPipelineOptions, TestPipelineOptions {

  @Description("A special flag that forces streaming in tests.")
  @Default.Boolean(false)
  boolean isForceStreaming();
  void setForceStreaming(boolean forceStreaming);

}
