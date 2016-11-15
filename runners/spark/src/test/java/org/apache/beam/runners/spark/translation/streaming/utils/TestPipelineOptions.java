package org.apache.beam.runners.spark.translation.streaming.utils;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.rules.ExternalResource;

/**
 * A rule to create a common {@link SparkPipelineOptions} test options for spark-runner.
 */
public class TestPipelineOptions extends ExternalResource {

  protected final SparkPipelineOptions options =
      PipelineOptionsFactory.as(SparkPipelineOptions.class);

  @Override
  protected void before() throws Throwable {
    options.setRunner(SparkRunner.class);
    options.setEnableSparkMetricSinks(false);
  }

  public SparkPipelineOptions getOptions() {
    return options;
  }
}
