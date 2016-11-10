package org.apache.beam.runners.spark.translation.streaming.utils;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

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

  public SparkPipelineOptions withTmpCheckpointDir(TemporaryFolder parent)
      throws IOException {
    // tests use JUnit's TemporaryFolder path in the form of: /.../junit/...
    options.setCheckpointDir(parent.newFolder(options.getJobName()).toURI().toURL().toString());
    return options;
  }

  public SparkPipelineOptions getOptions() {
    return options;
  }
}
