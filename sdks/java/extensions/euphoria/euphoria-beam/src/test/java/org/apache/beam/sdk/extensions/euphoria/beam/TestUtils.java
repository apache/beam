package org.apache.beam.sdk.extensions.euphoria.beam;

import java.time.Duration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Collection of methods reused among tests.
 */
public class TestUtils {

  static BeamExecutor createExecutor() {
    String[] args = {"--runner=DirectRunner"};
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    return new BeamExecutor(options).withAllowedLateness(Duration.ofHours(1));
  }
}
