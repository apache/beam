package org.apache.beam.runners.spark.structuredstreaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for beam to spark source translation.
 */
public class SourceTest {
  private static Pipeline pipeline;

  @BeforeClass
  public static void beforeClass(){
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testBoundedSource(){
    pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    pipeline.run();
  }

}
