package org.apache.beam.runners.spark.structuredstreaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

public class SourceTest {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    pipeline.run();
  }

}
