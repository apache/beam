package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.runners.TransformHierarchy;

public class StreamingPipelineTranslator extends PipelineTranslator {

  public StreamingPipelineTranslator(SparkPipelineOptions options) {
  }

  @Override protected TransformTranslator<?> getTransformTranslator(TransformHierarchy.Node node) {
    return null;
  }
}
