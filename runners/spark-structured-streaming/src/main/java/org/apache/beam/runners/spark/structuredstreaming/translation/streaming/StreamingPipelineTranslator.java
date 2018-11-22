package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;

/** {@link PipelineTranslator} for executing a {@link Pipeline} in Spark in streaming mode.
 * This contains only the components specific to streaming: {@link StreamingTranslationContext},
 * registry of batch {@link TransformTranslator} and registry lookup code. */

public class StreamingPipelineTranslator extends PipelineTranslator {

  public StreamingPipelineTranslator(SparkPipelineOptions options) {
  }

  @Override protected TransformTranslator<?> getTransformTranslator(TransformHierarchy.Node node) {
    return null;
  }
}
