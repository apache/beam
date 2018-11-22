package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;

/**
 * This class contains only streaming specific context components.
 */
public class StreamingTranslationContext extends TranslationContext {

  public StreamingTranslationContext(SparkPipelineOptions options) {
    super(options);
  }
}
