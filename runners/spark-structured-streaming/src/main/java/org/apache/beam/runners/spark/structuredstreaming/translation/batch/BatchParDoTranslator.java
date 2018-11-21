package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

class BatchParDoTranslator<InputT, OutputT> implements
    TransformTranslator<PTransform<PCollection<InputT>, PCollectionTuple>> {

  @Override public void translateNode(PTransform<PCollection<InputT>, PCollectionTuple> transform,
      TranslationContext context) {

  }
}
