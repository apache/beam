package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

class ParDoTranslatorBatch<InputT, OutputT> implements BatchTransformTranslator<PTransform<PCollection<InputT>, PCollectionTuple>> {

  @Override public void translateNode(PTransform<PCollection<InputT>, PCollectionTuple> transform,
      BatchTranslationContext context) {

  }
}
