package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.sdk.transforms.Reshuffle;

class ReshuffleTranslatorBatch<K, InputT> implements BatchTransformTranslator<Reshuffle<K, InputT>> {

  @Override public void translateNode(Reshuffle<K, InputT> transform,
      BatchTranslationContext context) {

  }
}
