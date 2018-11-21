package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.Reshuffle;

class BatchReshuffleTranslator<K, InputT> implements TransformTranslator<Reshuffle<K, InputT>> {

  @Override public void translateNode(Reshuffle<K, InputT> transform, TranslationContext context) {

  }
}
