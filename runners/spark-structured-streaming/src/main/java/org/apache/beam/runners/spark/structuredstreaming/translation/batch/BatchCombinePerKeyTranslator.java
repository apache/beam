package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class BatchCombinePerKeyTranslator<K, InputT, AccumT, OutputT> implements
    TransformTranslator<PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

  @Override public void translateNode(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      TranslationContext context) {

  }
}
