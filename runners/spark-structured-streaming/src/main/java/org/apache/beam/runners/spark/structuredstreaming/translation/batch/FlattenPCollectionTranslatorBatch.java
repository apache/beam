package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class FlattenPCollectionTranslatorBatch<T> implements BatchTransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {

  @Override public void translateNode(PTransform<PCollectionList<T>, PCollection<T>> transform,
      BatchTranslationContext context) {

  }
}
