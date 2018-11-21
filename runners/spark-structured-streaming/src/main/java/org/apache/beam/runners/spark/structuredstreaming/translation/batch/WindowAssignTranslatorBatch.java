package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

class WindowAssignTranslatorBatch<T> implements BatchTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {

  @Override public void translateNode(PTransform<PCollection<T>, PCollection<T>> transform,
      BatchTranslationContext context) {

  }
}
