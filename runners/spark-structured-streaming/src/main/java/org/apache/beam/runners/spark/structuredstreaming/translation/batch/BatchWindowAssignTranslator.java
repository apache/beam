package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

class BatchWindowAssignTranslator<T> implements
    TransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {

  @Override public void translateNode(PTransform<PCollection<T>, PCollection<T>> transform,
      TranslationContext context) {
  }
}
