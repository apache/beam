package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class BatchFlattenPCollectionTranslator<T> implements
    TransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {

  @Override public void translateTransform(PTransform<PCollectionList<T>, PCollection<T>> transform,
      TranslationContext context) {

  }
}
