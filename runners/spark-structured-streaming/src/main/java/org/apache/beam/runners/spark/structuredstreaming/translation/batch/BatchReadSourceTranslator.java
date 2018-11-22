package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

class BatchReadSourceTranslator<T> implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {

  @Override public void translateTransform(PTransform<PBegin, PCollection<T>> transform,
      TranslationContext context) {

  }
}
