package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

class ReadSourceTranslatorBatch<T> implements BatchTransformTranslator<PTransform<PBegin, PCollection<T>>> {

  @Override public void translateNode(PTransform<PBegin, PCollection<T>> transform, BatchTranslationContext context) {

  }
}
