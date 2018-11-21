package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import org.apache.beam.sdk.transforms.PTransform;

public interface BatchTransformTranslator<TransformT extends PTransform> {

  /** A translator of a {@link PTransform} in batch mode. */

  void translateNode(TransformT transform, BatchTranslationContext context);
  }

