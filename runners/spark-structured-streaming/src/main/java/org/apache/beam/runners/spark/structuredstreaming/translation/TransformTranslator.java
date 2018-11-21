package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.sdk.transforms.PTransform;

public interface TransformTranslator<TransformT extends PTransform> {

  /** A translator of a {@link PTransform}. */

  void translateNode(TransformT transform, TranslationContext context);
  }

