package org.apache.beam.runners.tez.translation;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Translates {@link PTransform} to Tez functions.
 */
interface TransformTranslator<T extends PTransform<?, ?>> extends Serializable {
  void translate(T transform, TranslationContext context);
}
