package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Base class that gives a context for {@link PTransform} translation.
 */
public class TranslationContext {

  private AppliedPTransform<?, ?, ?> currentTransform;

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

}
