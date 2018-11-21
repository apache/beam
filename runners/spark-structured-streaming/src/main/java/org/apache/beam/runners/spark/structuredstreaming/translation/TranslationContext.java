package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.sdk.runners.AppliedPTransform;

public class TranslationContext {

  private AppliedPTransform<?, ?, ?> currentTransform;

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

}
