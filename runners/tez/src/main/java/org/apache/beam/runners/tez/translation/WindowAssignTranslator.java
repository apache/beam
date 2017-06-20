package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.Assign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Assign} translation to Tez equivalent.
 */
class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(WindowAssignTranslator.class);

  @Override
  public void translate(Assign<T> transform, TranslationContext context) {
    //TODO: Translate transform to Tez and add to TranslationContext
  }
}