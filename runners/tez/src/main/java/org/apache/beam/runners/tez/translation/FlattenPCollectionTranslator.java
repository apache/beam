package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.transforms.Flatten;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Flatten} translation to Tez equivalent.
 */
class FlattenPCollectionTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlattenPCollectionTranslator.class);

  @Override
  public void translate(Flatten.PCollections<T> transform, TranslationContext context) {
    //TODO: Translate transform to Tez and add to TranslationContext
  }
}