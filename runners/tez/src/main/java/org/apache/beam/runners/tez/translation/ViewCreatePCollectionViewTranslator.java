package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.transforms.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link View.CreatePCollectionView} translation to Tez equivalent.
 */
class ViewCreatePCollectionViewTranslator<ElemT, ViewT> implements
    TransformTranslator<View.CreatePCollectionView<ElemT, ViewT>> {
  private static final Logger LOG = LoggerFactory.getLogger(ViewCreatePCollectionViewTranslator.class);

  @Override
  public void translate(View.CreatePCollectionView<ElemT, ViewT> transform, TranslationContext context) {
    //TODO: Translate transform to Tez and add to TranslationContext
  }
}