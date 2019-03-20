package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.IOException;
import org.apache.beam.runners.core.construction.CreatePCollectionViewTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.spark.sql.Dataset;

class CreatePCollectionViewTranslatorBatch<ElemT, ViewT>
    implements TransformTranslator<PTransform<PCollection<ElemT>, PCollection<ElemT>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<ElemT>, PCollection<ElemT>> transform, TranslationContext context) {

    Dataset<WindowedValue<ElemT>> inputDataSet = context.getDataset(context.getInput());

    @SuppressWarnings("unchecked") AppliedPTransform<
        PCollection<ElemT>, PCollection<ElemT>,
        PTransform<PCollection<ElemT>, PCollection<ElemT>>>
        application =
        (AppliedPTransform<
            PCollection<ElemT>, PCollection<ElemT>,
            PTransform<PCollection<ElemT>, PCollection<ElemT>>>)
            context.getCurrentTransform();
    PCollectionView<ViewT> input;
    try {
      input = CreatePCollectionViewTranslation.getView(application);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    context.setSideInputDataset(input, inputDataSet);
  }
}
