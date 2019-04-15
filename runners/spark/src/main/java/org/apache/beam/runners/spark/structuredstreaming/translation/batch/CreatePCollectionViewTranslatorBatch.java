/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    @SuppressWarnings("unchecked")
    AppliedPTransform<
            PCollection<ElemT>,
            PCollection<ElemT>,
            PTransform<PCollection<ElemT>, PCollection<ElemT>>>
        application =
            (AppliedPTransform<
                    PCollection<ElemT>,
                    PCollection<ElemT>,
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
