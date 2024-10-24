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
package org.apache.beam.runners.twister2.translators.batch;

import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import java.io.IOException;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.translators.BatchTransformTranslator;
import org.apache.beam.runners.twister2.translators.functions.ByteToElemFunction;
import org.apache.beam.runners.twister2.translators.functions.ByteToWindowFunctionPrimitive;
import org.apache.beam.runners.twister2.translators.functions.ElemToBytesFunction;
import org.apache.beam.runners.twister2.translators.functions.MapToTupleFunction;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CreatePCollectionViewTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

/** PCollectionView translator. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class PCollectionViewTranslatorBatch<ElemT, ViewT>
    implements BatchTransformTranslator<View.CreatePCollectionView<ElemT, ViewT>> {
  @Override
  public void translateNode(
      View.CreatePCollectionView<ElemT, ViewT> transform, Twister2BatchTranslationContext context) {
    BatchTSet<WindowedValue<ElemT>> inputDataSet =
        context.getInputDataSet(context.getInput(transform));
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
    org.apache.beam.sdk.values.PCollectionView<ViewT> input;
    PCollection<ElemT> inputPCol = context.getInput(transform);
    final Coder coder = inputPCol.getCoder();
    WindowingStrategy windowingStrategy = inputPCol.getWindowingStrategy();
    WindowFn windowFn = windowingStrategy.getWindowFn();
    try {
      input = CreatePCollectionViewTranslation.getView(application);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    switch (input.getViewFn().getMaterialization().getUrn()) {
      case Materializations.MULTIMAP_MATERIALIZATION_URN:
        KvCoder kvCoder = (KvCoder<?, ?>) coder;
        final Coder keyCoder = kvCoder.getKeyCoder();
        final WindowedValue.WindowedValueCoder kvwvCoder =
            WindowedValue.FullWindowedValueCoder.of(
                kvCoder.getValueCoder(), windowFn.windowCoder());
        BatchTSet<WindowedValue<ElemT>> multimapMaterialization =
            inputDataSet
                .direct()
                .map(new MapToTupleFunction<>(keyCoder, kvwvCoder))
                .allGather()
                .map(new ByteToWindowFunctionPrimitive(keyCoder, kvwvCoder));
        context.setSideInputDataSet(input.getTagInternal().getId(), multimapMaterialization);
        break;
      case Materializations.ITERABLE_MATERIALIZATION_URN:
        final WindowedValue.WindowedValueCoder wvCoder =
            WindowedValue.FullWindowedValueCoder.of(coder, windowFn.windowCoder());
        BatchTSet<WindowedValue<ElemT>> iterableMaterialization =
            inputDataSet
                .direct()
                .map(new ElemToBytesFunction<>(wvCoder))
                .allGather()
                .map(new ByteToElemFunction(wvCoder));
        try {
          input = CreatePCollectionViewTranslation.getView(application);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        context.setSideInputDataSet(input.getTagInternal().getId(), iterableMaterialization);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown side input materialization "
                + input.getViewFn().getMaterialization().getUrn());
    }
  }
}
