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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

/**
 * TODO: Add support of state and timers
 * TODO: Add support of side inputs
 *
 * @param <InputT>
 * @param <OutputT>
 */
class ParDoTranslatorBatch<InputT, OutputT>
    implements TransformTranslator<PTransform<PCollection<InputT>, PCollectionTuple>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<InputT>, PCollectionTuple> transform, TranslationContext context) {

    // Check for not supported advanced features
    // TODO: add support of Splittable DoFn
    DoFn<InputT, OutputT> doFn = getDoFn(context);
    checkState(
        !DoFnSignatures.signatureForDoFn(doFn).processElement().isSplittable(),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    // TODO: add support of states and timers
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    boolean stateful =
        signature.stateDeclarations().size() > 0 || signature.timerDeclarations().size() > 0;
    checkState(!stateful, "States and timers are not supported for the moment.");

    // TODO: add support of SideInputs
    List<PCollectionView<?>> sideInputs = getSideInputs(context);
    System.out.println("sideInputs = " + sideInputs);
    final boolean hasSideInputs = sideInputs != null && sideInputs.size() > 0;
    checkState(!hasSideInputs, "SideInputs are not supported for the moment.");

    // Init main variables
    Dataset<WindowedValue<InputT>> inputDataSet = context.getDataset(context.getInput());
    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    TupleTag<?> mainOutputTag = getTupleTag(context);
    List<TupleTag<?>> outputTags = Lists.newArrayList(outputs.keySet());
    WindowingStrategy<?, ?> windowingStrategy =
        ((PCollection<InputT>) context.getInput()).getWindowingStrategy();

    // construct a map from side input to WindowingStrategy so that
    // the DoFn runner can map main-input windows to side input windows
    Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
    for (PCollectionView<?> sideInput : sideInputs) {
      sideInputStrategies.put(sideInput, sideInput.getPCollection().getWindowingStrategy());
    }

    Map<TupleTag<?>, Coder<?>> outputCoderMap = context.getOutputCoders();
    Coder<InputT> inputCoder = ((PCollection<InputT>) context.getInput()).getCoder();

    @SuppressWarnings("unchecked")
    DoFnFunction<InputT, OutputT> doFnWrapper =
        new DoFnFunction(
            doFn,
            windowingStrategy,
            sideInputStrategies,
            context.getSerializableOptions(),
            outputTags,
            mainOutputTag,
            inputCoder,
            outputCoderMap);

    Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> allOutputs =
        inputDataSet.mapPartitions(doFnWrapper, EncoderHelpers.tuple2Encoder());

    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      pruneOutputFilteredByTag(context, allOutputs, output);
    }
  }

  private List<PCollectionView<?>> getSideInputs(TranslationContext context) {
    List<PCollectionView<?>> sideInputs;
    try {
      sideInputs = ParDoTranslation.getSideInputs(context.getCurrentTransform());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sideInputs;
  }

  private TupleTag<?> getTupleTag(TranslationContext context) {
    TupleTag<?> mainOutputTag;
    try {
      mainOutputTag = ParDoTranslation.getMainOutputTag(context.getCurrentTransform());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return mainOutputTag;
  }

  @SuppressWarnings("unchecked")
  private DoFn<InputT, OutputT> getDoFn(TranslationContext context) {
    DoFn<InputT, OutputT> doFn;
    try {
      doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(context.getCurrentTransform());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return doFn;
  }

  private void pruneOutputFilteredByTag(
      TranslationContext context,
      Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> allOutputs,
      Map.Entry<TupleTag<?>, PValue> output) {
    Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> filteredDataset =
        allOutputs.filter(new DoFnFilterFunction(output.getKey()));
    Dataset<WindowedValue<?>> outputDataset =
        filteredDataset.map(
            (MapFunction<Tuple2<TupleTag<?>, WindowedValue<?>>, WindowedValue<?>>)
                value -> value._2,
            EncoderHelpers.windowedValueEncoder());
    context.putDatasetWildcard(output.getValue(), outputDataset);
  }

  static class DoFnFilterFunction
      implements FilterFunction<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final TupleTag<?> key;

    public DoFnFilterFunction(TupleTag<?> key) {
      this.key = key;
    }

    @Override
    public boolean call(Tuple2<TupleTag<?>, WindowedValue<?>> value) {
      return value._1.equals(key);
    }
  }
}
