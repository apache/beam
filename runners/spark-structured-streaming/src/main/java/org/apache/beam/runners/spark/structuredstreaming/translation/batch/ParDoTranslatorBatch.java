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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

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

    Dataset<WindowedValue<InputT>> inputDataSet = context.getDataset(context.getInput());
    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    TupleTag<?> mainOutputTag = getTupleTag(context);

    Map<TupleTag<?>, Integer> outputTags = Maps.newHashMap();

    outputTags.put(mainOutputTag, 0);
    int count = 1;
    for (TupleTag<?> tag : outputs.keySet()) {
      if (!outputTags.containsKey(tag)) {
        outputTags.put(tag, count++);
      }
    }

    // Union coder elements must match the order of the output tags.
    Map<Integer, TupleTag<?>> indexMap = Maps.newTreeMap();
    for (Map.Entry<TupleTag<?>, Integer> entry : outputTags.entrySet()) {
      indexMap.put(entry.getValue(), entry.getKey());
    }

    // assume that the windowing strategy is the same for all outputs
    WindowingStrategy<?, ?> windowingStrategy = null;

    // collect all output Coders and create a UnionCoder for our tagged outputs
    List<Coder<?>> outputCoders = Lists.newArrayList();
    for (TupleTag<?> tag : indexMap.values()) {
      PValue taggedValue = outputs.get(tag);
      checkState(
          taggedValue instanceof PCollection,
          "Within ParDo, got a non-PCollection output %s of type %s",
          taggedValue,
          taggedValue.getClass().getSimpleName());
      PCollection<?> coll = (PCollection<?>) taggedValue;
      outputCoders.add(coll.getCoder());
      windowingStrategy = coll.getWindowingStrategy();
    }

    if (windowingStrategy == null) {
      throw new IllegalStateException("No outputs defined.");
    }

    UnionCoder unionCoder = UnionCoder.of(outputCoders);

    List<PCollectionView<?>> sideInputs = getSideInputs(context);
    final boolean hasSideInputs = sideInputs != null && sideInputs.size() > 0;
    // TODO: add support of SideInputs
    checkState(!hasSideInputs, "SideInputs are not supported for the moment.");

    // construct a map from side input to WindowingStrategy so that
    // the DoFn runner can map main-input windows to side input windows
    Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
    for (PCollectionView<?> sideInput : sideInputs) {
      sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
    }

    Map<TupleTag<?>, Coder<?>> outputCoderMap = context.getOutputCoders();

    @SuppressWarnings("unchecked")
    DoFnFunction<InputT, OutputT> doFnWrapper =
        new DoFnFunction(
            doFn,
            windowingStrategy,
            sideInputStrategies,
            context.getOptions(),
            outputTags,
            mainOutputTag,
            ((PCollection<InputT>)context.getInput()).getCoder(),
            outputCoderMap);

    Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> allOutputsDataset =
        inputDataSet.mapPartitions(doFnWrapper, EncoderHelpers.encoder());

    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      pruneOutput(context, allOutputsDataset, output);
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

  private <T> void pruneOutput(
      TranslationContext context,
      Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> tmpDataset,
      Map.Entry<TupleTag<?>, PValue> output) {
    Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> filteredDataset =
        tmpDataset.filter(new SparkDoFnFilterFunction(output.getKey()));
    Dataset<WindowedValue<?>> outputDataset =
        filteredDataset.map(
            (MapFunction<Tuple2<TupleTag<?>, WindowedValue<?>>, WindowedValue<?>>)
                value -> value._2,
            EncoderHelpers.encoder());
    context.putDatasetWildcard(output.getValue(), outputDataset);
  }

  static class SparkDoFnFilterFunction implements FilterFunction<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final TupleTag<?> key;

    public SparkDoFnFilterFunction(TupleTag<?> key) {
      this.key = key;
    }

    @Override
    public boolean call(Tuple2<TupleTag<?>, WindowedValue<?>> value) {
      return value._1.equals(key);
    }
  }
}
