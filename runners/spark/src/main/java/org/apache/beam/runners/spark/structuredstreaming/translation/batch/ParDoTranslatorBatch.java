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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.MultiOuputCoder;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

/**
 * TODO: Add support for state and timers.
 *
 * @param <InputT>
 * @param <OutputT>
 */
class ParDoTranslatorBatch<InputT, OutputT>
    implements TransformTranslator<PTransform<PCollection<InputT>, PCollectionTuple>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<InputT>, PCollectionTuple> transform, TranslationContext context) {
    String stepName = context.getCurrentTransform().getFullName();

    // Check for not supported advanced features
    // TODO: add support of Splittable DoFn
    DoFn<InputT, OutputT> doFn = getDoFn(context);
    checkState(
        !DoFnSignatures.isSplittable(doFn),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    // TODO: add support of states and timers
    checkState(
        !DoFnSignatures.isStateful(doFn), "States and timers are not supported for the moment.");

    checkState(
        !DoFnSignatures.requiresTimeSortedInput(doFn),
        "@RequiresTimeSortedInput is not " + "supported for the moment");

    DoFnSchemaInformation doFnSchemaInformation =
        ParDoTranslation.getSchemaInformation(context.getCurrentTransform());

    // Init main variables
    PValue input = context.getInput();
    Dataset<WindowedValue<InputT>> inputDataSet = context.getDataset(input);
    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    TupleTag<?> mainOutputTag = getTupleTag(context);
    List<TupleTag<?>> outputTags = new ArrayList<>(outputs.keySet());
    WindowingStrategy<?, ?> windowingStrategy =
        ((PCollection<InputT>) input).getWindowingStrategy();
    Coder<InputT> inputCoder = ((PCollection<InputT>) input).getCoder();
    Coder<? extends BoundedWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();

    // construct a map from side input to WindowingStrategy so that
    // the DoFn runner can map main-input windows to side input windows
    List<PCollectionView<?>> sideInputs = getSideInputs(context);
    Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
    for (PCollectionView<?> sideInput : sideInputs) {
      sideInputStrategies.put(sideInput, sideInput.getPCollection().getWindowingStrategy());
    }

    SideInputBroadcast broadcastStateData = createBroadcastSideInputs(sideInputs, context);

    Map<TupleTag<?>, Coder<?>> outputCoderMap = context.getOutputCoders();
    MetricsContainerStepMapAccumulator metricsAccum = MetricsAccumulator.getInstance();

    List<TupleTag<?>> additionalOutputTags = new ArrayList<>();
    for (TupleTag<?> tag : outputTags) {
      if (!tag.equals(mainOutputTag)) {
        additionalOutputTags.add(tag);
      }
    }

    Map<String, PCollectionView<?>> sideInputMapping =
        ParDoTranslation.getSideInputMapping(context.getCurrentTransform());
    @SuppressWarnings("unchecked")
    DoFnFunction<InputT, OutputT> doFnWrapper =
        new DoFnFunction(
            metricsAccum,
            stepName,
            doFn,
            windowingStrategy,
            sideInputStrategies,
            context.getSerializableOptions(),
            additionalOutputTags,
            mainOutputTag,
            inputCoder,
            outputCoderMap,
            broadcastStateData,
            doFnSchemaInformation,
            sideInputMapping);

    MultiOuputCoder multipleOutputCoder =
        MultiOuputCoder.of(SerializableCoder.of(TupleTag.class), outputCoderMap, windowCoder);
    Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> allOutputs =
        inputDataSet.mapPartitions(doFnWrapper, EncoderHelpers.fromBeamCoder(multipleOutputCoder));
    if (outputs.entrySet().size() > 1) {
      allOutputs.persist();
      for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
        pruneOutputFilteredByTag(context, allOutputs, output, windowCoder);
      }
    } else {
      Coder<OutputT> outputCoder = ((PCollection<OutputT>) outputs.get(mainOutputTag)).getCoder();
      Coder<WindowedValue<?>> windowedValueCoder =
          (Coder<WindowedValue<?>>) (Coder<?>) WindowedValue.getFullCoder(outputCoder, windowCoder);
      Dataset<WindowedValue<?>> outputDataset =
          allOutputs.map(
              (MapFunction<Tuple2<TupleTag<?>, WindowedValue<?>>, WindowedValue<?>>)
                  value -> value._2,
              EncoderHelpers.fromBeamCoder(windowedValueCoder));
      context.putDatasetWildcard(outputs.entrySet().iterator().next().getValue(), outputDataset);
    }
  }

  private static SideInputBroadcast createBroadcastSideInputs(
      List<PCollectionView<?>> sideInputs, TranslationContext context) {
    JavaSparkContext jsc =
        JavaSparkContext.fromSparkContext(context.getSparkSession().sparkContext());

    SideInputBroadcast sideInputBroadcast = new SideInputBroadcast();
    for (PCollectionView<?> sideInput : sideInputs) {
      Coder<? extends BoundedWindow> windowCoder =
          sideInput.getPCollection().getWindowingStrategy().getWindowFn().windowCoder();

      Coder<WindowedValue<?>> windowedValueCoder =
          (Coder<WindowedValue<?>>)
              (Coder<?>)
                  WindowedValue.getFullCoder(sideInput.getPCollection().getCoder(), windowCoder);
      Dataset<WindowedValue<?>> broadcastSet = context.getSideInputDataSet(sideInput);
      List<WindowedValue<?>> valuesList = broadcastSet.collectAsList();
      List<byte[]> codedValues = new ArrayList<>();
      for (WindowedValue<?> v : valuesList) {
        codedValues.add(CoderHelpers.toByteArray(v, windowedValueCoder));
      }

      sideInputBroadcast.add(
          sideInput.getTagInternal().getId(), jsc.broadcast(codedValues), windowedValueCoder);
    }
    return sideInputBroadcast;
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
      Map.Entry<TupleTag<?>, PValue> output,
      Coder<? extends BoundedWindow> windowCoder) {
    Dataset<Tuple2<TupleTag<?>, WindowedValue<?>>> filteredDataset =
        allOutputs.filter(new DoFnFilterFunction(output.getKey()));
    Coder<WindowedValue<?>> windowedValueCoder =
        (Coder<WindowedValue<?>>)
            (Coder<?>)
                WindowedValue.getFullCoder(
                    ((PCollection<OutputT>) output.getValue()).getCoder(), windowCoder);
    Dataset<WindowedValue<?>> outputDataset =
        filteredDataset.map(
            (MapFunction<Tuple2<TupleTag<?>, WindowedValue<?>>, WindowedValue<?>>)
                value -> value._2,
            EncoderHelpers.fromBeamCoder(windowedValueCoder));
    context.putDatasetWildcard(output.getValue(), outputDataset);
  }

  static class DoFnFilterFunction implements FilterFunction<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final TupleTag<?> key;

    DoFnFilterFunction(TupleTag<?> key) {
      this.key = key;
    }

    @Override
    public boolean call(Tuple2<TupleTag<?>, WindowedValue<?>> value) {
      return value._1.equals(key);
    }
  }
}
