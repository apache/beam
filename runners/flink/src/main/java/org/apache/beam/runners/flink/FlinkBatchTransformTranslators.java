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
package org.apache.beam.runners.flink;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.runners.core.Concatenate;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.functions.FlinkDoFnFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkExplodeWindowsFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkIdentityFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMergingNonShuffleReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMultiOutputPruningFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkNonMergingReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkStatefulDoFnFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.KvKeySelector;
import org.apache.beam.runners.flink.translation.types.WindowedKvKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.ImpulseInputFormat;
import org.apache.beam.runners.flink.translation.wrappers.SourceInputFormat;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CreatePCollectionViewTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.ReadTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.MultimapBuilder;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Translators for transforming {@link PTransform PTransforms} to Flink {@link DataSet DataSets}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class FlinkBatchTransformTranslators {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Multimap<String, FlinkBatchPipelineTranslator.BatchTransformTranslator>
      TRANSLATORS = MultimapBuilder.hashKeys().arrayListValues().build();

  static {
    TRANSLATORS.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslatorBatch());
    TRANSLATORS.put(
        PTransformTranslation.CREATE_VIEW_TRANSFORM_URN,
        new CreatePCollectionViewTranslatorBatch<>());
    TRANSLATORS.put(
        PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new CombinePerKeyTranslatorBatch<>());
    TRANSLATORS.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        new NonMergingGroupByKeyTranslatorBatch<>());
    TRANSLATORS.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslatorBatch<>());
    TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslatorBatch<>());
    TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionTranslatorBatch<>());
    TRANSLATORS.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslatorBatch<>());
    TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslatorBatch<>());
    TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslatorBatch<>());
  }

  @SuppressWarnings("unchecked")
  static FlinkBatchPipelineTranslator.BatchTransformTranslator<?> getTranslator(
      PTransform<?, ?> transform, FlinkBatchTranslationContext context) {
    final @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    if (urn != null && TRANSLATORS.containsKey(urn)) {
      for (FlinkBatchPipelineTranslator.BatchTransformTranslator<PTransform<?, ?>> translator :
          TRANSLATORS.get(urn)) {
        if (translator.canTranslate(transform, context)) {
          return translator;
        }
      }
    }
    return null;
  }

  private static String getCurrentTransformName(FlinkBatchTranslationContext context) {
    return context.getCurrentTransform().getFullName();
  }

  private static class ImpulseTranslatorBatch
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PBegin, PCollection<byte[]>>> {

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<byte[]>> transform, FlinkBatchTranslationContext context) {
      String name = transform.getName();
      PCollection<byte[]> output = context.getOutput(transform);

      TypeInformation<WindowedValue<byte[]>> typeInformation = context.getTypeInfo(output);
      DataSource<WindowedValue<byte[]>> dataSource =
          new DataSource<>(
              context.getExecutionEnvironment(), new ImpulseInputFormat(), typeInformation, name);

      context.setOutputDataSet(output, dataSource);
    }
  }

  private static class ReadSourceTranslatorBatch<T>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PBegin, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<T>> transform, FlinkBatchTranslationContext context) {
      @SuppressWarnings("unchecked")
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> application =
          (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
              context.getCurrentTransform();
      BoundedSource<T> source;
      try {
        source = ReadTranslation.boundedSourceFromTransform(application);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> typeInformation = context.getTypeInfo(output);

      String fullName = getCurrentTransformName(context);

      DataSource<WindowedValue<T>> dataSource =
          new DataSource<>(
              context.getExecutionEnvironment(),
              new SourceInputFormat<>(fullName, source, context.getPipelineOptions()),
              typeInformation,
              fullName);

      context.setOutputDataSet(output, dataSource);
    }
  }

  private static class WindowAssignTranslatorBatch<T>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollection<T>, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<T>, PCollection<T>> transform,
        FlinkBatchTranslationContext context) {
      PValue input = context.getInput(transform);

      TypeInformation<WindowedValue<T>> resultTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DataSet<WindowedValue<T>> inputDataSet = context.getInputDataSet(input);

      @SuppressWarnings("unchecked")
      final WindowingStrategy<T, ? extends BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, ? extends BoundedWindow>)
              context.getOutput(transform).getWindowingStrategy();

      WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

      FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
          new FlinkAssignWindows<>(windowFn);

      DataSet<WindowedValue<T>> resultDataSet =
          inputDataSet
              .flatMap(assignWindowsFunction)
              .name(context.getOutput(transform).getName())
              .returns(resultTypeInfo);

      context.setOutputDataSet(context.getOutput(transform), resultDataSet);
    }
  }

  /**
   * Optimized group by key translation for non-merging windows.
   *
   * @param <K> Key type.
   * @param <InputT> Input type.
   */
  private static class NonMergingGroupByKeyTranslatorBatch<K, InputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

    @Override
    public boolean canTranslate(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> transform,
        FlinkBatchTranslationContext context) {
      final WindowingStrategy<?, ?> windowingStrategy =
          context.getInput(transform).getWindowingStrategy();
      return !windowingStrategy.needsMerge()
          && windowingStrategy.getTimestampCombiner() == TimestampCombiner.END_OF_WINDOW
          && windowingStrategy.getWindowFn().windowCoder().consistentWithEquals();
    }

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> transform,
        FlinkBatchTranslationContext context) {
      final int numConsumers =
          context.getOutputs(transform).values().stream().mapToInt(context::getNumConsumers).sum();
      final boolean multipleConsumers = numConsumers > 1;
      final boolean reIterableResult =
          multipleConsumers
              || context
                  .getPipelineOptions()
                  .as(FlinkPipelineOptions.class)
                  .getReIterableGroupByKeyResult();
      final DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));
      final KvCoder<K, InputT> inputCoder =
          (KvCoder<K, InputT>) context.getInput(transform).getCoder();
      final WindowingStrategy<?, ?> windowingStrategy =
          context.getInput(transform).getWindowingStrategy();
      final String fullName = getCurrentTransformName(context);
      final UnsortedGrouping<WindowedValue<KV<K, InputT>>> inputGrouping =
          new FlatMapOperator<>(
                  inputDataSet,
                  inputDataSet.getType(),
                  new FlinkExplodeWindowsFunction<>(),
                  "ExplodeWindows: " + fullName)
              .groupBy(
                  new WindowedKvKeySelector<>(
                      inputCoder.getKeyCoder(), windowingStrategy.getWindowFn().windowCoder()));
      final TypeInformation<WindowedValue<KV<K, Iterable<InputT>>>> outputTypeInfo =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(
                  KvCoder.of(
                      inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getValueCoder())),
                  windowingStrategy.getWindowFn().windowCoder()),
              context.getPipelineOptions());
      final DataSet<WindowedValue<KV<K, Iterable<InputT>>>> outputDataSet =
          new GroupReduceOperator<>(
                  inputGrouping,
                  outputTypeInfo,
                  new FlinkNonMergingReduceFunction<>(windowingStrategy, reIterableResult),
                  fullName)
              .returns(outputTypeInfo);
      context.setOutputDataSet(context.getOutput(transform), outputDataSet);
    }
  }

  private static class GroupByKeyTranslatorBatch<K, InputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> transform,
        FlinkBatchTranslationContext context) {

      // for now, this is copied from the Combine.PerKey translator. Once we have the new runner API
      // we can replace GroupByKey by a Combine.PerKey with the Concatenate CombineFn

      DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      Combine.CombineFn<InputT, List<InputT>, List<InputT>> combineFn = new Concatenate<>();

      KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) context.getInput(transform).getCoder();

      Coder<List<InputT>> accumulatorCoder;

      try {
        accumulatorCoder =
            combineFn.getAccumulatorCoder(
                context.getInput(transform).getPipeline().getCoderRegistry(),
                inputCoder.getValueCoder());
      } catch (CannotProvideCoderException e) {
        throw new RuntimeException(e);
      }

      WindowingStrategy<?, ?> windowingStrategy =
          context.getInput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<KV<K, List<InputT>>>> partialReduceTypeInfo =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(
                  KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder),
                  windowingStrategy.getWindowFn().windowCoder()),
              context.getPipelineOptions());

      Grouping<WindowedValue<KV<K, InputT>>> inputGrouping =
          inputDataSet.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

      @SuppressWarnings("unchecked")
      WindowingStrategy<Object, BoundedWindow> boundedStrategy =
          (WindowingStrategy<Object, BoundedWindow>) windowingStrategy;

      FlinkPartialReduceFunction<K, InputT, List<InputT>, ?> partialReduceFunction =
          new FlinkPartialReduceFunction<>(
              combineFn, boundedStrategy, Collections.emptyMap(), context.getPipelineOptions());

      FlinkReduceFunction<K, List<InputT>, List<InputT>, ?> reduceFunction =
          new FlinkReduceFunction<>(
              combineFn, boundedStrategy, Collections.emptyMap(), context.getPipelineOptions());

      // Partially GroupReduce the values into the intermediate format AccumT (combine)
      String fullName = getCurrentTransformName(context);
      GroupCombineOperator<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, List<InputT>>>>
          groupCombine =
              new GroupCombineOperator<>(
                  inputGrouping,
                  partialReduceTypeInfo,
                  partialReduceFunction,
                  "GroupCombine: " + fullName);

      Grouping<WindowedValue<KV<K, List<InputT>>>> intermediateGrouping =
          groupCombine.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

      // Fully reduce the values and create output format VO
      GroupReduceOperator<WindowedValue<KV<K, List<InputT>>>, WindowedValue<KV<K, List<InputT>>>>
          outputDataSet =
              new GroupReduceOperator<>(
                  intermediateGrouping, partialReduceTypeInfo, reduceFunction, fullName);

      context.setOutputDataSet(context.getOutput(transform), outputDataSet);
    }
  }

  private static class ReshuffleTranslatorBatch<K, InputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<Reshuffle<K, InputT>> {

    @Override
    public void translateNode(
        Reshuffle<K, InputT> transform, FlinkBatchTranslationContext context) {
      final DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));
      // Construct an instance of CoderTypeInformation which contains the pipeline options.
      // This will be used to initialized FileSystems.
      final CoderTypeInformation<WindowedValue<KV<K, InputT>>> outputType =
          ((CoderTypeInformation<WindowedValue<KV<K, InputT>>>) inputDataSet.getType())
              .withPipelineOptions(context.getPipelineOptions());
      // We insert a NOOP here to initialize the FileSystems via the above CoderTypeInformation.
      // The output type coder may be relying on file system access. The shuffled data may have to
      // be deserialized on a different machine using this coder where FileSystems has not been
      // initialized.
      final DataSet<WindowedValue<KV<K, InputT>>> retypedDataSet =
          new MapOperator<>(
              inputDataSet,
              outputType,
              FlinkIdentityFunction.of(),
              getCurrentTransformName(context));
      WindowedValue.WindowedValueCoder<KV<K, InputT>> kvWvCoder =
          (WindowedValue.WindowedValueCoder<KV<K, InputT>>) outputType.getCoder();
      KvCoder<K, InputT> kvCoder = (KvCoder<K, InputT>) kvWvCoder.getValueCoder();
      DataSet<WindowedValue<KV<K, InputT>>> reshuffle =
          retypedDataSet
              .groupBy(new KvKeySelector<>(kvCoder.getKeyCoder()))
              .<WindowedValue<KV<K, InputT>>>reduceGroup((i, c) -> i.forEach(c::collect))
              .returns(outputType);
      context.setOutputDataSet(context.getOutput(transform), reshuffle);
    }
  }

  private static class CombinePerKeyTranslatorBatch<K, InputT, AccumT, OutputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

    @Override
    @SuppressWarnings("unchecked")
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
        FlinkBatchTranslationContext context) {
      final DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));
      final Combine.PerKey<K, InputT, OutputT> combineTransform =
          (Combine.PerKey<K, InputT, OutputT>) transform;
      final CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn =
          (CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT>) combineTransform.getFn();
      final KvCoder<K, InputT> inputCoder =
          (KvCoder<K, InputT>) context.getInput(transform).getCoder();
      final Coder<AccumT> accumulatorCoder;
      try {
        accumulatorCoder =
            combineFn.getAccumulatorCoder(
                context.getInput(transform).getPipeline().getCoderRegistry(),
                inputCoder.getValueCoder());
      } catch (CannotProvideCoderException e) {
        throw new RuntimeException(e);
      }

      final WindowingStrategy<Object, BoundedWindow> windowingStrategy =
          (WindowingStrategy<Object, BoundedWindow>)
              context.getInput(transform).getWindowingStrategy();

      final TypeInformation<WindowedValue<KV<K, AccumT>>> partialReduceTypeInfo =
          context.getTypeInfo(
              KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder), windowingStrategy);

      // In case of non-merging windows, we can pre-group elements by window to make code path more
      // efficient.
      final boolean canGroupByWindow =
          windowingStrategy.getWindowFn().isNonMerging()
              && windowingStrategy.getWindowFn().windowCoder().consistentWithEquals();

      final String fullName = getCurrentTransformName(context);

      final UnsortedGrouping<WindowedValue<KV<K, InputT>>> inputGrouping;
      if (canGroupByWindow) {
        inputGrouping =
            new FlatMapOperator<>(
                    inputDataSet,
                    inputDataSet.getType(),
                    new FlinkExplodeWindowsFunction<>(),
                    "ExplodeWindows: " + fullName)
                .groupBy(
                    new WindowedKvKeySelector<>(
                        inputCoder.getKeyCoder(), windowingStrategy.getWindowFn().windowCoder()));
      } else {
        inputGrouping = inputDataSet.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));
      }

      // construct a map from side input to WindowingStrategy so that
      // the DoFn runner can map main-input windows to side input windows
      final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
      for (PCollectionView<?> sideInput : combineTransform.getSideInputs()) {
        sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
      }

      if (!windowingStrategy.needsMerge()) {
        final FlinkPartialReduceFunction<K, InputT, AccumT, ?> partialReduceFunction =
            new FlinkPartialReduceFunction<>(
                combineFn,
                windowingStrategy,
                sideInputStrategies,
                context.getPipelineOptions(),
                canGroupByWindow);

        final FlinkReduceFunction<K, AccumT, OutputT, ?> reduceFunction =
            new FlinkReduceFunction<>(
                combineFn,
                windowingStrategy,
                sideInputStrategies,
                context.getPipelineOptions(),
                canGroupByWindow);

        // Partially GroupReduce the values into the intermediate format AccumT (combine)
        final GroupCombineOperator<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, AccumT>>>
            groupCombine =
                new GroupCombineOperator<>(
                    inputGrouping,
                    partialReduceTypeInfo,
                    partialReduceFunction,
                    "GroupCombine: " + fullName);

        transformSideInputs(combineTransform.getSideInputs(), groupCombine, context);

        final TypeInformation<WindowedValue<KV<K, OutputT>>> reduceTypeInfo =
            context.getTypeInfo(context.getOutput(transform));

        final Grouping<WindowedValue<KV<K, AccumT>>> intermediateGrouping;
        if (canGroupByWindow) {
          intermediateGrouping =
              groupCombine.groupBy(
                  new WindowedKvKeySelector<>(
                      inputCoder.getKeyCoder(), windowingStrategy.getWindowFn().windowCoder()));
        } else {
          intermediateGrouping =
              groupCombine.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));
        }

        // Fully reduce the values and create output format OutputT
        final GroupReduceOperator<WindowedValue<KV<K, AccumT>>, WindowedValue<KV<K, OutputT>>>
            outputDataSet =
                new GroupReduceOperator<>(
                    intermediateGrouping, reduceTypeInfo, reduceFunction, fullName);

        transformSideInputs(combineTransform.getSideInputs(), outputDataSet, context);

        context.setOutputDataSet(context.getOutput(transform), outputDataSet);

      } else {

        // for merging windows we can't to a pre-shuffle combine step since
        // elements would not be in their correct windows for side-input access

        final RichGroupReduceFunction<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, OutputT>>>
            reduceFunction =
                new FlinkMergingNonShuffleReduceFunction<>(
                    combineFn,
                    windowingStrategy,
                    sideInputStrategies,
                    context.getPipelineOptions());

        final TypeInformation<WindowedValue<KV<K, OutputT>>> reduceTypeInfo =
            context.getTypeInfo(context.getOutput(transform));

        final Grouping<WindowedValue<KV<K, InputT>>> grouping =
            inputDataSet.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

        // Fully reduce the values and create output format OutputT
        final GroupReduceOperator<WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, OutputT>>>
            outputDataSet =
                new GroupReduceOperator<>(grouping, reduceTypeInfo, reduceFunction, fullName);

        transformSideInputs(combineTransform.getSideInputs(), outputDataSet, context);

        context.setOutputDataSet(context.getOutput(transform), outputDataSet);
      }
    }
  }

  private static class ParDoTranslatorBatch<InputT, OutputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollection<InputT>, PCollectionTuple>> {

    @Override
    @SuppressWarnings("unchecked")
    public void translateNode(
        PTransform<PCollection<InputT>, PCollectionTuple> transform,
        FlinkBatchTranslationContext context) {
      DoFn<InputT, OutputT> doFn;
      try {
        doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      DoFnSignature signature = DoFnSignatures.signatureForDoFn(doFn);
      checkState(
          !signature.processElement().isSplittable(),
          "Not expected to directly translate splittable DoFn, should have been overridden: %s",
          doFn);
      DataSet<WindowedValue<InputT>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      Map<TupleTag<?>, PCollection<?>> outputs = context.getOutputs(transform);

      final TupleTag<OutputT> mainOutputTag;
      DoFnSchemaInformation doFnSchemaInformation;
      Map<String, PCollectionView<?>> sideInputMapping;
      try {
        mainOutputTag =
            (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      doFnSchemaInformation = ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
      sideInputMapping = ParDoTranslation.getSideInputMapping(context.getCurrentTransform());
      Map<TupleTag<?>, Integer> outputMap = Maps.newHashMap();
      // put the main output at index 0, FlinkMultiOutputDoFnFunction  expects this
      outputMap.put(mainOutputTag, 0);
      int count = 1;
      for (TupleTag<?> tag : outputs.keySet()) {
        if (!outputMap.containsKey(tag)) {
          outputMap.put(tag, count++);
        }
      }

      // Union coder elements must match the order of the output tags.
      Map<Integer, TupleTag<?>> indexMap = Maps.newTreeMap();
      for (Map.Entry<TupleTag<?>, Integer> entry : outputMap.entrySet()) {
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

      TypeInformation<WindowedValue<RawUnionValue>> typeInformation =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(unionCoder, windowingStrategy.getWindowFn().windowCoder()),
              context.getPipelineOptions());

      List<PCollectionView<?>> sideInputs;
      try {
        sideInputs = ParDoTranslation.getSideInputs(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // construct a map from side input to WindowingStrategy so that
      // the DoFn runner can map main-input windows to side input windows
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
      for (PCollectionView<?> sideInput : sideInputs) {
        sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
      }

      SingleInputUdfOperator<WindowedValue<InputT>, WindowedValue<RawUnionValue>, ?> outputDataSet;
      boolean usesStateOrTimers;
      try {
        usesStateOrTimers = ParDoTranslation.usesStateOrTimers(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      final Map<TupleTag<?>, Coder<?>> outputCoderMap = context.getOutputCoders(transform);

      String fullName = getCurrentTransformName(context);
      if (usesStateOrTimers) {
        KvCoder<?, ?> inputCoder = (KvCoder<?, ?>) context.getInput(transform).getCoder();
        FlinkStatefulDoFnFunction<?, ?, OutputT> doFnWrapper =
            new FlinkStatefulDoFnFunction<>(
                (DoFn) doFn,
                fullName,
                windowingStrategy,
                sideInputStrategies,
                context.getPipelineOptions(),
                outputMap,
                mainOutputTag,
                inputCoder,
                outputCoderMap,
                doFnSchemaInformation,
                sideInputMapping);

        // Based on the fact that the signature is stateful, DoFnSignatures ensures
        // that it is also keyed.
        Coder<Object> keyCoder = (Coder) inputCoder.getKeyCoder();
        final Grouping<WindowedValue<InputT>> grouping;
        if (signature.processElement().requiresTimeSortedInput()) {
          grouping =
              inputDataSet
                  .groupBy((KeySelector) new KvKeySelector<>(keyCoder))
                  .sortGroup(new KeyWithValueTimestampSelector<>(), Order.ASCENDING);
        } else {
          grouping = inputDataSet.groupBy((KeySelector) new KvKeySelector<>(keyCoder));
        }

        outputDataSet = new GroupReduceOperator(grouping, typeInformation, doFnWrapper, fullName);

      } else {
        final FlinkDoFnFunction<InputT, OutputT> doFnWrapper =
            new FlinkDoFnFunction<>(
                doFn,
                fullName,
                windowingStrategy,
                sideInputStrategies,
                context.getPipelineOptions(),
                outputMap,
                mainOutputTag,
                context.getInput(transform).getCoder(),
                outputCoderMap,
                doFnSchemaInformation,
                sideInputMapping);

        outputDataSet = new FlatMapOperator<>(inputDataSet, typeInformation, doFnWrapper, fullName);
      }

      transformSideInputs(sideInputs, outputDataSet, context);

      for (Entry<TupleTag<?>, PCollection<?>> output : outputs.entrySet()) {
        pruneOutput(
            outputDataSet,
            context,
            outputMap.get(output.getKey()),
            (PCollection) output.getValue());
      }
    }

    private <T> void pruneOutput(
        DataSet<WindowedValue<RawUnionValue>> taggedDataSet,
        FlinkBatchTranslationContext context,
        int integerTag,
        PCollection<T> collection) {
      TypeInformation<WindowedValue<T>> outputType = context.getTypeInfo(collection);

      FlinkMultiOutputPruningFunction<T> pruningFunction =
          new FlinkMultiOutputPruningFunction<>(integerTag, context.getPipelineOptions());

      FlatMapOperator<WindowedValue<RawUnionValue>, WindowedValue<T>> pruningOperator =
          new FlatMapOperator<>(taggedDataSet, outputType, pruningFunction, collection.getName());

      context.setOutputDataSet(collection, pruningOperator);
    }
  }

  private static class KeyWithValueTimestampSelector<K, V>
      implements KeySelector<WindowedValue<KV<K, V>>, Instant> {

    @Override
    public Instant getKey(WindowedValue<KV<K, V>> in) throws Exception {
      return in.getTimestamp();
    }
  }

  private static class FlattenPCollectionTranslatorBatch<T>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollectionList<T>, PCollection<T>>> {

    @Override
    @SuppressWarnings("unchecked")
    public void translateNode(
        PTransform<PCollectionList<T>, PCollection<T>> transform,
        FlinkBatchTranslationContext context) {

      Map<TupleTag<?>, PCollection<?>> allInputs = context.getInputs(transform);
      DataSet<WindowedValue<T>> result = null;

      if (allInputs.isEmpty()) {

        // create an empty dummy source to satisfy downstream operations
        // we cannot create an empty source in Flink, therefore we have to
        // add the flatMap that simply never forwards the single element
        DataSource<String> dummySource = context.getExecutionEnvironment().fromElements("dummy");
        result =
            dummySource
                .<WindowedValue<T>>flatMap(
                    (s, collector) -> {
                      // never return anything
                    })
                .returns(
                    new CoderTypeInformation<>(
                        WindowedValue.getFullCoder(
                            (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE),
                        context.getPipelineOptions()));
      } else {
        for (PValue taggedPc : allInputs.values()) {
          checkArgument(
              taggedPc instanceof PCollection,
              "Got non-PCollection input to flatten: %s of type %s",
              taggedPc,
              taggedPc.getClass().getSimpleName());
          PCollection<T> collection = (PCollection<T>) taggedPc;
          DataSet<WindowedValue<T>> current = context.getInputDataSet(collection);
          if (result == null) {
            result = current;
          } else {
            result = result.union(current);
          }
        }
      }

      // insert a dummy filter, there seems to be a bug in Flink
      // that produces duplicate elements after the union in some cases
      // if we don't
      result = result.filter(tWindowedValue -> true).name("UnionFixFilter");
      context.setOutputDataSet(context.getOutput(transform), result);
    }
  }

  private static class CreatePCollectionViewTranslatorBatch<ElemT, ViewT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          PTransform<PCollection<ElemT>, PCollection<ElemT>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<ElemT>, PCollection<ElemT>> transform,
        FlinkBatchTranslationContext context) {
      DataSet<WindowedValue<ElemT>> inputDataSet =
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
      PCollectionView<ViewT> input;
      try {
        input = CreatePCollectionViewTranslation.getView(application);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      context.setSideInputDataSet(input, inputDataSet);
    }
  }

  private static void transformSideInputs(
      List<PCollectionView<?>> sideInputs,
      SingleInputUdfOperator<?, ?, ?> outputDataSet,
      FlinkBatchTranslationContext context) {
    // get corresponding Flink broadcast DataSets
    for (PCollectionView<?> input : sideInputs) {
      DataSet<?> broadcastSet = context.getSideInputDataSet(input);
      outputDataSet.withBroadcastSet(broadcastSet, input.getTagInternal().getId());
    }
  }
}
