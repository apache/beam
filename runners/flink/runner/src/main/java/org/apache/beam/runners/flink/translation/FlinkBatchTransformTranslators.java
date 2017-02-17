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
package org.apache.beam.runners.flink.translation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.functions.FlinkDoFnFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMergingNonShuffleReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMergingPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMergingReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMultiOutputDoFnFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkMultiOutputPruningFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkReduceFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.KvKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.SourceInputFormat;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.util.Collector;

/**
 * Translators for transforming {@link PTransform PTransforms} to
 * Flink {@link DataSet DataSets}.
 */
class FlinkBatchTransformTranslators {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Map<
      Class<? extends PTransform>,
      FlinkBatchPipelineTranslator.BatchTransformTranslator> TRANSLATORS = new HashMap<>();

  static {
    TRANSLATORS.put(View.CreatePCollectionView.class, new CreatePCollectionViewTranslatorBatch());

    TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslatorBatch());
    TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslatorBatch());
    TRANSLATORS.put(Reshuffle.class, new ReshuffleTranslatorBatch());

    TRANSLATORS.put(Flatten.FlattenPCollectionList.class, new FlattenPCollectionTranslatorBatch());

    TRANSLATORS.put(Window.Bound.class, new WindowBoundTranslatorBatch());

    TRANSLATORS.put(ParDo.Bound.class, new ParDoBoundTranslatorBatch());
    TRANSLATORS.put(ParDo.BoundMulti.class, new ParDoBoundMultiTranslatorBatch());

    TRANSLATORS.put(Read.Bounded.class, new ReadSourceTranslatorBatch());
  }


  static FlinkBatchPipelineTranslator.BatchTransformTranslator<?> getTranslator(
      PTransform<?, ?> transform) {
    return TRANSLATORS.get(transform.getClass());
  }

  private static class ReadSourceTranslatorBatch<T>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<Read.Bounded<T>> {

    @Override
    public void translateNode(Read.Bounded<T> transform, FlinkBatchTranslationContext context) {
      String name = transform.getName();
      BoundedSource<T> source = transform.getSource();
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> typeInformation = context.getTypeInfo(output);

      DataSource<WindowedValue<T>> dataSource = new DataSource<>(
          context.getExecutionEnvironment(),
          new SourceInputFormat<>(source, context.getPipelineOptions()),
          typeInformation,
          name);

      context.setOutputDataSet(output, dataSource);
    }
  }

  private static class WindowBoundTranslatorBatch<T>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<Window.Bound<T>> {

    @Override
    public void translateNode(Window.Bound<T> transform, FlinkBatchTranslationContext context) {
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

      DataSet<WindowedValue<T>> resultDataSet = inputDataSet
          .flatMap(assignWindowsFunction)
          .name(context.getOutput(transform).getName())
          .returns(resultTypeInfo);

      context.setOutputDataSet(context.getOutput(transform), resultDataSet);
    }
  }

  private static class GroupByKeyTranslatorBatch<K, InputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<GroupByKey<K, InputT>> {

    @Override
    public void translateNode(
        GroupByKey<K, InputT> transform,
        FlinkBatchTranslationContext context) {

      // for now, this is copied from the Combine.PerKey translater. Once we have the new runner API
      // we can replace GroupByKey by a Combine.PerKey with the Concatenate CombineFn

      DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      Combine.KeyedCombineFn<K, InputT, List<InputT>, List<InputT>> combineFn =
          new Concatenate<InputT>().asKeyedFn();

      KvCoder<K, InputT> inputCoder =
          (KvCoder<K, InputT>) context.getInput(transform).getCoder();

      Coder<List<InputT>> accumulatorCoder;

      try {
        accumulatorCoder =
            combineFn.getAccumulatorCoder(
                context.getInput(transform).getPipeline().getCoderRegistry(),
                inputCoder.getKeyCoder(),
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
                  windowingStrategy.getWindowFn().windowCoder()));


      Grouping<WindowedValue<KV<K, InputT>>> inputGrouping =
          inputDataSet.groupBy(new KvKeySelector<InputT, K>(inputCoder.getKeyCoder()));

      FlinkPartialReduceFunction<K, InputT, List<InputT>, ?> partialReduceFunction;
      FlinkReduceFunction<K, List<InputT>, List<InputT>, ?> reduceFunction;

      if (windowingStrategy.getWindowFn().isNonMerging()) {
        @SuppressWarnings("unchecked")
        WindowingStrategy<?, BoundedWindow> boundedStrategy =
            (WindowingStrategy<?, BoundedWindow>) windowingStrategy;

        partialReduceFunction = new FlinkPartialReduceFunction<>(
            combineFn,
            boundedStrategy,
            Collections.<PCollectionView<?>, WindowingStrategy<?, ?>>emptyMap(),
            context.getPipelineOptions());

        reduceFunction = new FlinkReduceFunction<>(
            combineFn,
            boundedStrategy,
            Collections.<PCollectionView<?>, WindowingStrategy<?, ?>>emptyMap(),
            context.getPipelineOptions());

      } else {
        if (!windowingStrategy.getWindowFn().windowCoder().equals(IntervalWindow.getCoder())) {
          throw new UnsupportedOperationException(
              "Merging WindowFn with windows other than IntervalWindow are not supported.");
        }

        @SuppressWarnings("unchecked")
        WindowingStrategy<?, IntervalWindow> intervalStrategy =
            (WindowingStrategy<?, IntervalWindow>) windowingStrategy;

        partialReduceFunction = new FlinkMergingPartialReduceFunction<>(
            combineFn,
            intervalStrategy,
            Collections.<PCollectionView<?>, WindowingStrategy<?, ?>>emptyMap(),
            context.getPipelineOptions());

        reduceFunction = new FlinkMergingReduceFunction<>(
            combineFn,
            intervalStrategy,
            Collections.<PCollectionView<?>, WindowingStrategy<?, ?>>emptyMap(),
            context.getPipelineOptions());
      }

      // Partially GroupReduce the values into the intermediate format AccumT (combine)
      GroupCombineOperator<
          WindowedValue<KV<K, InputT>>,
          WindowedValue<KV<K, List<InputT>>>> groupCombine =
          new GroupCombineOperator<>(
              inputGrouping,
              partialReduceTypeInfo,
              partialReduceFunction,
              "GroupCombine: " + transform.getName());

      Grouping<WindowedValue<KV<K, List<InputT>>>> intermediateGrouping =
          groupCombine.groupBy(new KvKeySelector<List<InputT>, K>(inputCoder.getKeyCoder()));

      // Fully reduce the values and create output format VO
      GroupReduceOperator<
          WindowedValue<KV<K, List<InputT>>>, WindowedValue<KV<K, List<InputT>>>> outputDataSet =
          new GroupReduceOperator<>(
              intermediateGrouping, partialReduceTypeInfo, reduceFunction, transform.getName());

      context.setOutputDataSet(context.getOutput(transform), outputDataSet);

    }

  }

  private static class ReshuffleTranslatorBatch<K, InputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<Reshuffle<K, InputT>> {

    @Override
    public void translateNode(
        Reshuffle<K, InputT> transform,
        FlinkBatchTranslationContext context) {

      DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      context.setOutputDataSet(context.getOutput(transform), inputDataSet.rebalance());

    }

  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use to translate {@link GroupByKey}. For a large {@link PCollection} this
   * is expected to crash!
   *
   * <p>This is copied from the dataflow runner code.
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }


  private static class CombinePerKeyTranslatorBatch<K, InputT, AccumT, OutputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          Combine.PerKey<K, InputT, OutputT>> {

    @Override
    @SuppressWarnings("unchecked")
    public void translateNode(
        Combine.PerKey<K, InputT, OutputT> transform,
        FlinkBatchTranslationContext context) {
      DataSet<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, OutputT> combineFn =
          (CombineFnBase.PerKeyCombineFn<K, InputT, AccumT, OutputT>) transform.getFn();

      KvCoder<K, InputT> inputCoder =
          (KvCoder<K, InputT>) context.getInput(transform).getCoder();

      Coder<AccumT> accumulatorCoder;

      try {
        accumulatorCoder =
            combineFn.getAccumulatorCoder(
                context.getInput(transform).getPipeline().getCoderRegistry(),
                inputCoder.getKeyCoder(),
                inputCoder.getValueCoder());
      } catch (CannotProvideCoderException e) {
        throw new RuntimeException(e);
      }

      WindowingStrategy<?, ?> windowingStrategy =
          context.getInput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<KV<K, AccumT>>> partialReduceTypeInfo =
          context.getTypeInfo(
              KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder),
              windowingStrategy);

      Grouping<WindowedValue<KV<K, InputT>>> inputGrouping =
          inputDataSet.groupBy(new KvKeySelector<InputT, K>(inputCoder.getKeyCoder()));

      // construct a map from side input to WindowingStrategy so that
      // the OldDoFn runner can map main-input windows to side input windows
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
      for (PCollectionView<?> sideInput: transform.getSideInputs()) {
        sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
      }

      if (windowingStrategy.getWindowFn().isNonMerging()) {
        WindowingStrategy<?, BoundedWindow> boundedStrategy =
            (WindowingStrategy<?, BoundedWindow>) windowingStrategy;

        FlinkPartialReduceFunction<K, InputT, AccumT, ?> partialReduceFunction =
            new FlinkPartialReduceFunction<>(
                combineFn,
                boundedStrategy,
                sideInputStrategies,
                context.getPipelineOptions());

        FlinkReduceFunction<K, AccumT, OutputT, ?> reduceFunction =
            new FlinkReduceFunction<>(
                combineFn,
                boundedStrategy,
                sideInputStrategies,
                context.getPipelineOptions());

        // Partially GroupReduce the values into the intermediate format AccumT (combine)
        GroupCombineOperator<
            WindowedValue<KV<K, InputT>>,
            WindowedValue<KV<K, AccumT>>> groupCombine =
            new GroupCombineOperator<>(
                inputGrouping,
                partialReduceTypeInfo,
                partialReduceFunction,
                "GroupCombine: " + transform.getName());

        transformSideInputs(transform.getSideInputs(), groupCombine, context);

        TypeInformation<WindowedValue<KV<K, OutputT>>> reduceTypeInfo =
            context.getTypeInfo(context.getOutput(transform));

        Grouping<WindowedValue<KV<K, AccumT>>> intermediateGrouping =
            groupCombine.groupBy(new KvKeySelector<AccumT, K>(inputCoder.getKeyCoder()));

        // Fully reduce the values and create output format OutputT
        GroupReduceOperator<
            WindowedValue<KV<K, AccumT>>, WindowedValue<KV<K, OutputT>>> outputDataSet =
            new GroupReduceOperator<>(
                intermediateGrouping, reduceTypeInfo, reduceFunction, transform.getName());

        transformSideInputs(transform.getSideInputs(), outputDataSet, context);

        context.setOutputDataSet(context.getOutput(transform), outputDataSet);

      } else {
        if (!windowingStrategy.getWindowFn().windowCoder().equals(IntervalWindow.getCoder())) {
          throw new UnsupportedOperationException(
              "Merging WindowFn with windows other than IntervalWindow are not supported.");
        }

        // for merging windows we can't to a pre-shuffle combine step since
        // elements would not be in their correct windows for side-input access

        WindowingStrategy<?, IntervalWindow> intervalStrategy =
            (WindowingStrategy<?, IntervalWindow>) windowingStrategy;

        FlinkMergingNonShuffleReduceFunction<K, InputT, AccumT, OutputT, ?> reduceFunction =
            new FlinkMergingNonShuffleReduceFunction<>(
                combineFn,
                intervalStrategy,
                sideInputStrategies,
                context.getPipelineOptions());

        TypeInformation<WindowedValue<KV<K, OutputT>>> reduceTypeInfo =
            context.getTypeInfo(context.getOutput(transform));

        Grouping<WindowedValue<KV<K, InputT>>> grouping =
            inputDataSet.groupBy(new KvKeySelector<InputT, K>(inputCoder.getKeyCoder()));

        // Fully reduce the values and create output format OutputT
        GroupReduceOperator<
            WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, OutputT>>> outputDataSet =
            new GroupReduceOperator<>(
                grouping, reduceTypeInfo, reduceFunction, transform.getName());

        transformSideInputs(transform.getSideInputs(), outputDataSet, context);

        context.setOutputDataSet(context.getOutput(transform), outputDataSet);
      }


    }
  }

  private static void rejectSplittable(DoFn<?, ?> doFn) {
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    if (signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support splittable DoFn: %s",
              FlinkRunner.class.getSimpleName(), doFn));
    }
  }

  private static void rejectStateAndTimers(DoFn<?, ?> doFn) {
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    if (signature.stateDeclarations().size() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
              DoFn.StateId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              FlinkRunner.class.getSimpleName()));
    }

    if (signature.timerDeclarations().size() > 0) {
      throw new UnsupportedOperationException(
          String.format(
              "Found %s annotations on %s, but %s cannot yet be used with timers in the %s.",
              DoFn.TimerId.class.getSimpleName(),
              doFn.getClass().getName(),
              DoFn.class.getSimpleName(),
              FlinkRunner.class.getSimpleName()));
    }
  }

  private static class ParDoBoundTranslatorBatch<InputT, OutputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          ParDo.Bound<InputT, OutputT>> {

    @Override
    public void translateNode(
        ParDo.Bound<InputT, OutputT> transform,

        FlinkBatchTranslationContext context) {
      DoFn<InputT, OutputT> doFn = transform.getFn();
      rejectSplittable(doFn);
      rejectStateAndTimers(doFn);

      DataSet<WindowedValue<InputT>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      TypeInformation<WindowedValue<OutputT>> typeInformation =
          context.getTypeInfo(context.getOutput(transform));

      List<PCollectionView<?>> sideInputs = transform.getSideInputs();

      // construct a map from side input to WindowingStrategy so that
      // the OldDoFn runner can map main-input windows to side input windows
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
      for (PCollectionView<?> sideInput: sideInputs) {
        sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
      }

      FlinkDoFnFunction<InputT, OutputT> doFnWrapper =
          new FlinkDoFnFunction<>(
              doFn,
              context.getOutput(transform).getWindowingStrategy(),
              sideInputStrategies,
              context.getPipelineOptions());

      MapPartitionOperator<WindowedValue<InputT>, WindowedValue<OutputT>> outputDataSet =
          new MapPartitionOperator<>(
              inputDataSet,
              typeInformation,
              doFnWrapper,
              transform.getName());

      transformSideInputs(sideInputs, outputDataSet, context);

      context.setOutputDataSet(context.getOutput(transform), outputDataSet);
    }
  }

  private static class ParDoBoundMultiTranslatorBatch<InputT, OutputT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          ParDo.BoundMulti<InputT, OutputT>> {

    @Override
    public void translateNode(
        ParDo.BoundMulti<InputT, OutputT> transform,
        FlinkBatchTranslationContext context) {
      DoFn<InputT, OutputT> doFn = transform.getFn();
      rejectSplittable(doFn);
      rejectStateAndTimers(doFn);
      DataSet<WindowedValue<InputT>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      List<TaggedPValue> outputs = context.getOutputs(transform);

      Map<TupleTag<?>, Integer> outputMap = Maps.newHashMap();
      // put the main output at index 0, FlinkMultiOutputDoFnFunction  expects this
      outputMap.put(transform.getMainOutputTag(), 0);
      int count = 1;
      for (TaggedPValue taggedValue : outputs) {
        if (!outputMap.containsKey(taggedValue.getTag())) {
          outputMap.put(taggedValue.getTag(), count++);
        }
      }

      // assume that the windowing strategy is the same for all outputs
      WindowingStrategy<?, ?> windowingStrategy = null;

      // collect all output Coders and create a UnionCoder for our tagged outputs
      List<Coder<?>> outputCoders = Lists.newArrayList();
      for (TaggedPValue taggedValue : outputs) {
        checkState(
            taggedValue.getValue() instanceof PCollection,
            "Within ParDo, got a non-PCollection output %s of type %s",
            taggedValue.getValue(),
            taggedValue.getValue().getClass().getSimpleName());
        PCollection<?> coll = (PCollection<?>) taggedValue.getValue();
        outputCoders.add(coll.getCoder());
        windowingStrategy = coll.getWindowingStrategy();
      }

      if (windowingStrategy == null) {
        throw new IllegalStateException("No outputs defined.");
      }

      UnionCoder unionCoder = UnionCoder.of(outputCoders);

      TypeInformation<WindowedValue<RawUnionValue>> typeInformation =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(
                  unionCoder,
                  windowingStrategy.getWindowFn().windowCoder()));

      List<PCollectionView<?>> sideInputs = transform.getSideInputs();

      // construct a map from side input to WindowingStrategy so that
      // the OldDoFn runner can map main-input windows to side input windows
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
      for (PCollectionView<?> sideInput: sideInputs) {
        sideInputStrategies.put(sideInput, sideInput.getWindowingStrategyInternal());
      }

      @SuppressWarnings("unchecked")
      FlinkMultiOutputDoFnFunction<InputT, OutputT> doFnWrapper =
          new FlinkMultiOutputDoFnFunction(
              doFn,
              windowingStrategy,
              sideInputStrategies,
              context.getPipelineOptions(),
              outputMap);

      MapPartitionOperator<WindowedValue<InputT>, WindowedValue<RawUnionValue>> taggedDataSet =
          new MapPartitionOperator<>(
              inputDataSet,
              typeInformation,
              doFnWrapper,
              transform.getName());

      transformSideInputs(sideInputs, taggedDataSet, context);

      for (TaggedPValue output : outputs) {
        pruneOutput(
            taggedDataSet,
            context,
            outputMap.get(output.getTag()),
            (PCollection) output.getValue());
      }
    }

    private <T> void pruneOutput(
        MapPartitionOperator<WindowedValue<InputT>, WindowedValue<RawUnionValue>> taggedDataSet,
        FlinkBatchTranslationContext context,
        int integerTag,
        PCollection<T> collection) {
      TypeInformation<WindowedValue<T>> outputType = context.getTypeInfo(collection);

      FlinkMultiOutputPruningFunction<T> pruningFunction =
          new FlinkMultiOutputPruningFunction<>(integerTag);

      FlatMapOperator<WindowedValue<RawUnionValue>, WindowedValue<T>> pruningOperator =
          new FlatMapOperator<>(
              taggedDataSet,
              outputType,
              pruningFunction,
              collection.getName());

      context.setOutputDataSet(collection, pruningOperator);
    }
  }

  private static class FlattenPCollectionTranslatorBatch<T>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          Flatten.FlattenPCollectionList<T>> {

    @Override
    @SuppressWarnings("unchecked")
    public void translateNode(
        Flatten.FlattenPCollectionList<T> transform,
        FlinkBatchTranslationContext context) {

      List<TaggedPValue> allInputs = context.getInputs(transform);
      DataSet<WindowedValue<T>> result = null;

      if (allInputs.isEmpty()) {

        // create an empty dummy source to satisfy downstream operations
        // we cannot create an empty source in Flink, therefore we have to
        // add the flatMap that simply never forwards the single element
        DataSource<String> dummySource =
            context.getExecutionEnvironment().fromElements("dummy");
        result = dummySource.flatMap(new FlatMapFunction<String, WindowedValue<T>>() {
          @Override
          public void flatMap(String s, Collector<WindowedValue<T>> collector) throws Exception {
            // never return anything
          }
        }).returns(
            new CoderTypeInformation<>(
                WindowedValue.getFullCoder(
                    (Coder<T>) VoidCoder.of(),
                    GlobalWindow.Coder.INSTANCE)));
      } else {
        for (TaggedPValue taggedPc : allInputs) {
          checkArgument(
              taggedPc.getValue() instanceof PCollection,
              "Got non-PCollection input to flatten: %s of type %s",
              taggedPc.getValue(),
              taggedPc.getValue().getClass().getSimpleName());
          PCollection<T> collection = (PCollection<T>) taggedPc.getValue();
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
      result = result.filter(new FilterFunction<WindowedValue<T>>() {
        @Override
        public boolean filter(WindowedValue<T> tWindowedValue) throws Exception {
          return true;
        }
      }).name("UnionFixFilter");
      context.setOutputDataSet(context.getOutput(transform), result);
    }
  }

  private static class CreatePCollectionViewTranslatorBatch<ElemT, ViewT>
      implements FlinkBatchPipelineTranslator.BatchTransformTranslator<
          View.CreatePCollectionView<ElemT, ViewT>> {

    @Override
    public void translateNode(
        View.CreatePCollectionView<ElemT, ViewT> transform,
        FlinkBatchTranslationContext context) {
      DataSet<WindowedValue<ElemT>> inputDataSet =
          context.getInputDataSet(context.getInput(transform));

      PCollectionView<ViewT> input = transform.getView();

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

  private FlinkBatchTransformTranslators() {}

}
