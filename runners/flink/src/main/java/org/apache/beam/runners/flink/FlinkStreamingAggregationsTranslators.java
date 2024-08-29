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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.PartialReduceBundleOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

public class FlinkStreamingAggregationsTranslators {
  public static class ConcatenateAsIterable<T> extends Combine.CombineFn<T, List<T>, Iterable<T>> {
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
    public Coder<Iterable<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return IterableCoder.of(inputCoder);
    }
  }

  private static <InputT, OutputT>
      CombineFnBase.GlobalCombineFn<Object, Object, OutputT> toFinalFlinkCombineFn(
          CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> combineFn,
          Coder<InputT> inputTCoder) {

    if (combineFn instanceof Combine.CombineFn) {
      return new Combine.CombineFn<Object, Object, OutputT>() {

        @SuppressWarnings("unchecked")
        final Combine.CombineFn<InputT, Object, OutputT> fn =
            (Combine.CombineFn<InputT, Object, OutputT>) combineFn;

        @Override
        public Object createAccumulator() {
          return fn.createAccumulator();
        }

        @Override
        public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder<Object> inputCoder)
            throws CannotProvideCoderException {
          return fn.getAccumulatorCoder(registry, inputTCoder);
        }

        @Override
        public Object addInput(Object mutableAccumulator, Object input) {
          return fn.mergeAccumulators(ImmutableList.of(mutableAccumulator, input));
        }

        @Override
        public Object mergeAccumulators(Iterable<Object> accumulators) {
          return fn.mergeAccumulators(accumulators);
        }

        @Override
        public OutputT extractOutput(Object accumulator) {
          return fn.extractOutput(accumulator);
        }
      };
    } else if (combineFn instanceof CombineWithContext.CombineFnWithContext) {
      return new CombineWithContext.CombineFnWithContext<Object, Object, OutputT>() {

        @SuppressWarnings("unchecked")
        final CombineWithContext.CombineFnWithContext<InputT, Object, OutputT> fn =
            (CombineWithContext.CombineFnWithContext<InputT, Object, OutputT>) combineFn;

        @Override
        public Object createAccumulator(CombineWithContext.Context c) {
          return fn.createAccumulator(c);
        }

        @Override
        public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder<Object> inputCoder)
            throws CannotProvideCoderException {
          return fn.getAccumulatorCoder(registry, inputTCoder);
        }

        @Override
        public Object addInput(Object accumulator, Object input, CombineWithContext.Context c) {
          return fn.mergeAccumulators(ImmutableList.of(accumulator, input), c);
        }

        @Override
        public Object mergeAccumulators(
            Iterable<Object> accumulators, CombineWithContext.Context c) {
          return fn.mergeAccumulators(accumulators, c);
        }

        @Override
        public OutputT extractOutput(Object accumulator, CombineWithContext.Context c) {
          return fn.extractOutput(accumulator, c);
        }
      };
    }
    throw new IllegalArgumentException(
        "Unsupported CombineFn implementation: " + combineFn.getClass());
  }

  /**
   * Create a DoFnOperator instance that group elements per window and apply a combine function on
   * them.
   */
  public static <K, InputAccumT, OutputAccumT, InputT, OutputT>
      WindowDoFnOperator<K, InputAccumT, OutputAccumT> getWindowedAggregateDoFnOperator(
          FlinkStreamingTranslationContext context,
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
          KvCoder<K, InputAccumT> inputKvCoder,
          Coder<WindowedValue<KV<K, OutputAccumT>>> outputCoder,
          SystemReduceFn<K, InputAccumT, ?, OutputAccumT, BoundedWindow> reduceFn,
          Map<Integer, PCollectionView<?>> sideInputTagMapping,
          List<PCollectionView<?>> sideInputs) {

    // Naming
    String fullName = FlinkStreamingTransformTranslators.getCurrentTransformName(context);
    TupleTag<KV<K, OutputAccumT>> mainTag = new TupleTag<>("main output");

    // input infos
    PCollection<KV<K, InputT>> input = context.getInput(transform);

    @SuppressWarnings("unchecked")
    WindowingStrategy<?, BoundedWindow> windowingStrategy =
        (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();
    SerializablePipelineOptions serializablePipelineOptions =
        new SerializablePipelineOptions(context.getPipelineOptions());

    // Coders
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();

    SingletonKeyedWorkItemCoder<K, InputAccumT> workItemCoder =
        SingletonKeyedWorkItemCoder.of(
            keyCoder, inputKvCoder.getValueCoder(), windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<KeyedWorkItem<K, InputAccumT>> windowedWorkItemCoder =
        WindowedValue.getFullCoder(workItemCoder, windowingStrategy.getWindowFn().windowCoder());

    // Key selector
    WorkItemKeySelector<K, InputAccumT> workItemKeySelector =
        new WorkItemKeySelector<>(keyCoder, serializablePipelineOptions);

    return new WindowDoFnOperator<>(
        reduceFn,
        fullName,
        (Coder) windowedWorkItemCoder,
        mainTag,
        Collections.emptyList(),
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            mainTag, outputCoder, serializablePipelineOptions),
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        context.getPipelineOptions(),
        keyCoder,
        workItemKeySelector);
  }

  public static <K, InputAccumT, OutputAccumT, InputT, OutputT>
      WindowDoFnOperator<K, InputAccumT, OutputAccumT> getWindowedAggregateDoFnOperator(
          FlinkStreamingTranslationContext context,
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
          KvCoder<K, InputAccumT> inputKvCoder,
          Coder<WindowedValue<KV<K, OutputAccumT>>> outputCoder,
          CombineFnBase.GlobalCombineFn<? super InputAccumT, ?, OutputAccumT> combineFn,
          Map<Integer, PCollectionView<?>> sideInputTagMapping,
          List<PCollectionView<?>> sideInputs) {

    // Combining fn
    SystemReduceFn<K, InputAccumT, ?, OutputAccumT, BoundedWindow> reduceFn =
        SystemReduceFn.combining(
            inputKvCoder.getKeyCoder(),
            AppliedCombineFn.withInputCoder(
                combineFn,
                context.getInput(transform).getPipeline().getCoderRegistry(),
                inputKvCoder));

    return getWindowedAggregateDoFnOperator(
        context, transform, inputKvCoder, outputCoder, reduceFn, sideInputTagMapping, sideInputs);
  }

  public static <K, InputT, AccumT, OutputT>
      SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> batchCombinePerKey(
          FlinkStreamingTranslationContext context,
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
          CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn,
          Map<Integer, PCollectionView<?>> sideInputTagMapping,
          List<PCollectionView<?>> sideInputs) {

    Coder<WindowedValue<KV<K, AccumT>>> windowedAccumCoder;
    KvCoder<K, AccumT> accumKvCoder;

    PCollection<KV<K, InputT>> input = context.getInput(transform);
    String fullName = FlinkStreamingTransformTranslators.getCurrentTransformName(context);
    DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);
    KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();
    Coder<WindowedValue<KV<K, OutputT>>> outputCoder =
        context.getWindowedInputCoder(context.getOutput(transform));
    SerializablePipelineOptions serializablePipelineOptions =
        new SerializablePipelineOptions(context.getPipelineOptions());
    TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo =
        context.getTypeInfo(context.getOutput(transform));

    try {
      Coder<AccumT> accumulatorCoder =
          combineFn.getAccumulatorCoder(
              input.getPipeline().getCoderRegistry(), inputKvCoder.getValueCoder());

      accumKvCoder = KvCoder.of(inputKvCoder.getKeyCoder(), accumulatorCoder);

      windowedAccumCoder =
          WindowedValue.getFullCoder(
              accumKvCoder, input.getWindowingStrategy().getWindowFn().windowCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }

    TupleTag<KV<K, AccumT>> mainTag = new TupleTag<>("main output");

    PartialReduceBundleOperator<K, InputT, OutputT, AccumT> partialDoFnOperator =
        new PartialReduceBundleOperator<>(
            combineFn,
            fullName,
            context.getWindowedInputCoder(input),
            mainTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
                mainTag, windowedAccumCoder, serializablePipelineOptions),
            input.getWindowingStrategy(),
            sideInputTagMapping,
            sideInputs,
            context.getPipelineOptions());

    String partialName = "Combine: " + fullName;
    CoderTypeInformation<WindowedValue<KV<K, AccumT>>> partialTypeInfo =
        new CoderTypeInformation<>(windowedAccumCoder, context.getPipelineOptions());

    KvToByteBufferKeySelector<K, AccumT> accumKeySelector =
        new KvToByteBufferKeySelector<>(inputKvCoder.getKeyCoder(), serializablePipelineOptions);

    // final aggregation from AccumT to OutputT
    WindowDoFnOperator<K, AccumT, OutputT> finalDoFnOperator =
        getWindowedAggregateDoFnOperator(
            context,
            transform,
            accumKvCoder,
            outputCoder,
            toFinalFlinkCombineFn(combineFn, inputKvCoder.getValueCoder()),
            sideInputTagMapping,
            sideInputs);

    if (sideInputs.isEmpty()) {
      return inputDataStream
          .transform(partialName, partialTypeInfo, partialDoFnOperator)
          .uid(partialName)
          .keyBy(accumKeySelector)
          .transform(fullName, outputTypeInfo, finalDoFnOperator)
          .uid(fullName);
    } else {
      Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformSideInputs =
          FlinkStreamingTransformTranslators.transformSideInputs(sideInputs, context);

      KeyedStream<WindowedValue<KV<K, AccumT>>, ByteBuffer> keyedStream =
          inputDataStream
              .transform(partialName, partialTypeInfo, partialDoFnOperator)
              .uid(partialName)
              .keyBy(accumKeySelector);

      return buildTwoInputStream(
          keyedStream,
          transformSideInputs.f1,
          transform.getName(),
          finalDoFnOperator,
          outputTypeInfo);
    }
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  public static <K, InputT, OutputT>
      SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> buildTwoInputStream(
          KeyedStream<WindowedValue<KV<K, InputT>>, ByteBuffer> keyedStream,
          DataStream<RawUnionValue> sideInputStream,
          String name,
          WindowDoFnOperator<K, InputT, OutputT> operator,
          TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo) {
    // we have to manually construct the two-input transform because we're not
    // allowed to have only one input keyed, normally.
    TwoInputTransformation<
            WindowedValue<KV<K, InputT>>, RawUnionValue, WindowedValue<KV<K, OutputT>>>
        rawFlinkTransform =
            new TwoInputTransformation<>(
                keyedStream.getTransformation(),
                sideInputStream.broadcast().getTransformation(),
                name,
                operator,
                outputTypeInfo,
                keyedStream.getParallelism());

    rawFlinkTransform.setStateKeyType(keyedStream.getKeyType());
    rawFlinkTransform.setStateKeySelectors(keyedStream.getKeySelector(), null);

    @SuppressWarnings({"unchecked", "rawtypes"})
    SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream =
        new SingleOutputStreamOperator(
            keyedStream.getExecutionEnvironment(),
            rawFlinkTransform) {}; // we have to cheat around the ctor being protected

    keyedStream.getExecutionEnvironment().addOperator(rawFlinkTransform);

    return outDataStream;
  }

  public static <K, InputT, AccumT, OutputT>
      SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> batchCombinePerKeyNoSideInputs(
          FlinkStreamingTranslationContext context,
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
          CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn) {
    return batchCombinePerKey(
        context, transform, combineFn, new HashMap<>(), Collections.emptyList());
  }
}
