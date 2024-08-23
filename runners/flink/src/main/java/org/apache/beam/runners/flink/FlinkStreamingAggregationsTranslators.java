package org.apache.beam.runners.flink;

import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.*;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.*;

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

  private static <InputT, OutputT> CombineFnBase.GlobalCombineFn<Object, Object, OutputT> toFinalFlinkCombineFn(
      CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> combineFn, Coder<InputT> inputTCoder) {

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
   * Create a DoFnOperator instance that group elements per window and apply a combine function on them.
   */
  public static <K, InputC, OutputC, InputT, OutputT> WindowDoFnOperator<K, InputC, OutputC> getWindowedAggregateDoFnOperator(
      FlinkStreamingTranslationContext context,
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      KvCoder<K, InputC> inputKvCoder,
      Coder<WindowedValue<KV<K, OutputC>>> outputCoder,
      SystemReduceFn<K, InputC, ?, OutputC, BoundedWindow> reduceFn,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      List<PCollectionView<?>> sideInputs) {

    // Naming
    String fullName = FlinkStreamingTransformTranslators.getCurrentTransformName(context);
    TupleTag<KV<K, OutputC>> mainTag = new TupleTag<>("main output");

    // input infos
    PCollection<KV<K, InputT>> input = context.getInput(transform);

    @SuppressWarnings("unchecked")
    WindowingStrategy<?, BoundedWindow> windowingStrategy =
        (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();
    SerializablePipelineOptions serializablePipelineOptions =
        new SerializablePipelineOptions(context.getPipelineOptions());

    // Coders
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();

    SingletonKeyedWorkItemCoder<K, InputC> workItemCoder =
        SingletonKeyedWorkItemCoder.of(
            keyCoder,
            inputKvCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<KeyedWorkItem<K, InputC>> windowedWorkItemCoder =
        WindowedValue.getFullCoder(workItemCoder, windowingStrategy.getWindowFn().windowCoder());

    // Key selector
    WorkItemKeySelector<K, InputC> workItemKeySelector =
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

  public static <K, InputC, OutputC, InputT, OutputT> WindowDoFnOperator<K, InputC, OutputC> getWindowedAggregateDoFnOperator(
      FlinkStreamingTranslationContext context,
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      KvCoder<K, InputC> inputKvCoder,
      Coder<WindowedValue<KV<K, OutputC>>> outputCoder,
      CombineFnBase.GlobalCombineFn<? super InputC, ?, OutputC> combineFn,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      List<PCollectionView<?>> sideInputs) {

    // Combining fn
    SystemReduceFn<K, InputC, ?, OutputC, BoundedWindow> reduceFn =
        SystemReduceFn.combining(
            inputKvCoder.getKeyCoder(),
            AppliedCombineFn.withInputCoder(
                combineFn, context.getInput(transform).getPipeline().getCoderRegistry(), inputKvCoder));

    return getWindowedAggregateDoFnOperator(context, transform, inputKvCoder, outputCoder, reduceFn, sideInputTagMapping, sideInputs);
  }

  public static <K, InputT, AccumT, OutputT> SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> batchCombinePerKeyNoSideInputs(
      FlinkStreamingTranslationContext context,
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn) {

    Coder<WindowedValue<KV<K, AccumT>>> windowedAccumCoder;
    KvCoder<K, AccumT> accumKvCoder;

    PCollection<KV<K, InputT>> input = context.getInput(transform);
    String fullName = FlinkStreamingTransformTranslators.getCurrentTransformName(context);
    DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);
    KvCoder<K, InputT> inputKvCoder =
        (KvCoder<K, InputT>) input.getCoder();
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
            FlinkStreamingTransformTranslators.getCurrentTransformName(context),
            context.getWindowedInputCoder(input),
            mainTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
                mainTag, windowedAccumCoder, serializablePipelineOptions),
            input.getWindowingStrategy(),
            new HashMap<>(),
            Collections.emptyList(),
            context.getPipelineOptions());

    // final aggregation from AccumT to OutputT
    WindowDoFnOperator<K, AccumT, OutputT> finalDoFnOperator =
        getWindowedAggregateDoFnOperator(
            context,
            transform,
            accumKvCoder,
            outputCoder,
            toFinalFlinkCombineFn(combineFn, inputKvCoder.getValueCoder()),
            new HashMap<>(),
            Collections.emptyList());

    String partialName = "Combine: " + fullName;
    CoderTypeInformation<WindowedValue<KV<K, AccumT>>> partialTypeInfo =
        new CoderTypeInformation<>(windowedAccumCoder, context.getPipelineOptions());

    return
        inputDataStream
            .transform(partialName, partialTypeInfo, partialDoFnOperator)
            .uid(partialName)
            .keyBy(new KvToByteBufferKeySelector<>(inputKvCoder.getKeyCoder(), serializablePipelineOptions))
            .transform(fullName, outputTypeInfo, finalDoFnOperator)
            .uid(fullName);
  }
}
