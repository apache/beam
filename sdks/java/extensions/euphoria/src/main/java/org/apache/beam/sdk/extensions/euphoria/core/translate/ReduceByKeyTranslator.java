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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static java.util.Objects.requireNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableBinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.VoidFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareness;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.AdaptableCollector;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.SingleValueCollector;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Translator for {@code ReduceByKey} operator.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class ReduceByKeyTranslator<InputT, KeyT, ValueT, OutputT>
    implements OperatorTranslator<
        InputT, KV<KeyT, OutputT>, ReduceByKey<InputT, KeyT, ValueT, ?, OutputT>> {

  @Override
  public PCollection<KV<KeyT, OutputT>> translate(
      ReduceByKey<InputT, KeyT, ValueT, ?, OutputT> operator, PCollectionList<InputT> inputs) {

    // todo Could we even do values sorting in Beam ? And do we want it?
    checkState(!operator.getValueComparator().isPresent(), "Values sorting is not supported.");

    final UnaryFunction<InputT, KeyT> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<InputT, ValueT> valueExtractor = operator.getValueExtractor();

    final PCollection<InputT> input =
        operator
            .getWindow()
            .map(window -> PCollectionLists.getOnlyElement(inputs).apply(window))
            .orElseGet(() -> PCollectionLists.getOnlyElement(inputs));

    // ~ create key & value extractor
    final MapElements<InputT, KV<KeyT, ValueT>> extractor =
        MapElements.via(new KeyValueExtractor<>(keyExtractor, valueExtractor));

    final PCollection<KV<KeyT, ValueT>> extracted =
        input
            .apply("extract-keys", extractor)
            .setTypeDescriptor(
                TypeDescriptors.kvs(
                    TypeAwareness.orObjects(operator.getKeyType()),
                    TypeAwareness.orObjects(operator.getValueType())));

    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(AccumulatorProvider.of(inputs.getPipeline()));

    if (operator.isCombinable()) {
      // if operator is combinable we can process it in more efficient way
      @SuppressWarnings("unchecked")
      final PCollection combined;
      if (operator.isCombineFnStyle()) {
        combined = extracted.apply("combine", Combine.perKey(asCombineFn(operator)));
      } else {
        combined =
            extracted.apply(
                "combine",
                Combine.perKey(
                    asCombiner(
                        operator.getReducer(), accumulators, operator.getName().orElse(null))));
      }
      @SuppressWarnings("unchecked")
      final PCollection<KV<KeyT, OutputT>> cast = (PCollection) combined;
      return cast.setTypeDescriptor(
          operator
              .getOutputType()
              .orElseThrow(
                  () -> new IllegalStateException("Unable to infer output type descriptor.")));
    }

    return extracted
        .apply("group", GroupByKey.create())
        .setTypeDescriptor(
            TypeDescriptors.kvs(
                TypeAwareness.orObjects(operator.getKeyType()),
                TypeDescriptors.iterables(TypeAwareness.orObjects(operator.getValueType()))))
        .apply(
            "reduce",
            ParDo.of(
                new ReduceDoFn<>(
                    operator.getReducer(), accumulators, operator.getName().orElse(null))))
        .setTypeDescriptor(
            operator
                .getOutputType()
                .orElseThrow(
                    () -> new IllegalStateException("Unable to infer output type descriptor.")));
  }

  @Override
  public boolean canTranslate(ReduceByKey operator) {
    // translation of sorted values is not supported yet
    return !operator.getValueComparator().isPresent();
  }

  private static <InputT, KeyT, ValueT, AccT, OutputT>
      Combine.CombineFn<ValueT, AccT, OutputT> asCombineFn(
          ReduceByKey<InputT, KeyT, ValueT, AccT, OutputT> operator) {

    @SuppressWarnings("unchecked")
    ReduceByKey<InputT, KeyT, ValueT, AccT, OutputT> cast = (ReduceByKey) operator;

    VoidFunction<AccT> accumulatorFactory = cast.getAccumulatorFactory();
    BinaryFunction<AccT, ValueT, AccT> accumulate = cast.getAccumulate();
    CombinableBinaryFunction<AccT> mergeAccumulators = cast.getMergeAccumulators();
    UnaryFunction<AccT, OutputT> outputFn = cast.getOutputFn();
    TypeDescriptor<AccT> accumulatorType = cast.getAccumulatorType();

    return new Combine.CombineFn<ValueT, AccT, OutputT>() {

      @Override
      public AccT createAccumulator() {
        return accumulatorFactory.apply();
      }

      @Override
      public Coder<AccT> getAccumulatorCoder(CoderRegistry registry, Coder<ValueT> inputCoder)
          throws CannotProvideCoderException {
        return registry.getCoder(accumulatorType);
      }

      @Override
      public AccT addInput(AccT mutableAccumulator, ValueT input) {
        return accumulate.apply(mutableAccumulator, input);
      }

      @Override
      public AccT mergeAccumulators(Iterable<AccT> accumulators) {
        AccT accumulated = null;
        for (AccT o : accumulators) {
          if (accumulated == null) {
            accumulated = o;
          } else {
            accumulated = mergeAccumulators.apply(accumulated, o);
          }
        }
        return accumulated;
      }

      @Override
      public OutputT extractOutput(AccT accumulator) {
        return outputFn.apply(accumulator);
      }
    };
  }

  private static <InputT, OutputT> SerializableFunction<Iterable<InputT>, InputT> asCombiner(
      ReduceFunctor<InputT, OutputT> reducer,
      AccumulatorProvider accumulatorProvider,
      @Nullable String operatorName) {

    @SuppressWarnings("unchecked")
    final ReduceFunctor<InputT, InputT> combiner = (ReduceFunctor<InputT, InputT>) reducer;

    return (Iterable<InputT> input) -> {
      SingleValueCollector<InputT> collector =
          new SingleValueCollector<>(accumulatorProvider, operatorName);
      combiner.apply(StreamSupport.stream(input.spliterator(), false), collector);
      return collector.get();
    };
  }

  /**
   * Extract key and values from input data set.
   *
   * @param <InputT> type of input
   * @param <KeyT> type of key
   * @param <ValueT> type of value
   */
  private static class KeyValueExtractor<InputT, KeyT, ValueT>
      extends SimpleFunction<InputT, KV<KeyT, ValueT>> {

    private final UnaryFunction<InputT, KeyT> keyExtractor;
    private final UnaryFunction<InputT, ValueT> valueExtractor;

    KeyValueExtractor(
        UnaryFunction<InputT, KeyT> keyExtractor, UnaryFunction<InputT, ValueT> valueExtractor) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
    }

    @Override
    public KV<KeyT, ValueT> apply(InputT in) {
      return KV.of(keyExtractor.apply(in), valueExtractor.apply(in));
    }
  }

  /**
   * Perform reduction of given elements.
   *
   * @param <KeyT> type of key
   * @param <ValueT> type of value
   * @param <OutputT> type of output
   */
  private static class ReduceDoFn<KeyT, ValueT, OutputT>
      extends DoFn<KV<KeyT, Iterable<ValueT>>, KV<KeyT, OutputT>> {

    private final ReduceFunctor<ValueT, OutputT> reducer;
    private final AdaptableCollector<KV<KeyT, Iterable<ValueT>>, KV<KeyT, OutputT>, OutputT>
        collector;

    ReduceDoFn(
        ReduceFunctor<ValueT, OutputT> reducer,
        AccumulatorProvider accumulators,
        @Nullable String operatorName) {
      this.reducer = reducer;
      this.collector =
          new AdaptableCollector<>(
              accumulators,
              operatorName,
              (DoFn<KV<KeyT, Iterable<ValueT>>, KV<KeyT, OutputT>>.ProcessContext ctx,
                  OutputT out) -> ctx.output(KV.of(ctx.element().getKey(), out)));
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext ctx) {
      collector.setProcessContext(ctx);
      reducer.apply(
          StreamSupport.stream(requireNonNull(ctx.element().getValue()).spliterator(), false),
          collector);
    }
  }
}
