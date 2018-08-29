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

import static com.google.common.base.Preconditions.checkState;

import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.translate.window.WindowingUtils;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Translator for {@code ReduceByKey} operator. */
class ReduceByKeyTranslator implements OperatorTranslator<ReduceByKey> {

  @SuppressWarnings("unchecked")
  private static <InputT, K, V, OutputT, W extends BoundedWindow>
      PCollection<KV<K, OutputT>> doTranslate(
          ReduceByKey<InputT, K, V, OutputT, W> operator, TranslationContext context) {

    //TODO Could we even do values sorting in Beam ? And do we want it?
    checkState(operator.getValueComparator() == null, "Values sorting is not supported.");

    final UnaryFunction<InputT, K> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<InputT, V> valueExtractor = operator.getValueExtractor();
    final ReduceFunctor<V, OutputT> reducer = operator.getReducer();

    // ~ resolve coders
    final Coder<K> keyCoder = context.getKeyCoder(operator);
    final Coder<V> valueCoder = context.getValueCoder(operator);

    final PCollection<InputT> input =
        WindowingUtils.applyWindowingIfSpecified(
            operator, context.getInput(operator), context.getAllowedLateness(operator));

    // ~ create key & value extractor
    final MapElements<InputT, KV<K, V>> extractor =
        MapElements.via(
            new SimpleFunction<InputT, KV<K, V>>() {
              @Override
              public KV<K, V> apply(InputT in) {
                return KV.of(keyExtractor.apply(in), valueExtractor.apply(in));
              }
            });
    final PCollection<KV<K, V>> extracted =
        input
            .apply(operator.getName() + "::extract-keys", extractor)
            .setCoder(KvCoder.of(keyCoder, valueCoder));

    WindowingUtils.checkGropupByKeyApplicalble(operator, extracted);

    Coder<KV<K, OutputT>> outputCoder = context.getOutputCoder(operator);

    if (operator.isCombinable()) {
      final PCollection combined =
          extracted.apply(operator.getName() + "::combine", Combine.perKey(asCombiner(reducer)));
      combined.setCoder(outputCoder);

      return combined;

    } else {
      // reduce
      final AccumulatorProvider accumulators =
          new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings());

      final PCollection<KV<K, Iterable<V>>> grouped =
          extracted
              .apply(operator.getName() + "::group", GroupByKey.create())
              .setCoder(KvCoder.of(keyCoder, IterableCoder.of(valueCoder)));

      PCollection<KV<K, OutputT>> reduced =
          grouped.apply(
              operator.getName() + "::reduce",
              ParDo.of(new ReduceDoFn<>(reducer, accumulators, operator.getName())));
      reduced.setCoder(outputCoder);

      return reduced;
    }
  }

  @Override
  public boolean canTranslate(ReduceByKey operator) {
    // translation of sorted values is not supported yet
    return operator.getValueComparator() == null;
  }

  private static <InputT, OutputT> SerializableFunction<Iterable<InputT>, InputT> asCombiner(
      ReduceFunctor<InputT, OutputT> reducer) {

    @SuppressWarnings("unchecked")
    final ReduceFunctor<InputT, InputT> combiner = (ReduceFunctor<InputT, InputT>) reducer;

    return (Iterable<InputT> input) -> {
      SingleValueCollector<InputT> collector = new SingleValueCollector<>();
      combiner.apply(StreamSupport.stream(input.spliterator(), false), collector);
      return collector.get();
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(ReduceByKey operator, TranslationContext context) {
    return doTranslate(operator, context);
  }

  private static class ReduceDoFn<K, V, OutT> extends DoFn<KV<K, Iterable<V>>, KV<K, OutT>> {

    private final ReduceFunctor<V, OutT> reducer;
    private final DoFnCollector<KV<K, Iterable<V>>, KV<K, OutT>, OutT> collector;

    ReduceDoFn(
        ReduceFunctor<V, OutT> reducer, AccumulatorProvider accumulators, String operatorName) {
      this.reducer = reducer;
      this.collector = new DoFnCollector<>(accumulators, new Collector<>(operatorName));
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext ctx) {
      collector.setProcessContext(ctx);
      reducer.apply(StreamSupport.stream(ctx.element().getValue().spliterator(), false), collector);
    }
  }

  /**
   * Translation of {@link Collector} collect to Beam's context output. OperatorName serve as
   * namespace for Beam's metrics.
   */
  private static class Collector<K, V, OutT>
      implements DoFnCollector.BeamCollector<KV<K, Iterable<V>>, KV<K, OutT>, OutT> {

    private final String operatorName;

    public Collector(String operatorName) {
      this.operatorName = operatorName;
    }

    @Override
    public void collect(DoFn<KV<K, Iterable<V>>, KV<K, OutT>>.ProcessContext ctx, OutT out) {
      ctx.output(KV.of(ctx.element().getKey(), out));
    }

    @Override
    public String getOperatorName() {
      return operatorName;
    }
  }
}
