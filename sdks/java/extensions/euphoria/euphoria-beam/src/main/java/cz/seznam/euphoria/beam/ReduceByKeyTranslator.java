/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.window.BeamWindowFn;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shadow.com.google.common.collect.Streams;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.stream.StreamSupport;

/** Translator for {@code ReduceByKey} operator. */
class ReduceByKeyTranslator implements OperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(ReduceByKey operator, BeamExecutorContext context) {
    return doTranslate(operator, context);
  }

  @SuppressWarnings("unchecked")
  private static <IN, KEY, VALUE, OUT, W extends Window<W>> PCollection<Pair<KEY, OUT>> doTranslate(
      ReduceByKey<IN, KEY, VALUE, OUT, W> operator, BeamExecutorContext context) {

    final UnaryFunction<IN, KEY> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<IN, VALUE> valueExtractor = operator.getValueExtractor();
    final ReduceFunctor<VALUE, OUT> reducer = operator.getReducer();

    // ~ resolve coders
    final Coder<KEY> keyCoder = context.getCoder(keyExtractor);
    final Coder<VALUE> valueCoder = context.getCoder(valueExtractor);

    final PCollection<IN> input;

    // ~ apply windowing if specified
    if (operator.getWindowing() == null) {
      input = context.getInput(operator);
    } else {
      input =
          context
              .getInput(operator)
              .apply(
                  org.apache.beam.sdk.transforms.windowing.Window.into(
                          BeamWindowFn.wrap(operator.getWindowing()))
                      // FIXME: trigger
                      .triggering(AfterWatermark.pastEndOfWindow())
                      .discardingFiredPanes()
                      .withAllowedLateness(context.getAllowedLateness(operator)));
    }

    // ~ create key & value extractor
    final MapElements<IN, KV<KEY, VALUE>> extractor =
        MapElements.via(
            new SimpleFunction<IN, KV<KEY, VALUE>>() {
              @Override
              public KV<KEY, VALUE> apply(IN in) {
                return KV.of(keyExtractor.apply(in), valueExtractor.apply(in));
              }
            });
    final PCollection<KV<KEY, VALUE>> extracted =
        input.apply(extractor).setCoder(KvCoder.of(keyCoder, valueCoder));

    if (operator.isCombinable()) {
      final PCollection<KV<KEY, VALUE>> combined =
          extracted.apply(Combine.perKey(asCombiner(reducer)));

      // remap from KVs to Pairs
      PCollection<Pair<KEY, VALUE>> kvs =
          combined.apply(
              MapElements.via(
                  new SimpleFunction<KV<KEY, VALUE>, Pair<KEY, VALUE>>() {
                    @Override
                    public Pair<KEY, VALUE> apply(KV<KEY, VALUE> in) {
                      return Pair.of(in.getKey(), in.getValue());
                    }
                  }));
      return (PCollection) kvs;
    } else {
      // reduce
      final AccumulatorProvider accumulators =
          new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings());

      final PCollection<KV<KEY, Iterable<VALUE>>> grouped =
          extracted
              .apply(GroupByKey.create())
              .setCoder(KvCoder.of(keyCoder, IterableCoder.of(valueCoder)));

      return grouped.apply(ParDo.of(new ReduceDoFn<>(reducer, accumulators)));
    }
  }

  private static <IN, OUT> SerializableFunction<Iterable<IN>, IN> asCombiner(
      ReduceFunctor<IN, OUT> reducer) {

    @SuppressWarnings("unchecked")
    final ReduceFunctor<IN, IN> combiner = (ReduceFunctor<IN, IN>) reducer;
    final SingleValueCollector<IN> collector = new SingleValueCollector<>();
    return (Iterable<IN> input) -> {
      combiner.apply(Streams.stream(input), collector);
      return collector.get();
    };
  }

  private static class ReduceDoFn<K, V, O> extends DoFn<KV<K, Iterable<V>>, Pair<K, O>> {

    private final ReduceFunctor<V, O> reducer;
    private final DoFnCollector<KV<K, Iterable<V>>, Pair<K, O>, O> collector;

    ReduceDoFn(ReduceFunctor<V, O> reducer, AccumulatorProvider accumulators) {
      this.reducer = reducer;
      this.collector = new DoFnCollector<>(accumulators, new Collector<>());
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext ctx) {
      collector.setProcessContext(ctx);
      reducer.apply(StreamSupport.stream(ctx.element().getValue().spliterator(), false), collector);
    }
  }

  private static class Collector<K, V, O>
      implements DoFnCollector.BeamCollector<KV<K, Iterable<V>>, Pair<K, O>, O> {

    @Override
    public void collect(DoFn<KV<K, Iterable<V>>, Pair<K, O>>.ProcessContext ctx, O out) {
      ctx.output(Pair.of(ctx.element().getKey(), out));
    }
  }
}
