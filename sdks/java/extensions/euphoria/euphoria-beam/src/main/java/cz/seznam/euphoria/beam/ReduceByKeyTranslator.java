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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Translator for {@code ReduceByKey} operator.
 */
class ReduceByKeyTranslator implements OperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(ReduceByKey operator, BeamExecutorContext context) {
    return doTranslate(operator, context);
  }

  private static <IN, KEY, VALUE, OUT, W extends Window<W>> PCollection<Pair<KEY, OUT>> doTranslate(
      ReduceByKey<IN, KEY, VALUE, OUT, W> operator, BeamExecutorContext context) {

    final UnaryFunction<IN, KEY> keyExtractor = operator.getKeyExtractor();
    final UnaryFunction<IN, VALUE> valueExtractor = operator.getValueExtractor();
    final ReduceFunctor<VALUE, OUT> reducer = operator.getReducer();

    // ~ resolve coders
    final Coder<KEY> keyCoder = context.getCoder(keyExtractor);
    final Coder<VALUE> valueCoder = context.getCoder(valueExtractor);
    final Coder<OUT> outputCoder = context.getCoder(reducer);

    final PCollection<IN> input;

    // ~ apply windowing if specified
    if (operator.getWindowing() == null) {
      input = context.getInput(operator);
    } else {
      input = context.getInput(operator).apply(org.apache.beam.sdk.transforms.windowing.Window
          .into(BeamWindowFn.wrap(operator.getWindowing())));

//      input = (PCollection<IN>) input.apply(org.apache.beam.sdk.transforms.windowing.Window
//          .configure()
//          .discardingFiredPanes()
//          .triggering(AfterWatermark.pastEndOfWindow())
//          .into(BeamWindowFn.wrap(operator.getWindowing())));
    }

    // ~ create key & value extractor
//    final MapElements<IN, KV<KEY, VALUE>> extractor = MapElements
//        .into(TypeUtils.kvOf(keyType, valueType))
//        .via(e -> KV.of(keyExtractor.apply(e), valueExtractor.apply(e)));

    final PCollection<KV<KEY, VALUE>> extracted;

    // ~ apply key & value extractor
//    extracted = input
//        .apply(extractor)
//        .setCoder(KvCoder.of(keyCoder, valueCoder));

//    if (operator.isCombinable()) {
//      final PCollection<KV<KEY, VALUE>> combined = extracted.apply(
//          Combine.perKey(asCombiner(operator.getReducer())));

      // remap from KVs to Pairs
//      col = (PCollection<Object, Object>>) col.apply(
//          MapElements
//              .into(pairDescriptor())
//              .via((KV kv) -> Pair.of(kv.getKey(), kv.getValue())));
//      kvs.setCoder(new KryoCoder<>());
//    } else {
//    }
    // reduce
//    final PCollection<Pair<KEY, OUT>> result =

//    final AccumulatorProvider accumulators = new LazyAccumulatorProvider(
//        context.getAccumulatorFactory(),
//        context.getSettings());

//    final PCollection<KV<KEY, Iterable<VALUE>>> grouped = extracted
//        .apply(GroupByKey.create())
//        .setCoder(KvCoder.of(keyCoder, IterableCoder.of(valueCoder)));

//    return grouped
//        .apply(ParDo.of(new ReduceDoFn<>(operator.getReducer(), accumulators)))
//        .setCoder(PairCoder.of(keyCoder, outputCoder));

    throw new UnsupportedOperationException("FIXME");
  }

  private static <IN, OUT> SerializableFunction<Stream<IN>, IN> asCombiner(
      ReduceFunctor<IN, OUT> reducer) {
    @SuppressWarnings("unchecked")
    final ReduceFunctor<IN, IN> combiner = (ReduceFunctor<IN, IN>) reducer;
    final CombinableCollector<IN> collector = new CombinableCollector<>();
    return (Stream<IN> input) -> {
      combiner.apply(input, collector);
      return collector.get();
    };
  }

  private static class ReduceDoFn<K, V, O> extends DoFn<KV<K, Iterable<V>>, Pair<K, O>> {

    private final ReduceFunctor<V, O> reducer;
    private final AccumulatorProvider accumulators;

    ReduceDoFn(ReduceFunctor<V, O> reducer, AccumulatorProvider accumulators) {
      this.reducer = reducer;
      this.accumulators = accumulators;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      reducer.apply(StreamSupport.stream(ctx.element().getValue().spliterator(), false),
          new DoFnCollector<>(accumulators, (out) ->
              ctx.output(Pair.of(ctx.element().getKey(), out))));
    }
  }

}
