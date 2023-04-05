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

import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.eligibleForGlobalGroupBy;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.eligibleForGroupByWindow;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.explodeWindowedKey;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.value;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.valueKey;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.valueValue;
import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.windowedKV;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;

import java.util.Collection;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun1;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import scala.Tuple2;
import scala.collection.TraversableOnce;

/**
 * Translator for {@link Combine.PerKey} using {@link Dataset#groupByKey} with a Spark {@link
 * Aggregator}.
 *
 * <ul>
 *   <li>When using the default global window, window information is dropped and restored after the
 *       aggregation.
 *   <li>For non-merging windows, windows are exploded and moved into a composite key for better
 *       distribution. After the aggregation, windowed values are restored from the composite key.
 *   <li>All other cases use an aggregator on windowed values that is optimized for the current
 *       windowing strategy.
 * </ul>
 *
 * TODOs:
 * <li>combine with context (CombineFnWithContext)?
 * <li>combine with sideInputs?
 * <li>other there other missing features?
 */
class CombinePerKeyTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<KV<K, InT>>, PCollection<KV<K, OutT>>, Combine.PerKey<K, InT, OutT>> {

  CombinePerKeyTranslatorBatch() {
    super(0.2f);
  }

  @Override
  public void translate(Combine.PerKey<K, InT, OutT> transform, Context cxt) {
    WindowingStrategy<?, ?> windowing = cxt.getInput().getWindowingStrategy();
    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) transform.getFn();

    KvCoder<K, InT> inputCoder = (KvCoder<K, InT>) cxt.getInput().getCoder();
    KvCoder<K, OutT> outputCoder = (KvCoder<K, OutT>) cxt.getOutput().getCoder();

    Encoder<K> keyEnc = cxt.keyEncoderOf(inputCoder);
    Encoder<KV<K, InT>> inputEnc = cxt.encoderOf(inputCoder);
    Encoder<WindowedValue<KV<K, OutT>>> wvOutputEnc = cxt.windowedEncoder(outputCoder);
    Encoder<AccT> accumEnc = accumEncoder(combineFn, inputCoder.getValueCoder(), cxt);

    final Dataset<WindowedValue<KV<K, OutT>>> result;

    boolean globalGroupBy = eligibleForGlobalGroupBy(windowing, true);
    boolean groupByWindow = eligibleForGroupByWindow(windowing, true);

    if (globalGroupBy || groupByWindow) {
      Aggregator<KV<K, InT>, ?, OutT> valueAgg =
          Aggregators.value(combineFn, KV::getValue, accumEnc, cxt.valueEncoderOf(outputCoder));

      if (globalGroupBy) {
        // Drop window and group by key globally to run the aggregation (combineFn), afterwards the
        // global window is restored
        result =
            cxt.getDataset(cxt.getInput())
                .groupByKey(valueKey(), keyEnc)
                .mapValues(value(), inputEnc)
                .agg(valueAgg.toColumn())
                .map(globalKV(), wvOutputEnc);
      } else {
        Encoder<Tuple2<BoundedWindow, K>> windowedKeyEnc =
            cxt.tupleEncoder(cxt.windowEncoder(), keyEnc);

        // Group by window and key to run the aggregation (combineFn)
        result =
            cxt.getDataset(cxt.getInput())
                .flatMap(explodeWindowedKey(value()), cxt.tupleEncoder(windowedKeyEnc, inputEnc))
                .groupByKey(fun1(Tuple2::_1), windowedKeyEnc)
                .mapValues(fun1(Tuple2::_2), inputEnc)
                .agg(valueAgg.toColumn())
                .map(windowedKV(), wvOutputEnc);
      }
    } else {
      // Optimized aggregator for non-merging and session window functions, all others depend on
      // windowFn.mergeWindows
      Aggregator<WindowedValue<KV<K, InT>>, ?, Collection<WindowedValue<OutT>>> aggregator =
          Aggregators.windowedValue(
              combineFn,
              valueValue(),
              windowing,
              cxt.windowEncoder(),
              accumEnc,
              cxt.windowedEncoder(outputCoder.getValueCoder()));
      result =
          cxt.getDataset(cxt.getInput())
              .groupByKey(valueKey(), keyEnc)
              .agg(aggregator.toColumn())
              .flatMap(explodeWindows(), wvOutputEnc);
    }

    cxt.putDataset(cxt.getOutput(), result);
  }

  private static <K, V>
      Fun1<Tuple2<K, Collection<WindowedValue<V>>>, TraversableOnce<WindowedValue<KV<K, V>>>>
          explodeWindows() {
    return t ->
        ScalaInterop.scalaIterator(t._2).map(wv -> wv.withValue(KV.of(t._1, wv.getValue())));
  }

  private static <K, V> Fun1<Tuple2<K, V>, WindowedValue<KV<K, V>>> globalKV() {
    return t -> WindowedValue.valueInGlobalWindow(KV.of(t._1, t._2));
  }

  private Encoder<AccT> accumEncoder(
      CombineFn<InT, AccT, OutT> fn, Coder<InT> valueCoder, Context cxt) {
    try {
      CoderRegistry registry = cxt.getInput().getPipeline().getCoderRegistry();
      return cxt.encoderOf(fn.getAccumulatorCoder(registry, valueCoder));
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }
  }
}
