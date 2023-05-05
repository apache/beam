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

import static org.apache.beam.runners.spark.structuredstreaming.translation.batch.GroupByKeyHelpers.value;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;
import static scala.collection.Iterator.single;

import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun1;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import scala.collection.Iterator;

/**
 * Translator for {@link Combine.Globally} using a Spark {@link Aggregator}.
 *
 * <p>To minimize the amount of data shuffled, this first reduces the data per partition using
 * {@link Aggregator#reduce}, gathers the partial results (using {@code coalesce(1)}) and finally
 * merges these using {@link Aggregator#merge}.
 *
 * <p>TODOs:
 * <li>any missing features?
 */
class CombineGloballyTranslatorBatch<InT, AccT, OutT>
    extends TransformTranslator<PCollection<InT>, PCollection<OutT>, Combine.Globally<InT, OutT>> {

  CombineGloballyTranslatorBatch() {
    super(0.2f);
  }

  @Override
  protected void translate(Combine.Globally<InT, OutT> transform, Context cxt) {
    WindowingStrategy<?, ?> windowing = cxt.getInput().getWindowingStrategy();
    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) transform.getFn();

    Coder<InT> inputCoder = cxt.getInput().getCoder();
    Coder<OutT> outputCoder = cxt.getOutput().getCoder();
    Coder<AccT> accumCoder = accumulatorCoder(combineFn, inputCoder, cxt);

    Encoder<OutT> outEnc = cxt.encoderOf(outputCoder);
    Encoder<AccT> accEnc = cxt.encoderOf(accumCoder);
    Encoder<WindowedValue<OutT>> wvOutEnc = cxt.windowedEncoder(outEnc);

    Dataset<WindowedValue<InT>> dataset = cxt.getDataset(cxt.getInput());

    final Dataset<WindowedValue<OutT>> result;
    if (GroupByKeyHelpers.eligibleForGlobalGroupBy(windowing, true)) {
      Aggregator<InT, ?, OutT> agg = Aggregators.value(combineFn, v -> v, accEnc, outEnc);

      // Drop window and restore afterwards, produces single global aggregation result
      result = aggregate(dataset, agg, value(), windowedValue(), wvOutEnc);
    } else {
      Aggregator<WindowedValue<InT>, ?, Collection<WindowedValue<OutT>>> agg =
          Aggregators.windowedValue(
              combineFn, value(), windowing, cxt.windowEncoder(), accEnc, wvOutEnc);

      // Produces aggregation result per window
      result =
          aggregate(dataset, agg, v -> v, fun1(out -> ScalaInterop.scalaIterator(out)), wvOutEnc);
    }
    cxt.putDataset(cxt.getOutput(), result);
  }

  /**
   * Aggregate dataset globally without using key.
   *
   * <p>There is no global, typed version of {@link Dataset#agg(Map)} on datasets. This reduces all
   * partitions first, and then merges them to receive the final result.
   */
  private static <InT, OutT, AggInT, BuffT, AggOutT> Dataset<WindowedValue<OutT>> aggregate(
      Dataset<WindowedValue<InT>> ds,
      Aggregator<AggInT, BuffT, AggOutT> agg,
      Fun1<WindowedValue<InT>, AggInT> valueFn,
      Fun1<AggOutT, Iterator<WindowedValue<OutT>>> finishFn,
      Encoder<WindowedValue<OutT>> enc) {
    // reduce partition using aggregator
    Fun1<Iterator<WindowedValue<InT>>, Iterator<BuffT>> reduce =
        fun1(it -> single(it.map(valueFn).foldLeft(agg.zero(), agg::reduce)));
    // merge reduced partitions using aggregator
    Fun1<Iterator<BuffT>, Iterator<WindowedValue<OutT>>> merge =
        fun1(it -> finishFn.apply(agg.finish(it.hasNext() ? it.reduce(agg::merge) : agg.zero())));

    return ds.mapPartitions(reduce, agg.bufferEncoder()).coalesce(1).mapPartitions(merge, enc);
  }

  private Coder<AccT> accumulatorCoder(
      CombineFn<InT, AccT, OutT> fn, Coder<InT> valueCoder, Context cxt) {
    try {
      return fn.getAccumulatorCoder(cxt.getInput().getPipeline().getCoderRegistry(), valueCoder);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> Fun1<T, Iterator<WindowedValue<T>>> windowedValue() {
    return v -> single(WindowedValue.valueInGlobalWindow(v));
  }
}
