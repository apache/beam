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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.Concatenate;
import org.apache.beam.runners.spark.structuredstreaming.translation.AbstractTranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.KVHelpers;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;
import scala.Tuple2;

class GroupByKeyTranslatorBatch<K, V>
    implements TransformTranslator<
        PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> transform,
      AbstractTranslationContext context) {

    @SuppressWarnings("unchecked")
    final PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, List<V>>> output = (PCollection<KV<K, List<V>>>) context.getOutput();
    final Combine.CombineFn<V, List<V>, List<V>> combineFn = new Concatenate<>();

    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

    Dataset<WindowedValue<KV<K, V>>> inputDataset = context.getDataset(input);

    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputCoder.getKeyCoder();
    KvCoder<K, List<V>> outputKVCoder = (KvCoder<K, List<V>>) output.getCoder();
    Coder<List<V>> outputCoder = outputKVCoder.getValueCoder();

    KeyValueGroupedDataset<K, WindowedValue<KV<K, V>>> groupedDataset =
      inputDataset.groupByKey(KVHelpers.extractKey(), EncoderHelpers.fromBeamCoder(keyCoder));

    Coder<List<V>> accumulatorCoder = null;
    try {
      accumulatorCoder =
        combineFn.getAccumulatorCoder(
          input.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }

    Dataset<Tuple2<K, Iterable<WindowedValue<List<V>>>>> combinedDataset =
      groupedDataset.agg(
        new AggregatorCombiner<K, V, List<V>, List<V>, BoundedWindow>(
          combineFn, windowingStrategy, accumulatorCoder, outputCoder)
          .toColumn());

    // expand the list into separate elements and put the key back into the elements
    WindowedValue.WindowedValueCoder<KV<K, List<V>>> wvCoder =
      WindowedValue.FullWindowedValueCoder.of(
        outputKVCoder, input.getWindowingStrategy().getWindowFn().windowCoder());
    Dataset<WindowedValue<KV<K, List<V>>>> outputDataset =
      combinedDataset.flatMap(
        (FlatMapFunction<
          Tuple2<K, Iterable<WindowedValue<List<V>>>>, WindowedValue<KV<K, List<V>>>>)
          tuple2 -> {
            K key = tuple2._1();
            Iterable<WindowedValue<List<V>>> windowedValues = tuple2._2();
            List<WindowedValue<KV<K, List<V>>>> result = new ArrayList<>();
            for (WindowedValue<List<V>> windowedValue : windowedValues) {
              KV<K, List<V>> kv = KV.of(key, windowedValue.getValue());
              result.add(
                WindowedValue.of(
                  kv,
                  windowedValue.getTimestamp(),
                  windowedValue.getWindows(),
                  windowedValue.getPane()));
            }
            return result.iterator();
          },
        EncoderHelpers.fromBeamCoder(wvCoder));
    context.putDataset(output, outputDataset);
  }
}
