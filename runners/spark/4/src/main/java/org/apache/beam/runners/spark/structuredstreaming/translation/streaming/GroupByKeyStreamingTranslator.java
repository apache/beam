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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.GroupByKeyStatefulProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.TimeMode;

/** Translates {@link GroupByKey} for Spark 4 Structured Streaming via transformWithState. */
@Internal
public class GroupByKeyStreamingTranslator<K, V>
    extends TransformTranslator<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  public GroupByKeyStreamingTranslator() {
    super(0.2f);
  }

  @Override
  protected void translate(GroupByKey<K, V> transform, Context cxt) {
    PCollection<KV<K, V>> input = cxt.getInput();
    WindowingStrategy<?, ?> windowing = input.getWindowingStrategy();
    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputCoder.getKeyCoder();
    PCollection<KV<K, Iterable<V>>> output = cxt.getOutput();
    Coder<KV<K, Iterable<V>>> outputCoder = output.getCoder();

    GroupByKeyStatefulProcessor<K, V> processor =
        new GroupByKeyStatefulProcessor<>(inputCoder, windowing, cxt.getOptionsSupplier());

    MapFunction<WindowedValue<KV<K, V>>, byte[]> keyFn =
        v -> CoderHelpers.toByteArray(v.getValue().getKey(), keyCoder);
    Dataset<WindowedValue<KV<K, Iterable<V>>>> result =
        cxt.getDataset(input)
            .groupByKey(keyFn, Encoders.BINARY())
            .transformWithState(
                processor,
                TimeMode.EventTime(),
                OutputMode.Append(),
                cxt.windowedEncoder(outputCoder));

    cxt.putDataset(output, result);
  }
}
