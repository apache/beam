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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.GroupAlsoByWindowViaOutputBufferFn;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.KVHelpers;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;

class GroupByKeyTranslatorBatch<K, V>
    implements TransformTranslator<
        PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> transform,
      TranslationContext context) {

    @SuppressWarnings("unchecked")
    final PCollection<KV<K, V>> inputPCollection = (PCollection<KV<K, V>>) context.getInput();

    Dataset<WindowedValue<KV<K, V>>> input = context.getDataset(inputPCollection);

    // group by key only
    KeyValueGroupedDataset<K, WindowedValue<KV<K, V>>> groupByKeyOnly =
        input.groupByKey(KVHelpers.extractKey(), EncoderHelpers.genericEncoder());

    // Materialize groupByKeyOnly values, potential OOM because of creation of new iterable
    Dataset<KV<K, Iterable<WindowedValue<V>>>> materialized =
        groupByKeyOnly.mapGroups(
            (MapGroupsFunction<K, WindowedValue<KV<K, V>>, KV<K, Iterable<WindowedValue<V>>>>)
                (key, iterator) -> {
                  List<WindowedValue<V>> values = new ArrayList<>();
                  while (iterator.hasNext()) {
                    WindowedValue<KV<K, V>> next = iterator.next();
                    values.add(
                        WindowedValue.of(
                            next.getValue().getValue(),
                            next.getTimestamp(),
                            next.getWindows(),
                            next.getPane()));
                  }
                  KV<K, Iterable<WindowedValue<V>>> kv =
                      KV.of(key, Iterables.unmodifiableIterable(values));
                  return kv;
                },
            EncoderHelpers.kvEncoder());

    WindowingStrategy<?, ?> windowingStrategy = inputPCollection.getWindowingStrategy();
    KvCoder<K, V> coder = (KvCoder<K, V>) inputPCollection.getCoder();
    // group also by windows
    Dataset<WindowedValue<KV<K, Iterable<V>>>> output =
        materialized.flatMap(
            new GroupAlsoByWindowViaOutputBufferFn<>(
                windowingStrategy,
                new InMemoryStateInternalsFactory<>(),
                SystemReduceFn.buffering(coder.getValueCoder()),
                context.getSerializableOptions()),
            EncoderHelpers.windowedValueEncoder());

    context.putDataset(context.getOutput(), output);
  }

  /**
   * In-memory state internals factory.
   *
   * @param <K> State key type.
   */
  static class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K>, Serializable {
    @Override
    public StateInternals stateInternalsForKey(K key) {
      return InMemoryStateInternals.forKey(key);
    }
  }
}
