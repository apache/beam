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
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.GroupAlsoByWindowViaOutputBufferFn;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.KVHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
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
    WindowingStrategy<?, ?> windowingStrategy = inputPCollection.getWindowingStrategy();
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) inputPCollection.getCoder();
    Coder<V> valueCoder = kvCoder.getValueCoder();

    // group by key only
    Coder<K> keyCoder = kvCoder.getKeyCoder();
    KeyValueGroupedDataset<K, WindowedValue<KV<K, V>>> groupByKeyOnly =
        input.groupByKey(KVHelpers.extractKey(), EncoderHelpers.fromBeamCoder(keyCoder));

    // group also by windows
    WindowedValue.FullWindowedValueCoder<KV<K, Iterable<V>>> outputCoder =
        WindowedValue.FullWindowedValueCoder.of(
            KvCoder.of(keyCoder, IterableCoder.of(valueCoder)),
            windowingStrategy.getWindowFn().windowCoder());
    Dataset<WindowedValue<KV<K, Iterable<V>>>> output =
        groupByKeyOnly.flatMapGroups(
            new GroupAlsoByWindowViaOutputBufferFn<>(
                windowingStrategy,
                new InMemoryStateInternalsFactory<>(),
                SystemReduceFn.buffering(valueCoder),
                context.getSerializableOptions()),
            EncoderHelpers.fromBeamCoder(outputCoder));

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
