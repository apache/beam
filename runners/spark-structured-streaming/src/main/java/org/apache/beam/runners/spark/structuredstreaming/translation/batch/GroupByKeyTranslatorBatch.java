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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.WindowingHelpers;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.MapFunction;
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

    Dataset<WindowedValue<KV<K, V>>> input = context.getDataset(context.getInput());

    // Extract key to group by key only.
    KeyValueGroupedDataset<K, KV<K, V>> grouped =
        input
            .map(WindowingHelpers.unwindowMapFunction(), EncoderHelpers.kvEncoder())
            .groupByKey((MapFunction<KV<K, V>, K>) KV::getKey, EncoderHelpers.genericEncoder());

    // Materialize grouped values, potential OOM because of creation of new iterable
    Dataset<KV<K, Iterable<V>>> materialized =
        grouped.mapGroups(
            (MapGroupsFunction<K, KV<K, V>, KV<K, Iterable<V>>>)
                // TODO: We need to improve this part and avoid creating of new List (potential OOM)
                // (key, iterator) -> KV.of(key, () -> Iterators.transform(iterator, KV::getValue)),
                (key, iterator) -> {
                  List<V> values = Lists.newArrayList();
                  while (iterator.hasNext()) {
                    values.add(iterator.next().getValue());
                  }
                  return KV.of(key, Iterables.unmodifiableIterable(values));
                },
            EncoderHelpers.kvEncoder());

    // Window the result into global window.
    Dataset<WindowedValue<KV<K, Iterable<V>>>> output =
        materialized.map(
            WindowingHelpers.windowMapFunction(), EncoderHelpers.windowedValueEncoder());

    context.putDataset(context.getOutput(), output);
  }
}
