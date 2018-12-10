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

import com.google.common.collect.Iterators;
import org.apache.beam.runners.spark.structuredstreaming.translation.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
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

    // group by key only.
    KeyValueGroupedDataset<K, KV<K, V>> grouped =
        input
            .map(
                (MapFunction<WindowedValue<KV<K, V>>, KV<K, V>>) WindowedValue::getValue,
                EncoderHelpers.encoder())
            .groupByKey((MapFunction<KV<K, V>, K>) KV::getKey, EncoderHelpers.<K>encoder());

    Dataset<KV<K, Iterable<V>>> materialized =
        grouped.mapGroups(
            (MapGroupsFunction<K, KV<K, V>, KV<K, Iterable<V>>>)
                (key, iterator) -> KV.of(key, () -> Iterators.transform(iterator, KV::getValue)),
            EncoderHelpers.encoder());

    Dataset<WindowedValue<KV<K, Iterable<V>>>> output =
        materialized.map(
            (MapFunction<KV<K, Iterable<V>>, WindowedValue<KV<K, Iterable<V>>>>)
                WindowedValue::valueInGlobalWindow,
            EncoderHelpers.encoder());

    context.putDataset(context.getOutput(), output);
  }
}
