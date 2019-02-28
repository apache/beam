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

import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.AggregatorCombinerPerKey;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.KVHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.WindowingHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;
import scala.Tuple2;

class CombinePerKeyTranslatorBatch<K, InputT, AccumT, OutputT>
    implements TransformTranslator<
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      TranslationContext context) {

    Combine.PerKey combineTransform = (Combine.PerKey) transform;
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, InputT>> input = (PCollection<KV<K, InputT>>) context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, OutputT>> output = (PCollection<KV<K, OutputT>>) context.getOutput();
    @SuppressWarnings("unchecked")
    final Combine.CombineFn<InputT, AccumT, OutputT> combineFn =
        (Combine.CombineFn<InputT, AccumT, OutputT>) combineTransform.getFn();

    Dataset<WindowedValue<KV<K, InputT>>> inputDataset = context.getDataset(input);

    Dataset<KV<K, InputT>> keyedDataset =
        inputDataset.map(
            WindowingHelpers.unwindowMapFunction(), EncoderHelpers.kvEncoder());

    KeyValueGroupedDataset<K, KV<K, InputT>> groupedDataset =
        keyedDataset.groupByKey(KVHelpers.extractKey(), EncoderHelpers.genericEncoder());

    Dataset<Tuple2<K, OutputT>> combinedDataset =
        groupedDataset.agg(
            new AggregatorCombinerPerKey<K, InputT, AccumT, OutputT>(combineFn).toColumn());

    Dataset<KV<K, OutputT>> kvOutputDataset =
        combinedDataset.map(KVHelpers.tuple2ToKV(), EncoderHelpers.kvEncoder());

    // Window the result into global window.
    Dataset<WindowedValue<KV<K, OutputT>>> outputDataset =
        kvOutputDataset.map(
            WindowingHelpers.windowMapFunction(), EncoderHelpers.windowedValueEncoder());
    context.putDataset(output, outputDataset);
  }
}
