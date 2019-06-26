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
import java.util.Random;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;
import scala.Tuple2;

/**
 * By default Combine.globally is translated as a composite transform that does a Pardo (to key the
 * input PCollection with Void keys and then a Combine.PerKey transform. The problem is that
 * Combine.PerKey uses spark GroupByKey which is not very performant due to shuffle. So we add a
 * special CombineGloballyTranslator that does not need GroupByKey
 */
class CombineGloballyTranslatorBatch<InputT, AccumT, OutputT>
    implements TransformTranslator<PTransform<PCollection<InputT>, PCollection<OutputT>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<InputT>, PCollection<OutputT>> transform, TranslationContext context) {

    Combine.Globally combineTransform = (Combine.Globally) transform;
    @SuppressWarnings("unchecked")
    final PCollection<InputT> input = (PCollection<InputT>) context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<OutputT> output = (PCollection<OutputT>) context.getOutput();
    @SuppressWarnings("unchecked")
    final Combine.CombineFn<InputT, AccumT, OutputT> combineFn =
        (Combine.CombineFn<InputT, AccumT, OutputT>) combineTransform.getFn();
    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
    Dataset<WindowedValue<InputT>> inputDataset = context.getDataset(input);

    // applying a groupByKey avoids for some reason that the spark structured streaming fmwk
    // casts data to Row which makes it impossible to deserialize without
    // the coder shipped into the data. For performance reasons
    // (avoid memory consumption and having to deserialize), we do not ship coder + data.

    // We do not want to shuffle data during groupByKey, we cannot get the number of partitions for
    // the input dataset without triggering a costly operation (conversion to rdd) so we cannot use spark Hashpartitioner
    // so we apply a key to each input dataset partition and then trigger a GBK that should not shuffle data.

    Dataset<Tuple2<Integer, WindowedValue<InputT>>> keyedDataset = inputDataset
        .mapPartitions((MapPartitionsFunction<WindowedValue<InputT>, Tuple2<Integer, WindowedValue<InputT>>>) inputTIterator -> {
          List<Tuple2<Integer, WindowedValue<InputT>>> result = new ArrayList<>();
          Random random = new Random();
          while (inputTIterator.hasNext()) {
            result.add(Tuple2.apply(random.nextInt(), inputTIterator.next()));
          }
          return result.iterator();
        }, EncoderHelpers.tuple2Encoder());

    KeyValueGroupedDataset<Integer, Tuple2<Integer, WindowedValue<InputT>>> groupedDataset = keyedDataset
        .groupByKey(
            (MapFunction<Tuple2<Integer, WindowedValue<InputT>>, Integer>) value -> value._1(),
            EncoderHelpers.windowedValueEncoder());

    Dataset<Tuple2<Integer, Iterable<WindowedValue<OutputT>>>> combinedDataset = groupedDataset
        .agg(new AggregatorCombinerGlobally<>(combineFn, windowingStrategy).toColumn());

    Dataset<Iterable<WindowedValue<OutputT>>> accumulatedDataset = combinedDataset.map(
        (MapFunction<Tuple2<Integer, Iterable<WindowedValue<OutputT>>>, Iterable<WindowedValue<OutputT>>>) value -> value._2,
        EncoderHelpers.genericEncoder());

    Dataset<WindowedValue<OutputT>> outputDataset = accumulatedDataset.flatMap(
        (FlatMapFunction<Iterable<WindowedValue<OutputT>>, WindowedValue<OutputT>>)
            windowedValues -> windowedValues.iterator(), EncoderHelpers.windowedValueEncoder());
    context.putDataset(output, outputDataset);
  }
}
