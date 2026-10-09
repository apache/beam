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

import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.BeamStatefulProcessor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.TimeMode;

/** Translates a stateful {@link ParDo.MultiOutput} for Spark 4 Structured Streaming. */
@Internal
public class StatefulParDoStreamingTranslator<K, V, OutputT>
    extends TransformTranslator<
        PCollection<? extends KV<K, V>>, PCollectionTuple, ParDo.MultiOutput<KV<K, V>, OutputT>> {

  public StatefulParDoStreamingTranslator() {
    super(0.2f);
  }

  @Override
  protected void translate(ParDo.MultiOutput<KV<K, V>, OutputT> transform, Context cxt) {
    @SuppressWarnings("unchecked")
    PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) cxt.getInput();
    WindowingStrategy<?, ?> windowing = input.getWindowingStrategy();
    DoFn<KV<K, V>, OutputT> doFn = transform.getFn();

    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Encoder<K> keyEnc = cxt.keyEncoderOf(inputCoder);
    TupleTag<OutputT> mainOutputTag = transform.getMainOutputTag();
    PCollection<OutputT> output = cxt.getOutput(mainOutputTag);
    Coder<OutputT> outputCoder = output.getCoder();

    DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory =
        DoFnRunnerFactory.simple(
            cxt.getCurrentTransform(), input, SparkSideInputReader.empty(), false);
    MetricsAccumulator metrics = MetricsAccumulator.getInstance(cxt.getSparkSession());

    BeamStatefulProcessor<K, V, OutputT> processor =
        new BeamStatefulProcessor<>(
            doFn, runnerFactory, inputCoder, windowing, cxt.getOptionsSupplier(), metrics);

    MapFunction<WindowedValue<KV<K, V>>, K> keyFn = v -> v.getValue().getKey();
    Dataset<WindowedValue<OutputT>> result =
        cxt.getDataset(input)
            .groupByKey(keyFn, keyEnc)
            .transformWithState(
                processor,
                TimeMode.EventTime(),
                OutputMode.Append(),
                cxt.windowedEncoder(outputCoder));

    cxt.putDataset(output, result);
  }
}
