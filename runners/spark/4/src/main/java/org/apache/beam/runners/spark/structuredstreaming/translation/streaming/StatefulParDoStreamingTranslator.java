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

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.oneOfEncoder;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.ParDoTranslatorBatch;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.ParDoStatefulProcessor;
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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.TimeMode;
import scala.Tuple2;

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
    Coder<K> keyCoder = inputCoder.getKeyCoder();
    TupleTag<OutputT> mainOutputTag = transform.getMainOutputTag();
    Map<TupleTag<?>, PCollection<?>> outputs =
        ParDoTranslatorBatch.skipUnconsumedOutputs(
            cxt.getOutputs(), mainOutputTag, transform.getAdditionalOutputTags(), cxt);

    MetricsAccumulator metrics = MetricsAccumulator.getInstance(cxt.getSparkSession());
    MapFunction<WindowedValue<KV<K, V>>, byte[]> keyFn =
        v -> CoderHelpers.toByteArray(v.getValue().getKey(), keyCoder);

    if (outputs.size() > 1) {
      Map<String, Integer> tagColIdx = ParDoTranslatorBatch.tagsColumnIndex(outputs.keySet());
      List<Encoder<WindowedValue<Object>>> encoders =
          ParDoTranslatorBatch.createEncoders(outputs, tagColIdx, cxt);

      DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory =
          DoFnRunnerFactory.simple(
              cxt.getCurrentTransform(), input, SparkSideInputReader.empty(), false);
      ParDoStatefulProcessor<K, V, OutputT, Tuple2<Integer, WindowedValue<Object>>> processor =
          ParDoStatefulProcessor.multiOutput(
              doFn,
              runnerFactory,
              inputCoder,
              windowing,
              cxt.getOptionsSupplier(),
              metrics,
              tagColIdx);

      Dataset<Tuple2<Integer, WindowedValue<Object>>> allTagsDS =
          cxt.getDataset(input)
              .groupByKey(keyFn, Encoders.BINARY())
              .transformWithState(
                  processor, TimeMode.EventTime(), OutputMode.Append(), oneOfEncoder(encoders));

      // Each consumed tag runs its own stateful operator with its own state, see #41425.
      for (TupleTag<?> tag : outputs.keySet()) {
        int colIdx = checkStateNotNull(tagColIdx.get(tag.getId()), "Unknown tag");
        TypedColumn<Tuple2<Integer, WindowedValue<Object>>, WindowedValue<Object>> col =
            (TypedColumn) col(Integer.toString(colIdx)).as(encoders.get(colIdx));
        cxt.putDataset(
            cxt.getOutput((TupleTag) tag), allTagsDS.filter(col.isNotNull()).select(col), false);
      }
    } else {
      PCollection<OutputT> output = cxt.getOutput(mainOutputTag);
      boolean filterMainOutput = cxt.getOutputs().size() > 1;
      DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory =
          DoFnRunnerFactory.simple(
              cxt.getCurrentTransform(), input, SparkSideInputReader.empty(), filterMainOutput);
      ParDoStatefulProcessor<K, V, OutputT, WindowedValue<OutputT>> processor =
          ParDoStatefulProcessor.singleOutput(
              doFn, runnerFactory, inputCoder, windowing, cxt.getOptionsSupplier(), metrics);

      Dataset<WindowedValue<OutputT>> result =
          cxt.getDataset(input)
              .groupByKey(keyFn, Encoders.BINARY())
              .transformWithState(
                  processor,
                  TimeMode.EventTime(),
                  OutputMode.Append(),
                  cxt.windowedEncoder(output.getCoder()));

      cxt.putDataset(output, result);
    }
  }
}
