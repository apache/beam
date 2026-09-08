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

import java.util.Collections;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.BeamStatefulProcessorConfig;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * Streaming translator for {@link GroupByKey}, implemented as a group-also-by-window hosted by the
 * generic {@code transformWithState} operator.
 *
 * <p>Unlike the batch translator, which may ignore triggering and simply collect every value of a
 * key, a streaming {@code GroupByKey} has to respect the watermark: a window's single on-time pane
 * is emitted only once the watermark has passed the end of that window, and values arriving after
 * that are dropped as late. Both are the responsibility of the Beam {@code ReduceFnRunner} that
 * {@code BeamStatefulProcessorConfig.Mode#GROUP_ALSO_BY_WINDOW} sets up inside the operator, so all
 * this translator does is put the data into and take it back out of the operator's byte[] row
 * layout, see {@link TwsTransformFactory}.
 *
 * <p>{@code Combine.PerKey} is deliberately not registered for streaming, so combines reach this
 * translator already expanded into {@code GroupByKey} plus a plain {@code ParDo}.
 */
public class GroupByKeyStreamingTranslator<K, V>
    extends TransformTranslator<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  /** Output tag of the group-also-by-window {@code DoFn}, index 0, its only output. */
  private static final String MAIN_OUTPUT_TAG_ID = "gbk-main-output";

  public GroupByKeyStreamingTranslator() {
    super(0.2f);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void translate(GroupByKey<K, V> transform, Context cxt) {
    PCollection<KV<K, V>> input = cxt.getInput();
    String stepName = cxt.getCurrentTransform().getFullName();

    WindowingStrategy<?, ?> windowing = input.getWindowingStrategy();
    StreamingTranslationHelpers.checkSupportedWindowing(windowing, stepName);

    if (!(input.getCoder() instanceof KvCoder)) {
      throw StreamingTranslationHelpers.unsupported(
          stepName, "the non KV input coder " + input.getCoder());
    }
    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputCoder.getKeyCoder();
    Coder<V> valueCoder = inputCoder.getValueCoder();
    StreamingTranslationHelpers.checkDeterministicKeyCoder(keyCoder, stepName);

    Coder<? extends BoundedWindow> windowCoder = windowing.getWindowFn().windowCoder();
    KvCoder<K, Iterable<V>> outputCoder = KvCoder.of(keyCoder, IterableCoder.of(valueCoder));
    TupleTag<KV<K, Iterable<V>>> mainOutputTag = new TupleTag<>(MAIN_OUTPUT_TAG_ID);

    Dataset<byte[]> keyedRows =
        cxt.getDataset(input)
            .map(
                new StreamingTranslationHelpers.EncodeKeyedRow<>(
                    keyCoder, WindowedValues.getFullCoder(valueCoder, windowCoder)),
                Encoders.BINARY());

    BeamStatefulProcessorConfig config =
        BeamStatefulProcessorConfig.builder()
            .setMode(BeamStatefulProcessorConfig.Mode.GROUP_ALSO_BY_WINDOW)
            .setKeyCoder(keyCoder)
            .setValueCoder(valueCoder)
            .setWindowingStrategy(windowing)
            .setMainOutputTag(mainOutputTag)
            .setOutputCoders(
                Collections.<TupleTag<?>, Coder<?>>singletonMap(mainOutputTag, outputCoder))
            .setOptionsSupplier(
                StreamingTranslationHelpers.optionsSupplier(cxt.getOptionsSupplier()))
            .setStepName(stepName)
            .build();

    // GROUP_ALSO_BY_WINDOW has a single output tag, so every row carries index 0 and no filtering
    // by tag is needed on the way out.
    Dataset<byte[]> outputRows = TwsTransformFactory.transform(keyedRows, config);

    Encoder<WindowedValue<KV<K, Iterable<V>>>> encoder = cxt.windowedEncoder(outputCoder);
    Dataset<WindowedValue<KV<K, Iterable<V>>>> result =
        outputRows.map(
            new StreamingTranslationHelpers.DecodeTaggedOutput<>(
                WindowedValues.getFullCoder(outputCoder, windowCoder)),
            encoder);

    cxt.putDataset(cxt.getOutput(), result);
  }
}
