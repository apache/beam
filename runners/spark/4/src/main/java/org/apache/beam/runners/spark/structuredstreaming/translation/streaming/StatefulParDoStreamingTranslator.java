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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.BeamStatefulProcessorConfig;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * Streaming translator for a stateful {@link ParDo.MultiOutput}, that is a {@code ParDo} whose
 * {@code DoFn} declares {@code @StateId} state or {@code @TimerId} timers. Stateless {@code ParDo}s
 * keep using the batch translator unchanged, the streaming registry only routes stateful ones here.
 *
 * <p>The user's {@code DoFn} is hosted verbatim by the generic {@code transformWithState} operator
 * in {@code BeamStatefulProcessorConfig.Mode#STATEFUL_PARDO}, which runs it through {@code
 * DoFnRunners.simpleRunner} wrapped in {@code DoFnRunners.defaultStatefulDoFnRunner}, so Beam's own
 * state, timer and window garbage collection semantics apply. This translator's whole job is the
 * byte[] row plumbing documented on {@link TwsTransformFactory}: encode {@code WindowedValue<KV<K,
 * V>>} into keyed input rows, and split the tagged output rows back into one {@code
 * Dataset<WindowedValue<T>>} per {@link TupleTag}.
 *
 * <p>Note on multiple outputs: each additional tag adds one {@code filter} plus one {@code map} on
 * top of the same operator, which Spark plans as a separate branch. Only outputs that are actually
 * consumed downstream, plus the main output, get a dataset at all.
 */
public class StatefulParDoStreamingTranslator<K, V, OutputT>
    extends TransformTranslator<
        PCollection<? extends KV<K, V>>, PCollectionTuple, ParDo.MultiOutput<KV<K, V>, OutputT>> {

  public StatefulParDoStreamingTranslator() {
    super(0.2f);
  }

  /**
   * Unlike the batch translators, this override never returns {@code false}: {@link
   * #rejectUnsupported} throws with the offending feature named instead. Silently declining here
   * would make {@code PipelineTranslator#getSupportedTranslator} fall back to the batch {@code
   * ParDo} translator, which would run the stateful {@code DoFn} without the streaming state and
   * timer semantics and produce quietly wrong results.
   */
  @Override
  protected boolean canTranslate(ParDo.MultiOutput<KV<K, V>, OutputT> transform) {
    rejectUnsupported(transform);
    return true;
  }

  /**
   * Throws when the stateful {@code ParDo} uses a feature this translator does not implement,
   * naming that feature; returns normally otherwise.
   */
  private void rejectUnsupported(ParDo.MultiOutput<KV<K, V>, OutputT> transform) {
    DoFn<KV<K, V>, OutputT> doFn = transform.getFn();
    String stepName = doFn.getClass().getName();
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(doFn);

    checkState(
        !signature.processElement().isSplittable(),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    StreamingTranslationHelpers.checkNoProcessingTimeTimers(doFn, signature, stepName);

    if (signature.onWindowExpiration() != null) {
      throw StreamingTranslationHelpers.unsupported(stepName, "@OnWindowExpiration");
    }
    if (signature.processElement().requiresTimeSortedInput()) {
      throw StreamingTranslationHelpers.unsupported(stepName, "@RequiresTimeSortedInput");
    }
    if (!transform.getSideInputs().isEmpty()) {
      throw StreamingTranslationHelpers.unsupported(
          stepName,
          "side inputs on a stateful ParDo. Broadcasting a side input requires collecting its "
              + "PCollection, which is not possible while the pipeline is streaming");
    }
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected void translate(ParDo.MultiOutput<KV<K, V>, OutputT> transform, Context cxt) {
    PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) cxt.getInput();
    String stepName = cxt.getCurrentTransform().getFullName();

    WindowingStrategy<?, ?> windowing = input.getWindowingStrategy();
    StreamingTranslationHelpers.checkSupportedWindowing(windowing, stepName);

    // Beam guarantees a stateful DoFn is applied to a keyed PCollection, but say so clearly rather
    // than failing with a ClassCastException deep in the operator.
    if (!(input.getCoder() instanceof KvCoder)) {
      throw StreamingTranslationHelpers.unsupported(
          stepName,
          "state or timers on the non KV input coder "
              + input.getCoder()
              + ". A stateful ParDo must be applied to a PCollection of KVs");
    }
    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputCoder.getKeyCoder();
    Coder<V> valueCoder = inputCoder.getValueCoder();
    StreamingTranslationHelpers.checkDeterministicKeyCoder(keyCoder, stepName);

    Coder<? extends BoundedWindow> windowCoder = windowing.getWindowFn().windowCoder();

    TupleTag<OutputT> mainOutputTag = transform.getMainOutputTag();
    List<TupleTag<?>> additionalOutputTags =
        new ArrayList<>(transform.getAdditionalOutputTags().getAll());

    // One coder per tag the DoFn may emit to, taken from the PCollection behind that tag.
    Map<TupleTag<?>, Coder<?>> outputCoders = new LinkedHashMap<>();
    outputCoders.put(mainOutputTag, cxt.getOutput(mainOutputTag).getCoder());
    for (TupleTag<?> tag : additionalOutputTags) {
      outputCoders.put(tag, cxt.getOutput((TupleTag) tag).getCoder());
    }

    Dataset<byte[]> keyedRows =
        cxt.getDataset(input)
            .map(
                new StreamingTranslationHelpers.EncodeKeyedRow<>(
                    keyCoder, WindowedValues.getFullCoder(valueCoder, windowCoder)),
                Encoders.BINARY());

    BeamStatefulProcessorConfig config =
        BeamStatefulProcessorConfig.builder()
            .setMode(BeamStatefulProcessorConfig.Mode.STATEFUL_PARDO)
            .setDoFn(transform.getFn())
            .setKeyCoder(keyCoder)
            .setValueCoder(valueCoder)
            .setWindowingStrategy(windowing)
            .setMainOutputTag(mainOutputTag)
            .setAdditionalOutputTags(additionalOutputTags)
            .setOutputCoders(outputCoders)
            .setDoFnSchemaInformation(
                ParDoTranslation.getSchemaInformation(cxt.getCurrentTransform()))
            .setOptionsSupplier(
                StreamingTranslationHelpers.optionsSupplier(cxt.getOptionsSupplier()))
            .setStepName(stepName)
            .build();

    Dataset<byte[]> outputRows = TwsTransformFactory.transform(keyedRows, config);

    List<TupleTag<?>> allTags = config.outputTags();
    boolean singleTag = allTags.size() == 1;
    for (int tagIndex = 0; tagIndex < allTags.size(); tagIndex++) {
      TupleTag<Object> tag = (TupleTag<Object>) allTags.get(tagIndex);
      PCollection<Object> outputPCollection = cxt.getOutput(tag);
      if (tagIndex > 0 && cxt.isLeaf(outputPCollection)) {
        // An additional output nobody consumes: emitting it would start a whole extra streaming
        // query re-running this operator for rows that are then thrown away.
        continue;
      }
      Coder<Object> outputCoder = outputPCollection.getCoder();
      Encoder<WindowedValue<Object>> encoder = cxt.windowedEncoder(outputCoder);
      Dataset<byte[]> taggedRows =
          singleTag
              ? outputRows
              : outputRows.filter(new StreamingTranslationHelpers.TagIndexFilter(tagIndex));
      cxt.putDataset(
          outputPCollection,
          taggedRows.map(
              new StreamingTranslationHelpers.DecodeTaggedOutput<>(
                  WindowedValues.getFullCoder(outputCoder, windowCoder)),
              encoder));
    }
  }
}
