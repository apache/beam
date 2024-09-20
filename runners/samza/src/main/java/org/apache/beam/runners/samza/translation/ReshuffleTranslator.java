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
package org.apache.beam.runners.samza.translation;

import com.google.auto.service.AutoService;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.runners.samza.util.WindowUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.KVSerde;

/**
 * Translates Reshuffle transform into Samza's native partitionBy operator, which will partition
 * each incoming message by the key into a Task corresponding to that key.
 */
public class ReshuffleTranslator<K, InT, OutT>
    implements TransformTranslator<PTransform<PCollection<KV<K, InT>>, PCollection<KV<K, OutT>>>> {

  private final String prefix;

  ReshuffleTranslator(String prefix) {
    this.prefix = prefix;
  }

  ReshuffleTranslator() {
    this("rshfl-");
  }

  @Override
  public void translate(
      PTransform<PCollection<KV<K, InT>>, PCollection<KV<K, OutT>>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {

    final PCollection<KV<K, InT>> input = ctx.getInput(transform);
    final PCollection<KV<K, OutT>> output = ctx.getOutput(transform);
    final MessageStream<OpMessage<KV<K, InT>>> inputStream = ctx.getMessageStream(input);
    // input will be OpMessage of Windowed<KV<K, Iterable<V>>>
    final KvCoder<K, InT> inputCoder = (KvCoder<K, InT>) input.getCoder();
    final Coder<WindowedValue<KV<K, InT>>> elementCoder = SamzaCoders.of(input);

    final MessageStream<OpMessage<KV<K, InT>>> outputStream =
        doTranslate(
            inputStream,
            inputCoder.getKeyCoder(),
            elementCoder,
            prefix + ctx.getTransformId(),
            ctx.getPipelineOptions().getMaxSourceParallelism() > 1);

    ctx.registerMessageStream(output, outputStream);
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {

    final String inputId = ctx.getInputId(transform);
    final MessageStream<OpMessage<KV<K, InT>>> inputStream = ctx.getMessageStreamById(inputId);
    final WindowedValue.WindowedValueCoder<KV<K, InT>> windowedInputCoder =
        WindowUtils.instantiateWindowedCoder(inputId, pipeline.getComponents());
    final String outputId = ctx.getOutputId(transform);

    final MessageStream<OpMessage<KV<K, InT>>> outputStream =
        doTranslate(
            inputStream,
            ((KvCoder<K, InT>) windowedInputCoder.getValueCoder()).getKeyCoder(),
            windowedInputCoder,
            prefix + ctx.getTransformId(),
            ctx.getPipelineOptions().getMaxSourceParallelism() > 1);

    ctx.registerMessageStream(outputId, outputStream);
  }

  private static <K, InT> MessageStream<OpMessage<KV<K, InT>>> doTranslate(
      MessageStream<OpMessage<KV<K, InT>>> inputStream,
      Coder<K> keyCoder,
      Coder<WindowedValue<KV<K, InT>>> valueCoder,
      String partitionById, // will be used in the intermediate stream name
      boolean needRepartition) {

    return needRepartition
        ? inputStream
            .filter(op -> OpMessage.Type.ELEMENT == op.getType())
            .partitionBy(
                opMessage -> opMessage.getElement().getValue().getKey(),
                OpMessage::getElement, // windowed value
                KVSerde.of(SamzaCoders.toSerde(keyCoder), SamzaCoders.toSerde(valueCoder)),
                partitionById)
            // convert back to OpMessage
            .map(kv -> OpMessage.ofElement(kv.getValue()))
        : inputStream.filter(op -> OpMessage.Type.ELEMENT == op.getType());
  }

  /** Predicate to determine whether a URN is a Samza native transform. */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsSamzaNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return false;
      // Re-enable after https://github.com/apache/beam/issues/21188 is completed
      //       return PTransformTranslation.RESHUFFLE_URN.equals(
      //          PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }
}
