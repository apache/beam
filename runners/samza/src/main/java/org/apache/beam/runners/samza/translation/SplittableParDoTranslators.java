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

import static org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils.escape;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.DoFnOp;
import org.apache.beam.runners.samza.runtime.KvToKeyedWorkItemOp;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.runtime.SplittableParDoProcessKeyedElementsOp;
import org.apache.beam.runners.samza.translation.ParDoBoundMultiTranslator.RawUnionValueToValue;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.KVSerde;

/** A set of translators for {@link SplittableParDo}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SplittableParDoTranslators {

  /**
   * Translates {@link SplittableParDo.ProcessKeyedElements} to Samza {@link
   * SplittableParDoProcessKeyedElementsOp}.
   */
  static class ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
      implements TransformTranslator<
          SplittableParDo.ProcessKeyedElements<
              InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>> {

    @Override
    public void translate(
        SplittableParDo.ProcessKeyedElements<
                InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
            transform,
        Node node,
        TranslationContext ctx) {
      final PCollection<KV<byte[], KV<InputT, RestrictionT>>> input = ctx.getInput(transform);

      final ArrayList<Map.Entry<TupleTag<?>, PCollection<?>>> outputs =
          new ArrayList<>(node.getOutputs().entrySet());

      final Map<TupleTag<?>, Integer> tagToIndexMap = new HashMap<>();
      final Map<Integer, PCollection<?>> indexToPCollectionMap = new HashMap<>();

      for (int index = 0; index < outputs.size(); ++index) {
        final Map.Entry<TupleTag<?>, PCollection<?>> taggedOutput = outputs.get(index);
        tagToIndexMap.put(taggedOutput.getKey(), index);

        if (!(taggedOutput.getValue() instanceof PCollection)) {
          throw new IllegalArgumentException(
              "Expected side output to be PCollection, but was: " + taggedOutput.getValue());
        }
        final PCollection<?> sideOutputCollection = (PCollection<?>) taggedOutput.getValue();
        indexToPCollectionMap.put(index, sideOutputCollection);
      }

      @SuppressWarnings("unchecked")
      final WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      final MessageStream<OpMessage<KV<byte[], KV<InputT, RestrictionT>>>> inputStream =
          ctx.getMessageStream(input);

      final KvCoder<byte[], KV<InputT, RestrictionT>> kvInputCoder =
          (KvCoder<byte[], KV<InputT, RestrictionT>>) input.getCoder();
      final Coder<WindowedValue<KV<byte[], KV<InputT, RestrictionT>>>> elementCoder =
          SamzaCoders.of(input);

      final MessageStream<OpMessage<KV<byte[], KV<InputT, RestrictionT>>>> filteredInputStream =
          inputStream.filter(msg -> msg.getType() == OpMessage.Type.ELEMENT);

      final MessageStream<OpMessage<KV<byte[], KV<InputT, RestrictionT>>>> partitionedInputStream;
      if (!needRepartition(ctx)) {
        partitionedInputStream = filteredInputStream;
      } else {
        partitionedInputStream =
            filteredInputStream
                .partitionBy(
                    msg -> msg.getElement().getValue().getKey(),
                    msg -> msg.getElement(),
                    KVSerde.of(
                        SamzaCoders.toSerde(kvInputCoder.getKeyCoder()),
                        SamzaCoders.toSerde(elementCoder)),
                    "sdf-" + escape(ctx.getTransformId()))
                .map(kv -> OpMessage.ofElement(kv.getValue()));
      }

      final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
          partitionedInputStream
              .flatMapAsync(OpAdapter.adapt(new KvToKeyedWorkItemOp<>(), ctx))
              .flatMapAsync(
                  OpAdapter.adapt(
                      new SplittableParDoProcessKeyedElementsOp<>(
                          transform.getMainOutputTag(),
                          transform,
                          windowingStrategy,
                          new DoFnOp.MultiOutputManagerFactory(tagToIndexMap),
                          ctx.getTransformFullName(),
                          ctx.getTransformId(),
                          input.isBounded()),
                      ctx));

      for (int outputIndex : tagToIndexMap.values()) {
        @SuppressWarnings("unchecked")
        final MessageStream<OpMessage<OutputT>> outputStream =
            taggedOutputStream
                .filter(
                    message ->
                        message.getType() != OpMessage.Type.ELEMENT
                            || message.getElement().getValue().getUnionTag() == outputIndex)
                .flatMapAsync(OpAdapter.adapt(new RawUnionValueToValue(), ctx));

        ctx.registerMessageStream(indexToPCollectionMap.get(outputIndex), outputStream);
      }
    }

    private static boolean needRepartition(TranslationContext ctx) {
      if (ctx.getPipelineOptions().getMaxSourceParallelism() == 1) {
        // Only one task will be created, no need for repartition
        return false;
      }
      return true;
    }
  }
}
