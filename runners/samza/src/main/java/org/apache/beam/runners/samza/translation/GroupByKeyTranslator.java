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

import java.io.IOException;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.CombineTranslation;
import org.apache.beam.runners.samza.runtime.DoFnOp;
import org.apache.beam.runners.samza.runtime.GroupByKeyOp;
import org.apache.beam.runners.samza.runtime.KvToKeyedWorkItemOp;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.KVSerde;

/**
 * Translates {@link GroupByKey} to Samza {@link GroupByKeyOp}.
 */
class GroupByKeyTranslator<K, InputT, OutputT>
    implements TransformTranslator<PTransform<PCollection<KV<K, InputT>>,
                                   PCollection<KV<K, OutputT>>>> {

  @Override
  public void
  translate(PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
            TransformHierarchy.Node node,
            TranslationContext ctx) {
    final PCollection<KV<K, InputT>> input = ctx.getInput(transform);

    final PCollection<KV<K, OutputT>> output = ctx.getOutput(transform);
    final TupleTag<KV<K, OutputT>> outputTag = ctx.getOutputTag(transform);

    @SuppressWarnings("unchecked")
    final WindowingStrategy<?, BoundedWindow> windowingStrategy =
        (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

    final MessageStream<OpMessage<KV<K, InputT>>> inputStream = ctx.getMessageStream(input);

    final KvCoder<K, InputT> kvInputCoder = (KvCoder<K, InputT>) input.getCoder();
    final Coder<WindowedValue<KV<K, InputT>>> elementCoder = SamzaCoders.of(input);

    final MessageStream<OpMessage<KV<K, InputT>>> filteredInputStream = inputStream
        .filter(msg -> msg.getType() == OpMessage.Type.ELEMENT);

    final MessageStream<OpMessage<KV<K, InputT>>> partitionedInputStream;
    if (ctx.getPipelineOptions().getMaxSourceParallelism() == 1) {
      // Only one task will be created, no need for repartition
      partitionedInputStream = filteredInputStream;
    } else {
      partitionedInputStream = filteredInputStream
          .partitionBy(msg -> msg.getElement().getValue().getKey(), msg -> msg.getElement(),
              KVSerde.of(
                  SamzaCoders.toSerde(kvInputCoder.getKeyCoder()),
                  SamzaCoders.toSerde(elementCoder)),
              "gbk-" + ctx.getCurrentTopologicalId())
          .map(kv -> OpMessage.ofElement(kv.getValue()));
    }

    final Coder<KeyedWorkItem<K, InputT>> keyedWorkItemCoder =
        KeyedWorkItemCoder.of(
            kvInputCoder.getKeyCoder(),
            kvInputCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn =
        getSystemReduceFn(
            transform,
            ctx.getCurrentTransform(),
            input.getPipeline(),
            kvInputCoder);

    final MessageStream<OpMessage<KV<K, OutputT>>> outputStream =
        partitionedInputStream
            .flatMap(OpAdapter.adapt(new KvToKeyedWorkItemOp<>()))
            .flatMap(OpAdapter.adapt(new GroupByKeyOp<>(
                outputTag,
                keyedWorkItemCoder,
                reduceFn,
                windowingStrategy,
                new DoFnOp.SingleOutputManagerFactory<>(),
                node.getFullName())));

    ctx.registerMessageStream(output, outputStream);
  }

  @SuppressWarnings("unchecked")
  private SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> getSystemReduceFn(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      AppliedPTransform<?, ?, ?> appliedPTransform,
      Pipeline pipeline,
      KvCoder<K, InputT> kvInputCoder) {

    if (transform instanceof GroupByKey) {
      return (SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow>)
          SystemReduceFn.buffering(kvInputCoder.getValueCoder());
    } else if (transform instanceof Combine.PerKey) {
      final CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> combineFn;
      try {
        combineFn = (CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT>)
            CombineTranslation.getCombineFn(appliedPTransform)
                .orElseThrow(() ->
                    new IOException("CombineFn not found in node."));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return SystemReduceFn.combining(
          kvInputCoder.getKeyCoder(),
          AppliedCombineFn
              .withInputCoder(combineFn, pipeline.getCoderRegistry(), kvInputCoder));
    } else {
      throw new RuntimeException("Transform " + transform + " cannot be translated as GroupByKey.");
    }
  }
}
