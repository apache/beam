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

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.DoFnOp;
import org.apache.beam.runners.samza.runtime.GroupByKeyOp;
import org.apache.beam.runners.samza.runtime.KvToKeyedWorkItemOp;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.transforms.GroupWithoutRepartition;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils;
import org.apache.beam.runners.samza.util.WindowUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.serializers.KVSerde;

/** Translates {@link GroupByKey} to Samza {@link GroupByKeyOp}. */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
class GroupByKeyTranslator<K, InputT, OutputT>
    implements TransformTranslator<
            PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>>,
        TransformConfigGenerator<
            PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

  @Override
  public void translate(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    doTranslate(transform, node, ctx);
  }

  private static <K, InputT, OutputT> void doTranslate(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
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

    final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn =
        getSystemReduceFn(transform, input.getPipeline(), kvInputCoder);

    final MessageStream<OpMessage<KV<K, OutputT>>> outputStream =
        doTranslateGBK(
            inputStream,
            needRepartition(node, ctx),
            reduceFn,
            windowingStrategy,
            kvInputCoder,
            elementCoder,
            ctx,
            outputTag,
            input.isBounded());

    ctx.registerMessageStream(output, outputStream);
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    final String inputId = ctx.getInputId(transform);
    final RunnerApi.PCollection input = pipeline.getComponents().getPcollectionsOrThrow(inputId);
    final MessageStream<OpMessage<KV<K, InputT>>> inputStream = ctx.getMessageStreamById(inputId);
    final WindowingStrategy<?, BoundedWindow> windowingStrategy =
        WindowUtils.getWindowStrategy(inputId, pipeline.getComponents());
    final WindowedValue.WindowedValueCoder<KV<K, InputT>> windowedInputCoder =
        WindowUtils.instantiateWindowedCoder(inputId, pipeline.getComponents());
    final TupleTag<KV<K, OutputT>> outputTag =
        new TupleTag<>(Iterables.getOnlyElement(transform.getTransform().getOutputsMap().keySet()));

    final MessageStream<OpMessage<KV<K, OutputT>>> outputStream =
        doTranslatePortable(
            input, inputStream, windowingStrategy, windowedInputCoder, outputTag, ctx);

    ctx.registerMessageStream(ctx.getOutputId(transform), outputStream);
  }

  @Override
  public Map<String, String> createConfig(
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
      TransformHierarchy.Node node,
      ConfigContext ctx) {
    return ConfigBuilder.createRocksDBStoreConfig(ctx.getPipelineOptions());
  }

  @Override
  public Map<String, String> createPortableConfig(
      PipelineNode.PTransformNode transform, SamzaPipelineOptions options) {
    return ConfigBuilder.createRocksDBStoreConfig(options);
  }

  /**
   * The method is used to translate both portable GBK transform as well as grouping side inputs
   * into Samza.
   */
  static <K, InputT, OutputT> MessageStream<OpMessage<KV<K, OutputT>>> doTranslatePortable(
      RunnerApi.PCollection input,
      MessageStream<OpMessage<KV<K, InputT>>> inputStream,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      WindowedValue.WindowedValueCoder<KV<K, InputT>> windowedInputCoder,
      TupleTag<KV<K, OutputT>> outputTag,
      PortableTranslationContext ctx) {
    final boolean needRepartition = ctx.getPipelineOptions().getMaxSourceParallelism() > 1;
    final Coder<BoundedWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    final KvCoder<K, InputT> kvInputCoder = (KvCoder<K, InputT>) windowedInputCoder.getValueCoder();
    final Coder<WindowedValue<KV<K, InputT>>> elementCoder =
        WindowedValue.FullWindowedValueCoder.of(kvInputCoder, windowCoder);

    @SuppressWarnings("unchecked")
    final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn =
        (SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow>)
            SystemReduceFn.buffering(kvInputCoder.getValueCoder());

    final PCollection.IsBounded isBounded = SamzaPipelineTranslatorUtils.isBounded(input);

    return doTranslateGBK(
        inputStream,
        needRepartition,
        reduceFn,
        windowingStrategy,
        kvInputCoder,
        elementCoder,
        ctx,
        outputTag,
        isBounded);
  }

  private static <K, InputT, OutputT> MessageStream<OpMessage<KV<K, OutputT>>> doTranslateGBK(
      MessageStream<OpMessage<KV<K, InputT>>> inputStream,
      boolean needRepartition,
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      KvCoder<K, InputT> kvInputCoder,
      Coder<WindowedValue<KV<K, InputT>>> elementCoder,
      TranslationContext ctx,
      TupleTag<KV<K, OutputT>> outputTag,
      PCollection.IsBounded isBounded) {
    final MessageStream<OpMessage<KV<K, InputT>>> filteredInputStream =
        inputStream.filter(msg -> msg.getType() == OpMessage.Type.ELEMENT);

    final MessageStream<OpMessage<KV<K, InputT>>> partitionedInputStream;
    if (!needRepartition) {
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
                  "gbk-" + escape(ctx.getTransformId()))
              .map(kv -> OpMessage.ofElement(kv.getValue()));
    }

    final Coder<KeyedWorkItem<K, InputT>> keyedWorkItemCoder =
        KeyedWorkItemCoder.of(
            kvInputCoder.getKeyCoder(),
            kvInputCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    final MessageStream<OpMessage<KV<K, OutputT>>> outputStream =
        partitionedInputStream
            .flatMapAsync(OpAdapter.adapt(new KvToKeyedWorkItemOp<>(), ctx))
            .flatMapAsync(
                OpAdapter.adapt(
                    new GroupByKeyOp<>(
                        outputTag,
                        keyedWorkItemCoder,
                        reduceFn,
                        windowingStrategy,
                        new DoFnOp.SingleOutputManagerFactory<>(),
                        ctx.getTransformFullName(),
                        ctx.getTransformId(),
                        isBounded),
                    ctx));
    return outputStream;
  }

  @SuppressWarnings("unchecked")
  private static <K, InputT, OutputT>
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> getSystemReduceFn(
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
          Pipeline pipeline,
          KvCoder<K, InputT> kvInputCoder) {
    if (transform instanceof GroupByKey) {
      return (SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow>)
          SystemReduceFn.buffering(kvInputCoder.getValueCoder());
    } else if (transform instanceof Combine.PerKey) {
      final CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> combineFn =
          ((Combine.PerKey) transform).getFn();
      return SystemReduceFn.combining(
          kvInputCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(combineFn, pipeline.getCoderRegistry(), kvInputCoder));
    } else {
      throw new RuntimeException("Transform " + transform + " cannot be translated as GroupByKey.");
    }
  }

  private static boolean needRepartition(TransformHierarchy.Node node, TranslationContext ctx) {
    if (ctx.getPipelineOptions().getMaxSourceParallelism() == 1) {
      // Only one task will be created, no need for repartition
      return false;
    }

    if (node == null) {
      return true;
    }

    if (node.getTransform() instanceof GroupWithoutRepartition) {
      return false;
    } else {
      return needRepartition(node.getEnclosingNode(), ctx);
    }
  }
}
