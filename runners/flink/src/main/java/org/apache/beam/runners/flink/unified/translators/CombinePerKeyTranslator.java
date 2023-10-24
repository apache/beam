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
package org.apache.beam.runners.flink.unified.translators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

public class CombinePerKeyTranslator<K, InputT, OutputT>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  static class ToKeyedWorkItem<K, InputT>
      extends RichFlatMapFunction<
          WindowedValue<KV<K, InputT>>, WindowedValue<KeyedWorkItem<K, InputT>>> {

    private final SerializablePipelineOptions options;

    ToKeyedWorkItem(PipelineOptions options) {
      this.options = new SerializablePipelineOptions(options);
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public void flatMap(
        WindowedValue<KV<K, InputT>> inWithMultipleWindows,
        Collector<WindowedValue<KeyedWorkItem<K, InputT>>> out) {

      // we need to wrap each one work item per window for now
      // since otherwise the PushbackSideInputRunner will not correctly
      // determine whether side inputs are ready
      //
      // this is tracked as https://github.com/apache/beam/issues/18358
      for (WindowedValue<KV<K, InputT>> in : inWithMultipleWindows.explodeWindows()) {
        SingletonKeyedWorkItem<K, InputT> workItem =
            new SingletonKeyedWorkItem<>(
                in.getValue().getKey(), in.withValue(in.getValue().getValue()));

        out.collect(in.withValue(workItem));
      }
    }
  }

  @Override
  public void translate(
      PTransformNode transform, Pipeline pipeline, UnifiedTranslationContext context) {

    RunnerApi.PTransform pTransform = transform.getTransform();
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    String fullName = pTransform.getUniqueName();

    WindowingStrategy<?, ?> windowingStrategy =
        context.getWindowingStrategy(pipeline, inputPCollectionId);

    WindowedValueCoder<KV<K, InputT>> windowedInputCoder =
        (WindowedValueCoder) context.getWindowedInputCoder(pipeline, inputPCollectionId);

    KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) windowedInputCoder.getValueCoder();

    SingletonKeyedWorkItemCoder<K, InputT> workItemCoder =
        SingletonKeyedWorkItemCoder.of(
            inputKvCoder.getKeyCoder(),
            inputKvCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    DataStream<WindowedValue<KV<K, InputT>>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    WindowedValue.FullWindowedValueCoder<KeyedWorkItem<K, InputT>> windowedWorkItemCoder =
        WindowedValue.getFullCoder(workItemCoder, windowingStrategy.getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<KeyedWorkItem<K, InputT>>> workItemTypeInfo =
        new CoderTypeInformation<>(windowedWorkItemCoder, context.getPipelineOptions());

    DataStream<WindowedValue<KeyedWorkItem<K, InputT>>> workItemStream =
        inputDataStream
            .flatMap(new ToKeyedWorkItem<>(context.getPipelineOptions()))
            .returns(workItemTypeInfo)
            .name("ToKeyedWorkItem");

    WorkItemKeySelector<K, InputT> keySelector =
        new WorkItemKeySelector<>(
            inputKvCoder.getKeyCoder(),
            new SerializablePipelineOptions(context.getPipelineOptions()));

    KeyedStream<WindowedValue<KeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream =
        workItemStream.keyBy(keySelector);

    CombinePayload combinePayload;
    try {
      combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    // TODO: not sure this is correct
    // TODO: Combine with Side input will NOT be translated into Combine.Globally instances.
    // see: https://github.com/apache/beam/pull/4924
    @SuppressWarnings("unchecked")
    GlobalCombineFn<? super InputT, ?, OutputT> combineFn =
        (GlobalCombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");

    Coder<?> accumulatorCoder;
    try {
      accumulatorCoder =
          context.getComponents(pipeline).getCoder(combinePayload.getAccumulatorCoderId());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    @SuppressWarnings("unchecked")
    AppliedCombineFn<K, InputT, ?, OutputT> appliedCombineFn =
        AppliedCombineFn.withAccumulatorCoder(combineFn, (Coder) accumulatorCoder);

    SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn =
        SystemReduceFn.combining(inputKvCoder.getKeyCoder(), appliedCombineFn);
    // TODO: EOF not sure this is correct

    Coder<WindowedValue<KV<K, OutputT>>> outputCoder =
        context.getWindowedInputCoder(pipeline, outputPCollectionId);

    TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo =
        context.getTypeInfo(pipeline, outputPCollectionId);

    // TODO: DO we need to support Combine with side inputs ? Is that even a thing ?
    TupleTag<KV<K, OutputT>> mainTag = new TupleTag<>("main output");

    @SuppressWarnings("unchecked")
    WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
        new WindowDoFnOperator<>(
            reduceFn,
            fullName,
            (Coder) windowedWorkItemCoder,
            mainTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
                mainTag,
                outputCoder,
                new SerializablePipelineOptions(context.getPipelineOptions())),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            context.getPipelineOptions(),
            inputKvCoder.getKeyCoder(),
            keySelector);

    SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream =
        keyedWorkItemStream.transform(fullName, outputTypeInfo, doFnOperator).uid(fullName);

    context.addDataStream(outputPCollectionId, outDataStream);
  }
}
