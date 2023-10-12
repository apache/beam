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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
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
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

public class GroupByKeyTranslator<K, V>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  /**
   * Convert the values of a windowed KV to byte[] to avoid unecesary serde cycle in intermediate
   * PCollections.
   */
  static class ToBinaryKeyedWorkItem<K, InputT>
      extends RichFlatMapFunction<
          WindowedValue<KV<K, InputT>>, WindowedValue<KeyedWorkItem<K, byte[]>>> {

    private final SerializablePipelineOptions options;
    private final Coder<InputT> valueCoder;

    ToBinaryKeyedWorkItem(PipelineOptions options, Coder<InputT> valueCoder) {
      this.options = new SerializablePipelineOptions(options);
      this.valueCoder = valueCoder;
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
        Collector<WindowedValue<KeyedWorkItem<K, byte[]>>> out)
        throws CoderException {

      // we need to wrap each one work item per window for now
      // since otherwise the PushbackSideInputRunner will not correctly
      // determine whether side inputs are ready
      //
      // this is tracked as https://github.com/apache/beam/issues/18358
      for (WindowedValue<KV<K, InputT>> in : inWithMultipleWindows.explodeWindows()) {
        final byte[] binaryValue =
            CoderUtils.encodeToByteArray(valueCoder, in.getValue().getValue());
        final SingletonKeyedWorkItem<K, byte[]> workItem =
            new SingletonKeyedWorkItem<>(in.getValue().getKey(), in.withValue(binaryValue));
        out.collect(in.withValue(workItem));
      }
    }
  }

  /**
   * Convert back the values of a windowed KV to their original type after ToBinaryKeyedWorkItem was
   * applied.
   */
  static class ToGroupByKeyResult<KeyT, ValueT>
      extends RichFlatMapFunction<
          WindowedValue<KV<KeyT, Iterable<byte[]>>>, WindowedValue<KV<KeyT, Iterable<ValueT>>>> {

    private final SerializablePipelineOptions options;
    private final Coder<ValueT> valueCoder;

    ToGroupByKeyResult(PipelineOptions options, Coder<ValueT> valueCoder) {
      this.options = new SerializablePipelineOptions(options);
      this.valueCoder = valueCoder;
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public void flatMap(
        WindowedValue<KV<KeyT, Iterable<byte[]>>> element,
        Collector<WindowedValue<KV<KeyT, Iterable<ValueT>>>> collector)
        throws CoderException {
      final List<ValueT> result = new ArrayList<>();
      for (byte[] binaryValue : element.getValue().getValue()) {
        result.add(CoderUtils.decodeFromByteArray(valueCoder, binaryValue));
      }
      collector.collect(element.withValue(KV.of(element.getValue().getKey(), result)));
    }
  }

  public static <K, V> SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> addGBK(
      DataStream<WindowedValue<KV<K, V>>> inputDataStream,
      WindowingStrategy<?, ?> windowingStrategy,
      WindowedValueCoder<KV<K, V>> windowedInputCoder,
      String fullName,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) windowedInputCoder.getValueCoder();

    SingletonKeyedWorkItemCoder<K, byte[]> workItemCoder =
        SingletonKeyedWorkItemCoder.<K, byte[]>of(
            inputKvCoder.getKeyCoder(),
            ByteArrayCoder.of(),
            windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<KeyedWorkItem<K, byte[]>> windowedWorkItemCoder =
        WindowedValue.getFullCoder(
            workItemCoder, windowingStrategy.getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<KeyedWorkItem<K, byte[]>>> workItemTypeInfo =
        new CoderTypeInformation<>(windowedWorkItemCoder, context.getPipelineOptions());

    DataStream<WindowedValue<KeyedWorkItem<K, byte[]>>> workItemStream =
        inputDataStream
            .flatMap(
                new ToBinaryKeyedWorkItem<>(
                    context.getPipelineOptions(), inputKvCoder.getValueCoder()))
            .returns(workItemTypeInfo)
            .name("ToBinaryKeyedWorkItem");

    WorkItemKeySelector<K, byte[]> keySelector =
        new WorkItemKeySelector<>(
            inputKvCoder.getKeyCoder(),
            new SerializablePipelineOptions(context.getPipelineOptions()));

    KeyedStream<WindowedValue<KeyedWorkItem<K, byte[]>>, ByteBuffer> keyedWorkItemStream =
        workItemStream.keyBy(keySelector);

    SystemReduceFn<K, byte[], Iterable<byte[]>, Iterable<byte[]>, BoundedWindow> reduceFn =
        SystemReduceFn.buffering(ByteArrayCoder.of());

    // Use Byte based GBK
    Coder<WindowedValue<KV<K, Iterable<byte[]>>>> outputCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(inputKvCoder.getKeyCoder(), IterableCoder.of(ByteArrayCoder.of())),
            windowingStrategy.getWindowFn().windowCoder());

    TypeInformation<WindowedValue<KV<K, Iterable<byte[]>>>> outputTypeInfo =
        new CoderTypeInformation<>(outputCoder, context.getPipelineOptions());

    TupleTag<KV<K, Iterable<byte[]>>> mainTag = new TupleTag<>("main output");

    WindowDoFnOperator<K, byte[], Iterable<byte[]>> doFnOperator =
        new WindowDoFnOperator<>(
            reduceFn,
            fullName,
            windowedWorkItemCoder,
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

    Coder<Iterable<V>> accumulatorCoder = IterableCoder.of(inputKvCoder.getValueCoder());

    Coder<WindowedValue<KV<K, Iterable<V>>>> resultCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(inputKvCoder.getKeyCoder(), accumulatorCoder),
            windowingStrategy.getWindowFn().windowCoder());

    TypeInformation<WindowedValue<KV<K, Iterable<V>>>> resultTypeInfo =
        new CoderTypeInformation<>(resultCoder, context.getPipelineOptions());

    return keyedWorkItemStream
        .transform(fullName, outputTypeInfo, doFnOperator)
        .uid(fullName)
        .flatMap(
            new ToGroupByKeyResult<>(context.getPipelineOptions(), inputKvCoder.getValueCoder()))
        .returns(resultTypeInfo)
        .name("ToGBKResult");
  }

  @Override
  public void translate(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    RunnerApi.PTransform pTransform = transform.getTransform();
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());

    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    WindowingStrategy<?, ?> windowingStrategy =
        context.getWindowingStrategy(pipeline, inputPCollectionId);

    WindowedValueCoder<KV<K, V>> windowedInputCoder =
        context.getWindowedInputCoder(pipeline, inputPCollectionId);

    String fullName = pTransform.getUniqueName();

    final SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> outDataStream =
        GroupByKeyTranslator.addGBK(
            inputDataStream, windowingStrategy, windowedInputCoder, fullName, context);

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()), outDataStream);
  }
}
