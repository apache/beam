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
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

public class GBKIntoKeyedWorkItemsTranslator<K, V>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  private static class ToKeyedWorkItemInGlobalWindow<K0, InputT>
      extends RichFlatMapFunction<
          WindowedValue<KV<K0, InputT>>, WindowedValue<KeyedWorkItem<K0, InputT>>> {

    private final SerializablePipelineOptions options;

    ToKeyedWorkItemInGlobalWindow(PipelineOptions options) {
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
        WindowedValue<KV<K0, InputT>> inWithMultipleWindows,
        Collector<WindowedValue<KeyedWorkItem<K0, InputT>>> out)
        throws Exception {

      // we need to wrap each one work item per window for now
      // since otherwise the PushbackSideInputRunner will not correctly
      // determine whether side inputs are ready
      //
      // this is tracked as https://github.com/apache/beam/issues/18358
      for (WindowedValue<KV<K0, InputT>> in : inWithMultipleWindows.explodeWindows()) {
        SingletonKeyedWorkItem<K0, InputT> workItem =
            new SingletonKeyedWorkItem<>(
                in.getValue().getKey(), in.withValue(in.getValue().getValue()));

        out.collect(WindowedValue.valueInGlobalWindow(workItem));
      }
    }
  }

  @Override
  public void translate(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    RunnerApi.PTransform pTransform = transform.getTransform();
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());

    WindowingStrategy<?, ?> windowingStrategy =
        context.getWindowingStrategy(pipeline, inputPCollectionId);

    WindowedValueCoder<KV<K, V>> windowedInputCoder =
        context.getWindowedInputCoder(pipeline, inputPCollectionId);

    KvCoder<K, V> inputKvCoder =
        (KvCoder<K, V>) windowedInputCoder.getValueCoder();

    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    SingletonKeyedWorkItemCoder<K, V> workItemCoder =
        SingletonKeyedWorkItemCoder.of(
            inputKvCoder.getKeyCoder(),
            inputKvCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.ValueOnlyWindowedValueCoder<KeyedWorkItem<K, V>> windowedWorkItemCoder =
        WindowedValue.getValueOnlyCoder(workItemCoder);

    CoderTypeInformation<WindowedValue<KeyedWorkItem<K, V>>> workItemTypeInfo =
        new CoderTypeInformation<>(windowedWorkItemCoder, context.getPipelineOptions());

    DataStream<WindowedValue<KeyedWorkItem<K, V>>> workItemStream =
        inputDataStream
            .flatMap(new ToKeyedWorkItemInGlobalWindow<>(context.getPipelineOptions()))
            .returns(workItemTypeInfo)
            .name("ToKeyedWorkItem");

    KeyedStream<WindowedValue<KeyedWorkItem<K, V>>, ByteBuffer> keyedWorkItemStream =
        workItemStream.keyBy(
            new WorkItemKeySelector<>(
                inputKvCoder.getKeyCoder(),
                new SerializablePipelineOptions(context.getPipelineOptions())));

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()), keyedWorkItemStream);
  }
}
