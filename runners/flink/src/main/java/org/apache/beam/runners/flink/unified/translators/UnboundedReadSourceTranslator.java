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

import static java.lang.String.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.DedupingOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded.FlinkUnboundedSource;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class UnboundedReadSourceTranslator<T>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  static class ValueWithRecordIdKeySelector<T>
      implements KeySelector<WindowedValue<ValueWithRecordId<T>>, ByteBuffer>,
          ResultTypeQueryable<ByteBuffer> {

    @Override
    public ByteBuffer getKey(WindowedValue<ValueWithRecordId<T>> value) throws Exception {
      return ByteBuffer.wrap(value.getValue().getId());
    }

    @Override
    public TypeInformation<ByteBuffer> getProducedType() {
      return new GenericTypeInfo<>(ByteBuffer.class);
    }
  }

  public static class StripIdsMap<T>
      extends RichFlatMapFunction<WindowedValue<ValueWithRecordId<T>>, WindowedValue<T>> {

    private final SerializablePipelineOptions options;

    StripIdsMap(PipelineOptions options) {
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
        WindowedValue<ValueWithRecordId<T>> value, Collector<WindowedValue<T>> collector)
        throws Exception {
      collector.collect(value.withValue(value.getValue().getValue()));
    }
  }

  @Override
  public void translate(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {
    DataStream<WindowedValue<T>> source;

    if (context.isPortableRunnerExec()) {
      source = translatePortable(transform, pipeline, context);
    } else {
      source = translateLegacy(transform, pipeline, context);
    }

    String outputPCollectionId =
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());

    context.addDataStream(outputPCollectionId, source);
  }

  private DataStream<WindowedValue<T>> getDedupedSource(
      RunnerApi.PTransform pTransform,
      TypeInformation<WindowedValue<ValueWithRecordId<T>>> withIdTypeInfo,
      TypeInformation<WindowedValue<T>> sdkTypeInformation,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    DataStream<WindowedValue<T>> source;
    RunnerApi.ReadPayload payload;
    try {
      payload = RunnerApi.ReadPayload.parseFrom(pTransform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse ReadPayload from transform", e);
    }

    UnboundedSource<T, ?> rawSource =
        (UnboundedSource) ReadTranslation.unboundedSourceFromProto(payload);

    String fullName = pTransform.getUniqueName();

    int parallelism =
        context.getExecutionEnvironment().getMaxParallelism() > 0
            ? context.getExecutionEnvironment().getMaxParallelism()
            : context.getExecutionEnvironment().getParallelism();

    FlinkUnboundedSource<T> unboundedSource =
        FlinkSource.unbounded(
            pTransform.getUniqueName(),
            rawSource,
            new SerializablePipelineOptions(context.getPipelineOptions()),
            parallelism);

    DataStream<WindowedValue<ValueWithRecordId<T>>> nonDedupSource =
        context
            .getExecutionEnvironment()
            .fromSource(unboundedSource, WatermarkStrategy.noWatermarks(), fullName, withIdTypeInfo)
            .uid(fullName);

    if (rawSource.requiresDeduping()) {
      source =
          nonDedupSource
              .keyBy(new ValueWithRecordIdKeySelector<>())
              .transform(
                  "deduping",
                  sdkTypeInformation,
                  new DedupingOperator<>(context.getPipelineOptions()))
              .uid(format("%s/__deduplicated__", fullName));
    } else {
      source =
          nonDedupSource
              .flatMap(new StripIdsMap<>(context.getPipelineOptions()))
              .returns(sdkTypeInformation);
    }
    return source;
  }

  private DataStream<WindowedValue<T>> translatePortable(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    RunnerApi.PTransform pTransform = transform.getTransform();

    PipelineOptions pipelineOptions = context.getPipelineOptions();

    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    WindowingStrategy<?, ?> windowStrategy =
        context.getWindowingStrategy(pipeline, outputPCollectionId);

    @SuppressWarnings("unchecked")
    WindowedValue.FullWindowedValueCoder<T> wireCoder =
        (WindowedValue.FullWindowedValueCoder)
            PipelineTranslatorUtils.instantiateCoder(outputPCollectionId, pipeline.getComponents());

    WindowedValue.FullWindowedValueCoder<T> sdkCoder =
        context.getSdkCoder(outputPCollectionId, pipeline.getComponents());

    CoderTypeInformation<WindowedValue<T>> outputTypeInfo =
        new CoderTypeInformation<>(wireCoder, pipelineOptions);

    CoderTypeInformation<WindowedValue<T>> sdkTypeInformation =
        new CoderTypeInformation<>(sdkCoder, pipelineOptions);

    TypeInformation<WindowedValue<ValueWithRecordId<T>>> withIdTypeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(
                ValueWithRecordId.ValueWithRecordIdCoder.of(sdkCoder.getValueCoder()),
                windowStrategy.getWindowFn().windowCoder()),
            pipelineOptions);

    DataStream<WindowedValue<T>> source =
        getDedupedSource(pTransform, withIdTypeInfo, sdkTypeInformation, context);

    return source
        .map(value -> ReadSourceTranslator.intoWireTypes(sdkCoder, wireCoder, value))
        .returns(outputTypeInfo);
  }

  private DataStream<WindowedValue<T>> translateLegacy(
      PTransformNode transform,
      RunnerApi.Pipeline pipeline,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    RunnerApi.PTransform pTransform = transform.getTransform();

    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    TypeInformation<WindowedValue<T>> outputTypeInfo =
        context.getTypeInfo(pipeline, outputPCollectionId);

    WindowingStrategy<?, ?> windowingStrategy =
        context.getWindowingStrategy(pipeline, outputPCollectionId);

    WindowedValueCoder<T> windowedOutputCoder =
        context.getWindowedInputCoder(pipeline, outputPCollectionId);

    Coder<T> coder = windowedOutputCoder.getValueCoder();

    CoderTypeInformation<WindowedValue<ValueWithRecordId<T>>> withIdTypeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(
                ValueWithRecordId.ValueWithRecordIdCoder.of(coder),
                windowingStrategy.getWindowFn().windowCoder()),
            context.getPipelineOptions());

    return getDedupedSource(pTransform, withIdTypeInfo, outputTypeInfo, context);
  }
}
