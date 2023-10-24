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
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded.FlinkBoundedSource;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

public class BoundedReadSourceTranslator<T>
      implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

    private DataStream<WindowedValue<T>> getSource(
      RunnerApi.PTransform pTransform,
      TypeInformation<WindowedValue<T>> sdkTypeInformation,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

        RunnerApi.ReadPayload payload;
        try {
          payload = RunnerApi.ReadPayload.parseFrom(pTransform.getSpec().getPayload());
        } catch (IOException e) {
          throw new RuntimeException("Failed to parse ReadPayload from transform", e);
        }

        BoundedSource<T> rawSource;
        try {
          rawSource = (BoundedSource) ReadTranslation.boundedSourceFromProto(payload);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }

        String fullName = pTransform.getUniqueName();

        int parallelism =
            context.getExecutionEnvironment().getMaxParallelism() > 0
                ? context.getExecutionEnvironment().getMaxParallelism()
                : context.getExecutionEnvironment().getParallelism();

        FlinkBoundedSource<T> flinkBoundedSource =
          FlinkSource.bounded(
            pTransform.getUniqueName(),
            rawSource,
            new SerializablePipelineOptions(context.getPipelineOptions()),
            parallelism);

        try {
          return context
            .getExecutionEnvironment()
            .fromSource(flinkBoundedSource, WatermarkStrategy.noWatermarks(), fullName, sdkTypeInformation)
            .uid(fullName);
        } catch (Exception e) {
          throw new RuntimeException("Error while translating BoundedSource: " + rawSource, e);
        }
    }

    public DataStream<WindowedValue<T>> translateLegacy(
        PTransformNode transform,
        RunnerApi.Pipeline pipeline,
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

        RunnerApi.PTransform pTransform =
          transform.getTransform();

        String outputPCollectionId =
          Iterables.getOnlyElement(pTransform.getOutputsMap().values());

        TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(pipeline, outputPCollectionId);

        return getSource(transform.getTransform(), outputTypeInfo, context);
    }

    public DataStream<WindowedValue<T>> translatePortable(
        PTransformNode transform,
        RunnerApi.Pipeline pipeline,
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

        String outputPCollectionId =
          Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());

        PipelineOptions pipelineOptions = context.getPipelineOptions();

        WindowedValue.FullWindowedValueCoder<T> wireCoder =
        (WindowedValue.FullWindowedValueCoder)
          PipelineTranslatorUtils.instantiateCoder(outputPCollectionId, pipeline.getComponents());

        WindowedValue.FullWindowedValueCoder<T> sdkCoder =
          context.getSdkCoder(outputPCollectionId, pipeline.getComponents());

        CoderTypeInformation<WindowedValue<T>> outputTypeInfo =
            new CoderTypeInformation<>(wireCoder, pipelineOptions);

        CoderTypeInformation<WindowedValue<T>> sdkTypeInfo =
            new CoderTypeInformation<>(sdkCoder, pipelineOptions);

        return getSource(transform.getTransform(), sdkTypeInfo, context)
              .map(value -> ReadSourceTranslator.intoWireTypes(sdkCoder, wireCoder, value))
              .returns(outputTypeInfo);
    }

    @Override
    public void translate(
        PTransformNode transform,
        RunnerApi.Pipeline pipeline,
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

      String outputPCollectionId =
        Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());

      DataStream<WindowedValue<T>> source;
      if(context.isPortableRunnerExec()) {
        source = translatePortable(transform, pipeline, context);
      } else {
        source = translateLegacy(transform, pipeline, context);
      }

      context.addDataStream(outputPCollectionId, source);
    }
  }