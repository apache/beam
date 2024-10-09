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
package org.apache.beam.runners.flink.adapter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * An adapter class that allows one to apply Apache Beam PTransforms directly to Flink DataStreams.
 */
public class BeamFlinkDataStreamAdapter {
  private final PipelineOptions pipelineOptions;
  private final CoderRegistry coderRegistry = CoderRegistry.createDefault(null);

  public BeamFlinkDataStreamAdapter() {
    this(PipelineOptionsFactory.create());
  }

  public BeamFlinkDataStreamAdapter(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public <InputT, OutputT, CollectionT extends PCollection<? extends InputT>>
      DataStream<OutputT> applyBeamPTransform(
          DataStream<InputT> input, PTransform<CollectionT, PCollection<OutputT>> transform) {
    return (DataStream)
        getNonNull(
            applyBeamPTransformInternal(
                ImmutableMap.of("input", input),
                (pipeline, map) -> (CollectionT) getNonNull(map, "input"),
                (output) -> ImmutableMap.of("output", output),
                transform,
                input.getExecutionEnvironment()),
            "output");
  }

  public <OutputT> DataStream<OutputT> applyBeamPTransform(
      Map<String, ? extends DataStream<?>> inputs,
      PTransform<PCollectionTuple, PCollection<OutputT>> transform) {
    return (DataStream)
        getNonNull(
            applyBeamPTransformInternal(
                inputs,
                BeamAdapterUtils::mapToTuple,
                (output) -> ImmutableMap.of("output", output),
                transform,
                inputs.values().stream().findAny().get().getExecutionEnvironment()),
            "output");
  }

  public <OutputT> DataStream<OutputT> applyBeamPTransform(
      StreamExecutionEnvironment executionEnvironment,
      PTransform<PBegin, PCollection<OutputT>> transform) {
    return (DataStream)
        getNonNull(
            applyBeamPTransformInternal(
                ImmutableMap.of(),
                (pipeline, map) -> PBegin.in(pipeline),
                (output) -> ImmutableMap.of("output", output),
                transform,
                executionEnvironment),
            "output");
  }

  public <InputT, CollectionT extends PCollection<? extends InputT>>
      Map<String, DataStream<?>> applyMultiOutputBeamPTransform(
          DataStream<InputT> input, PTransform<CollectionT, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        ImmutableMap.of("input", input),
        (pipeline, map) -> (CollectionT) getNonNull(map, "input"),
        BeamAdapterUtils::tupleToMap,
        transform,
        input.getExecutionEnvironment());
  }

  public Map<String, DataStream<?>> applyMultiOutputBeamPTransform(
      Map<String, ? extends DataStream<?>> inputs,
      PTransform<PCollectionTuple, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        inputs,
        BeamAdapterUtils::mapToTuple,
        BeamAdapterUtils::tupleToMap,
        transform,
        inputs.values().stream().findAny().get().getExecutionEnvironment());
  }

  public Map<String, DataStream<?>> applyMultiOutputBeamPTransform(
      StreamExecutionEnvironment executionEnvironment,
      PTransform<PBegin, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        ImmutableMap.of(),
        (pipeline, map) -> PBegin.in(pipeline),
        BeamAdapterUtils::tupleToMap,
        transform,
        executionEnvironment);
  }

  public <InputT, CollectionT extends PCollection<? extends InputT>>
      void applyNoOutputBeamPTransform(
          DataStream<InputT> input, PTransform<CollectionT, PDone> transform) {
    applyBeamPTransformInternal(
        ImmutableMap.of("input", input),
        (pipeline, map) -> (CollectionT) getNonNull(map, "input"),
        pDone -> ImmutableMap.of(),
        transform,
        input.getExecutionEnvironment());
  }

  public void applyNoOutputBeamPTransform(
      Map<String, ? extends DataStream<?>> inputs, PTransform<PCollectionTuple, PDone> transform) {
    applyBeamPTransformInternal(
        inputs,
        BeamAdapterUtils::mapToTuple,
        pDone -> ImmutableMap.of(),
        transform,
        inputs.values().stream().findAny().get().getExecutionEnvironment());
  }

  public void applyNoOutputBeamPTransform(
      StreamExecutionEnvironment executionEnvironment, PTransform<PBegin, PDone> transform) {
    applyBeamPTransformInternal(
        ImmutableMap.of(),
        (pipeline, map) -> PBegin.in(pipeline),
        pDone -> ImmutableMap.of(),
        transform,
        executionEnvironment);
  }

  private <BeamInputT extends PInput, BeamOutputT extends POutput>
      Map<String, DataStream<?>> applyBeamPTransformInternal(
          Map<String, ? extends DataStream<?>> inputs,
          BiFunction<Pipeline, Map<String, PCollection<?>>, BeamInputT> toBeamInput,
          Function<BeamOutputT, Map<String, PCollection<?>>> fromBeamOutput,
          PTransform<? super BeamInputT, BeamOutputT> transform,
          StreamExecutionEnvironment executionEnvironment) {
    return BeamAdapterUtils.applyBeamPTransformInternal(
        inputs,
        toBeamInput,
        fromBeamOutput,
        transform,
        executionEnvironment,
        false,
        dataStream -> dataStream.getType(),
        pipelineOptions,
        coderRegistry,
        (flinkInputs, pipelineProto, env) -> {
          Map<String, DataStream<?>> flinkOutputs = new HashMap<>();
          FlinkStreamingPortablePipelineTranslator translator =
              new FlinkStreamingPortablePipelineTranslator(
                  ImmutableMap.of(
                      FlinkInput.URN, flinkInputTranslator(flinkInputs),
                      FlinkOutput.URN, flinkOutputTranslator(flinkOutputs)));
          FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context =
              translator.createTranslationContext(
                  JobInfo.create(
                      "unusedJobId",
                      "unusedJobName",
                      "unusedRetrievalToken",
                      PipelineOptionsTranslation.toProto(pipelineOptions)),
                  pipelineOptions.as(FlinkPipelineOptions.class),
                  env);
          translator.translate(context, translator.prepareForTranslation(pipelineProto));
          return flinkOutputs;
        });
  }

  private <InputT>
      FlinkStreamingPortablePipelineTranslator.PTransformTranslator<
              FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext>
          flinkInputTranslator(Map<String, ? extends DataStream<?>> inputMap) {
    return (String id,
        RunnerApi.Pipeline p,
        FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) -> {
      // When we run into a FlinkInput operator, it "produces" the corresponding input as its
      // "computed result."
      RunnerApi.PTransform transform = p.getComponents().getTransformsOrThrow(id);
      String inputId = transform.getSpec().getPayload().toStringUtf8();
      DataStream<InputT> flinkInput =
          Preconditions.checkStateNotNull(
              (DataStream<InputT>) inputMap.get(inputId),
              "missing input referenced in proto: " + inputId);
      context.addDataStream(
          Iterables.getOnlyElement(transform.getOutputsMap().values()),
          flinkInput.process(
              new ProcessFunction<InputT, WindowedValue<InputT>>() {
                @Override
                public void processElement(
                    InputT value,
                    ProcessFunction<InputT, WindowedValue<InputT>>.Context ctx,
                    Collector<WindowedValue<InputT>> out)
                    throws Exception {
                  out.collect(
                      WindowedValue.timestampedValueInGlobalWindow(
                          value,
                          ctx.timestamp() == null
                              ? BoundedWindow.TIMESTAMP_MIN_VALUE
                              : Instant.ofEpochMilli(ctx.timestamp())));
                }
              },
              BeamAdapterCoderUtils.coderToTypeInformation(
                  WindowedValue.getFullCoder(
                      BeamAdapterCoderUtils.typeInformationToCoder(
                          flinkInput.getType(), coderRegistry),
                      GlobalWindow.Coder.INSTANCE),
                  pipelineOptions)));
    };
  }

  private <InputT>
      FlinkStreamingPortablePipelineTranslator.PTransformTranslator<
              FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext>
          flinkOutputTranslator(Map<String, DataStream<?>> outputMap) {
    return (String id,
        RunnerApi.Pipeline p,
        FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) -> {
      // When we run into a FlinkOutput operator, we cache the computed PCollection to return to the
      // user.
      RunnerApi.PTransform transform = p.getComponents().getTransformsOrThrow(id);
      DataStream<WindowedValue<InputT>> inputDataStream =
          context.getDataStreamOrThrow(Iterables.getOnlyElement(transform.getInputsMap().values()));
      String outputId = transform.getSpec().getPayload().toStringUtf8();
      Coder<InputT> outputCoder =
          BeamAdapterCoderUtils.lookupCoder(
              p, Iterables.getOnlyElement(transform.getInputsMap().values()));
      outputMap.put(
          outputId,
          inputDataStream.transform(
              "StripWindows",
              BeamAdapterCoderUtils.coderToTypeInformation(outputCoder, pipelineOptions),
              new UnwrapWindowOperator<InputT>()));
    };
  }

  /**
   * Forwards the Beam timestamps to the underlying Flink timestamps, but unlike {@link
   * DataStream#assignTimestampsAndWatermarks(WatermarkStrategy)} does not discard the underlying
   * watermark signals.
   *
   * @param <T> user element type
   */
  private static class UnwrapWindowOperator<T> extends AbstractStreamOperator<T>
      implements OneInputStreamOperator<WindowedValue<T>, T> {
    @Override
    public void processElement(StreamRecord<WindowedValue<T>> element) {
      output.collect(
          element.replace(
              element.getValue().getValue(), element.getValue().getTimestamp().getMillis()));
    }
  }

  private <K, V> V getNonNull(Map<K, V> map, K key) {
    return Preconditions.checkStateNotNull(map.get(Preconditions.checkArgumentNotNull(key)));
  }
}
