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
import org.apache.beam.runners.flink.FlinkBatchPortablePipelineTranslator;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

/** An adapter class that allows one to apply Apache Beam PTransforms directly to Flink DataSets. */
public class BeamFlinkDataSetAdapter {
  private final PipelineOptions pipelineOptions;
  private final CoderRegistry coderRegistry = CoderRegistry.createDefault();

  public BeamFlinkDataSetAdapter() {
    this(PipelineOptionsFactory.create());
  }

  public BeamFlinkDataSetAdapter(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public <InputT, OutputT, CollectionT extends PCollection<? extends InputT>>
      DataSet<OutputT> applyBeamPTransform(
          DataSet<InputT> input, PTransform<CollectionT, PCollection<OutputT>> transform) {
    return (DataSet)
        getNonNull(
            applyBeamPTransformInternal(
                ImmutableMap.of("input", input),
                (pipeline, map) -> (CollectionT) getNonNull(map, "input"),
                (output) -> ImmutableMap.of("output", output),
                transform,
                input.getExecutionEnvironment()),
            "output");
  }

  public <OutputT> DataSet<OutputT> applyBeamPTransform(
      Map<String, ? extends DataSet<?>> inputs,
      PTransform<PCollectionTuple, PCollection<OutputT>> transform) {
    return (DataSet)
        getNonNull(
            applyBeamPTransformInternal(
                inputs,
                BeamAdapterUtils::mapToTuple,
                (output) -> ImmutableMap.of("output", output),
                transform,
                inputs.values().stream().findAny().get().getExecutionEnvironment()),
            "output");
  }

  public <OutputT> DataSet<OutputT> applyBeamPTransform(
      ExecutionEnvironment executionEnvironment,
      PTransform<PBegin, PCollection<OutputT>> transform) {
    return (DataSet)
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
      Map<String, DataSet<?>> applyMultiOutputBeamPTransform(
          DataSet<InputT> input, PTransform<CollectionT, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        ImmutableMap.of("input", input),
        (pipeline, map) -> (CollectionT) getNonNull(map, "input"),
        BeamAdapterUtils::tupleToMap,
        transform,
        input.getExecutionEnvironment());
  }

  public Map<String, DataSet<?>> applyMultiOutputBeamPTransform(
      Map<String, ? extends DataSet<?>> inputs,
      PTransform<PCollectionTuple, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        inputs,
        BeamAdapterUtils::mapToTuple,
        BeamAdapterUtils::tupleToMap,
        transform,
        inputs.values().stream().findAny().get().getExecutionEnvironment());
  }

  public Map<String, DataSet<?>> applyMultiOutputBeamPTransform(
      ExecutionEnvironment executionEnvironment, PTransform<PBegin, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        ImmutableMap.of(),
        (pipeline, map) -> PBegin.in(pipeline),
        BeamAdapterUtils::tupleToMap,
        transform,
        executionEnvironment);
  }

  public <InputT, CollectionT extends PCollection<? extends InputT>>
      void applyNoOutputBeamPTransform(
          DataSet<InputT> input, PTransform<CollectionT, PDone> transform) {
    applyBeamPTransformInternal(
        ImmutableMap.of("input", input),
        (pipeline, map) -> (CollectionT) getNonNull(map, "input"),
        pDone -> ImmutableMap.of(),
        transform,
        input.getExecutionEnvironment());
  }

  public void applyNoOutputBeamPTransform(
      Map<String, ? extends DataSet<?>> inputs, PTransform<PCollectionTuple, PDone> transform) {
    applyBeamPTransformInternal(
        inputs,
        BeamAdapterUtils::mapToTuple,
        pDone -> ImmutableMap.of(),
        transform,
        inputs.values().stream().findAny().get().getExecutionEnvironment());
  }

  public void applyNoOutputBeamPTransform(
      ExecutionEnvironment executionEnvironment, PTransform<PBegin, PDone> transform) {
    applyBeamPTransformInternal(
        ImmutableMap.of(),
        (pipeline, map) -> PBegin.in(pipeline),
        pDone -> ImmutableMap.of(),
        transform,
        executionEnvironment);
  }

  private <BeamInputT extends PInput, BeamOutputT extends POutput>
      Map<String, DataSet<?>> applyBeamPTransformInternal(
          Map<String, ? extends DataSet<?>> inputs,
          BiFunction<Pipeline, Map<String, PCollection<?>>, BeamInputT> toBeamInput,
          Function<BeamOutputT, Map<String, PCollection<?>>> fromBeamOutput,
          PTransform<? super BeamInputT, BeamOutputT> transform,
          ExecutionEnvironment executionEnvironment) {
    return BeamAdapterUtils.applyBeamPTransformInternal(
        inputs,
        toBeamInput,
        fromBeamOutput,
        transform,
        executionEnvironment,
        true,
        dataSet -> dataSet.getType(),
        pipelineOptions,
        coderRegistry,
        (flinkInputs, pipelineProto, env) -> {
          Map<String, DataSet<?>> flinkOutputs = new HashMap<>();
          FlinkBatchPortablePipelineTranslator translator =
              FlinkBatchPortablePipelineTranslator.createTranslator(
                  ImmutableMap.of(
                      FlinkInput.URN, flinkInputTranslator(flinkInputs),
                      FlinkOutput.URN, flinkOutputTranslator(flinkOutputs)));
          FlinkBatchPortablePipelineTranslator.BatchTranslationContext context =
              FlinkBatchPortablePipelineTranslator.createTranslationContext(
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

  private <InputT> FlinkBatchPortablePipelineTranslator.PTransformTranslator flinkInputTranslator(
      Map<String, ? extends DataSet<?>> inputMap) {
    return (PipelineNode.PTransformNode t,
        RunnerApi.Pipeline p,
        FlinkBatchPortablePipelineTranslator.BatchTranslationContext context) -> {
      // When we run into a FlinkInput operator, it "produces" the corresponding input as its
      // "computed result."
      String inputId = t.getTransform().getSpec().getPayload().toStringUtf8();
      DataSet<InputT> flinkInput =
          org.apache.beam.sdk.util.Preconditions.checkStateNotNull(
              (DataSet<InputT>) inputMap.get(inputId),
              "missing input referenced in proto: ",
              inputId);
      context.addDataSet(
          Iterables.getOnlyElement(t.getTransform().getOutputsMap().values()),
          // new MapOperator(...) rather than .map to manually designate the type information.
          // Note that MapOperator is a subclass of DataSet.
          new MapOperator<InputT, WindowedValue<InputT>>(
              flinkInput,
              BeamAdapterCoderUtils.coderToTypeInformation(
                  WindowedValue.getValueOnlyCoder(
                      BeamAdapterCoderUtils.typeInformationToCoder(
                          flinkInput.getType(), coderRegistry)),
                  pipelineOptions),
              x -> WindowedValue.valueInGlobalWindow(x),
              "AddGlobalWindows"));
    };
  }

  private <InputT> FlinkBatchPortablePipelineTranslator.PTransformTranslator flinkOutputTranslator(
      Map<String, DataSet<?>> outputMap) {
    return (PipelineNode.PTransformNode t,
        RunnerApi.Pipeline p,
        FlinkBatchPortablePipelineTranslator.BatchTranslationContext context) -> {
      DataSet<WindowedValue<InputT>> inputDataSet =
          context.getDataSetOrThrow(
              Iterables.getOnlyElement(t.getTransform().getInputsMap().values()));
      // When we run into a FlinkOutput operator, we cache the computed PCollection to return to the
      // user.
      String outputId = t.getTransform().getSpec().getPayload().toStringUtf8();
      Coder<InputT> outputCoder =
          BeamAdapterCoderUtils.lookupCoder(
              p, Iterables.getOnlyElement(t.getTransform().getInputsMap().values()));
      outputMap.put(
          outputId,
          new MapOperator<WindowedValue<InputT>, InputT>(
              inputDataSet,
              BeamAdapterCoderUtils.coderToTypeInformation(outputCoder, pipelineOptions),
              w -> w.getValue(),
              "StripWindows"));
    };
  }

  private <K, V> V getNonNull(Map<K, V> map, K key) {
    return Preconditions.checkStateNotNull(map.get(Preconditions.checkArgumentNotNull(key)));
  }
}
