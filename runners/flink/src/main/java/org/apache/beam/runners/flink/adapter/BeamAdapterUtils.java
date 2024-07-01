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

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;

class BeamAdapterUtils {
  private BeamAdapterUtils() {}

  interface PipelineFragmentTranslator<DataSetOrStreamT, ExecutionEnvironmentT> {
    Map<String, DataSetOrStreamT> translate(
        Map<String, ? extends DataSetOrStreamT> inputs,
        RunnerApi.Pipeline pipelineProto,
        ExecutionEnvironmentT executionEnvironment);
  }

  @SuppressWarnings({"rawtypes"})
  static <
          DataSetOrStreamT,
          ExecutionEnvironmentT,
          BeamInputT extends PInput,
          BeamOutputT extends POutput>
      Map<String, DataSetOrStreamT> applyBeamPTransformInternal(
          Map<String, ? extends DataSetOrStreamT> inputs,
          BiFunction<Pipeline, Map<String, PCollection<?>>, BeamInputT> toBeamInput,
          Function<BeamOutputT, Map<String, PCollection<?>>> fromBeamOutput,
          PTransform<? super BeamInputT, BeamOutputT> transform,
          ExecutionEnvironmentT executionEnvironment,
          boolean isBounded,
          Function<DataSetOrStreamT, TypeInformation<?>> getTypeInformation,
          PipelineOptions pipelineOptions,
          CoderRegistry coderRegistry,
          PipelineFragmentTranslator<DataSetOrStreamT, ExecutionEnvironmentT> translator) {
    Pipeline pipeline = Pipeline.create();

    // Construct beam inputs corresponding to each Flink input.
    Map<String, PCollection<?>> beamInputs =
        // Copy as transformEntries lazy recomputes entries.
        ImmutableMap.copyOf(
            Maps.transformEntries(
                inputs,
                (key, flinkInput) ->
                    pipeline.apply(
                        new FlinkInput<>(
                            key,
                            BeamAdapterCoderUtils.typeInformationToCoder(
                                getTypeInformation.apply(
                                    Preconditions.checkArgumentNotNull(flinkInput)),
                                coderRegistry),
                            isBounded))));

    // Actually apply the transform to create Beam outputs.
    Map<String, PCollection<?>> beamOutputs =
        fromBeamOutput.apply(applyTransform(toBeamInput.apply(pipeline, beamInputs), transform));

    // This attaches PTransforms to each output which will be used to populate the Flink outputs
    // during translation.
    beamOutputs.entrySet().stream()
        .forEach(
            e -> {
              ((PCollection<Object>) e.getValue()).apply(new FlinkOutput<Object>(e.getKey()));
            });

    // This "environment" executes the SDK harness in the parent worker process.
    // TODO(robertwb): Support other modes.
    // TODO(robertwb): In embedded mode, consider an optimized data (and state) channel rather than
    // serializing everything over grpc protos.
    pipelineOptions
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);

    // Extract the pipeline definition so that we can apply or Flink translation logic.
    SdkComponents components = SdkComponents.create(pipelineOptions);
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, components);

    // Avoid swapping input and output coders for BytesCoders.
    // As we have instantiated the actual coder objects here, there is no need ot length prefix them
    // anyway.
    // TODO(robertwb): Even better would be to avoid coding and decoding along these edges via a
    // direct
    // in-memory channel for embedded mode.  As well as improving performance, there could be
    // control-flow advantages too.
    for (RunnerApi.PTransform transformProto :
        pipelineProto.getComponents().getTransforms().values()) {
      if (FlinkInput.URN.equals(PTransformTranslation.urnForTransformOrNull(transformProto))) {
        BeamAdapterCoderUtils.registerKnownCoderFor(
            pipelineProto, Iterables.getOnlyElement(transformProto.getOutputs().values()));
      } else if (FlinkOutput.URN.equals(
          PTransformTranslation.urnForTransformOrNull(transformProto))) {
        BeamAdapterCoderUtils.registerKnownCoderFor(
            pipelineProto, Iterables.getOnlyElement(transformProto.getInputs().values()));
      }
    }

    return translator.translate(inputs, pipelineProto, executionEnvironment);
  }

  static Map<String, PCollection<?>> tupleToMap(PCollectionTuple tuple) {
    return tuple.getAll().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getId(), e -> e.getValue()));
  }

  static PCollectionTuple mapToTuple(Pipeline p, Map<String, PCollection<?>> map) {
    PCollectionTuple tuple = PCollectionTuple.empty(p);
    for (Map.Entry<String, PCollection<?>> entry : map.entrySet()) {
      tuple = tuple.and(entry.getKey(), entry.getValue());
    }
    return tuple;
  }

  /**
   * This is required as there is no apply() method on the base PInput type due to the inability to
   * declare type parameters as self types.
   */
  private static <BeamInputT extends PInput, BeamOutputT extends POutput>
      BeamOutputT applyTransform(
          BeamInputT beamInput, PTransform<? super BeamInputT, BeamOutputT> transform) {
    if (beamInput instanceof PCollection) {
      return (BeamOutputT) ((PCollection) beamInput).apply(transform);
    } else if (beamInput instanceof PCollectionTuple) {
      return (BeamOutputT) ((PCollectionTuple) beamInput).apply((PTransform) transform);
    } else if (beamInput instanceof PBegin) {
      return (BeamOutputT) ((PBegin) beamInput).apply((PTransform) transform);
    } else {
      // We should never get here as we control the creation of all Beam types above.
      // If new types of transform inputs are supported, this enumeration may need to be updated.
      throw new IllegalArgumentException(
          "Unknown Beam input type " + beamInput.getClass().getTypeName() + " for " + beamInput);
    }
  }
}
