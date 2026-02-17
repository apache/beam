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
package org.apache.beam.sdk.expansion.service;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.resources.PipelineResources;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * Provides a mapping of {@link RunnerApi.FunctionSpec} to a {@link PTransform}, together with
 * mappings of its inputs and outputs to maps of PCollections.
 *
 * @param <InputT> input {@link PInput} type of the transform
 * @param <OutputT> output {@link POutput} type of the transform
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public interface TransformProvider<InputT extends PInput, OutputT extends POutput> {

  PTransform<InputT, OutputT> getTransform(RunnerApi.FunctionSpec spec, PipelineOptions options);

  default InputT createInput(Pipeline p, Map<String, PCollection<?>> inputs) {
    inputs =
        checkArgumentNotNull(inputs); // spotbugs claims incorrectly that it is annotated @Nullable
    if (inputs.size() == 0) {
      return (InputT) p.begin();
    }
    if (inputs.size() == 1) {
      return (InputT) Iterables.getOnlyElement(inputs.values());
    } else {
      PCollectionTuple inputTuple = PCollectionTuple.empty(p);
      for (Map.Entry<String, PCollection<?>> entry : inputs.entrySet()) {
        inputTuple = inputTuple.and(new TupleTag<>(entry.getKey()), entry.getValue());
      }
      return (InputT) inputTuple;
    }
  }

  default Map<String, PCollection<?>> extractOutputs(OutputT output) {
    if (output instanceof PDone) {
      return Collections.emptyMap();
    } else if (output instanceof PCollection) {
      return ImmutableMap.of("output", (PCollection<?>) output);
    } else if (output instanceof PCollectionTuple) {
      return ((PCollectionTuple) output)
          .getAll().entrySet().stream()
              .collect(Collectors.toMap(entry -> entry.getKey().getId(), Map.Entry::getValue));
    } else if (output instanceof PCollectionList<?>) {
      PCollectionList<?> listOutput = (PCollectionList<?>) output;
      ImmutableMap.Builder<String, PCollection<?>> indexToPCollection = ImmutableMap.builder();
      int i = 0;
      for (PCollection<?> pc : listOutput.getAll()) {
        indexToPCollection.put(Integer.toString(i), pc);
        i++;
      }
      return indexToPCollection.build();
    } else if (output instanceof POutput) {
      // This is needed to support custom output types.
      Map<TupleTag<?>, PValue> values = output.expand();
      Map<String, PCollection<?>> returnMap = new HashMap<>();
      for (Map.Entry<TupleTag<?>, PValue> entry : values.entrySet()) {
        if (!(entry.getValue() instanceof PCollection)) {
          throw new UnsupportedOperationException(
              "Unable to parse the output type "
                  + output.getClass()
                  + " due to key "
                  + entry.getKey()
                  + " not mapping to a PCollection");
        }
        returnMap.put(entry.getKey().getId(), (PCollection<?>) entry.getValue());
      }
      return returnMap;
    } else {
      throw new UnsupportedOperationException("Unknown output type: " + output.getClass());
    }
  }

  default Map<String, PCollection<?>> apply(
      Pipeline p, String name, RunnerApi.FunctionSpec spec, Map<String, PCollection<?>> inputs) {
    return extractOutputs(
        Pipeline.applyTransform(name, createInput(p, inputs), getTransform(spec, p.getOptions())));
  }

  default String getTransformUniqueID(RunnerApi.FunctionSpec spec) {
    if (BeamUrns.getUrn(ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM)
        .equals(spec.getUrn())) {
      ExternalTransforms.SchemaTransformPayload payload;
      try {
        payload = ExternalTransforms.SchemaTransformPayload.parseFrom(spec.getPayload());
        if (PTransformTranslation.MANAGED_TRANSFORM_URN.equals(payload.getIdentifier())) {
          try {
            // ManagedSchemaTransform includes a schema field transform_identifier that includes the
            // underlying schema
            // transform ID so we special case that here.
            Row configRow =
                RowCoder.of(SchemaTranslation.schemaFromProto(payload.getConfigurationSchema()))
                    .decode(new ByteArrayInputStream(payload.getConfigurationRow().toByteArray()));

            for (String field : configRow.getSchema().getFieldNames()) {
              if (field.equals("transform_identifier")) {
                return configRow.getValue(field);
              }
            }
            throw new RuntimeException(
                "Expected the ManagedTransform schema to include a field named "
                    + "'transform_identifier' but received "
                    + configRow);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          return payload.getIdentifier();
        }
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(
            "Invalid payload type for URN "
                + BeamUrns.getUrn(ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM),
            e);
      }
    }
    return spec.getUrn();
  }

  default List<String> getDependencies(RunnerApi.FunctionSpec spec, PipelineOptions options) {
    ExpansionServiceConfig config =
        options.as(ExpansionServiceOptions.class).getExpansionServiceConfig();
    String transformUniqueID = getTransformUniqueID(spec);

    boolean isManagedExpansion = false;
    if (getUrn(ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM).equals(spec.getUrn())) {
      try {
        ExternalTransforms.SchemaTransformPayload schemaTransformPayload =
            ExternalTransforms.SchemaTransformPayload.parseFrom(spec.getPayload());
        isManagedExpansion =
            PTransformTranslation.MANAGED_TRANSFORM_URN.equals(
                schemaTransformPayload.getIdentifier());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }

    // Providing specific dependencies for expansion if possible.
    // For managed transforms expansion, we only do this if useExpansionServiceConfigForDependencies
    // option
    // is specified.
    if (transformUniqueID != null
        && config.getDependencies().containsKey(transformUniqueID)
        && (!isManagedExpansion
            || options.as(ExpansionServiceOptions.class).getUseConfigDependenciesForManaged())) {
      List<String> updatedDependencies =
          config.getDependencies().get(transformUniqueID).stream()
              .map(dependency -> dependency.getPath())
              .collect(Collectors.toList());
      return updatedDependencies;
    }

    List<String> filesToStage = options.as(PortablePipelineOptions.class).getFilesToStage();

    if (filesToStage == null || filesToStage.isEmpty()) {
      ClassLoader classLoader = Environments.class.getClassLoader();
      if (classLoader == null) {
        throw new RuntimeException(
            "Cannot detect classpath: classloader is null (is it the bootstrap classloader?)");
      }
      filesToStage = PipelineResources.detectClassPathResourcesToStage(classLoader, options);
      if (filesToStage.isEmpty()) {
        throw new IllegalArgumentException("No classpath elements found.");
      }
    }
    return filesToStage;
  }
}
