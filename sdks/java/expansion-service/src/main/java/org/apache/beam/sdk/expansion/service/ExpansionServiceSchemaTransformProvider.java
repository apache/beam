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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.expansion.service.ExpansionService.TransformProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({"rawtypes"})
public class ExpansionServiceSchemaTransformProvider
    implements TransformProvider<PCollectionRowTuple, PCollectionRowTuple> {

  static final String DEFAULT_INPUT_TAG = "INPUT";

  private Map<String, org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider>
      schemaTransformProviders = new HashMap<>();
  private static @Nullable ExpansionServiceSchemaTransformProvider transformProvider = null;

  private ExpansionServiceSchemaTransformProvider() {
    try {
      for (org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider schemaTransformProvider :
          ServiceLoader.load(
              org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider.class)) {
        if (schemaTransformProviders.containsKey(schemaTransformProvider.identifier())) {
          throw new IllegalArgumentException(
              "Found multiple SchemaTransformProvider implementations with the same identifier "
                  + schemaTransformProvider.identifier());
        }
        schemaTransformProviders.put(schemaTransformProvider.identifier(), schemaTransformProvider);
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static ExpansionServiceSchemaTransformProvider of() {
    if (transformProvider == null) {
      transformProvider = new ExpansionServiceSchemaTransformProvider();
    }

    return transformProvider;
  }

  @Override
  public PCollectionRowTuple createInput(Pipeline p, Map<String, PCollection<?>> inputs) {
    if (inputs.size() == 0) {
      return PCollectionRowTuple.empty(p);
    }
    if (inputs.size() == 1) {
      return PCollectionRowTuple.of(
          DEFAULT_INPUT_TAG, (PCollection<Row>) inputs.values().iterator().next());
    } else {
      PCollectionRowTuple inputRowTuple = PCollectionRowTuple.empty(p);
      for (Map.Entry<String, PCollection<?>> entry : inputs.entrySet()) {
        inputRowTuple = inputRowTuple.and(entry.getKey(), (PCollection<Row>) entry.getValue());
      }
      return inputRowTuple;
    }
  }

  @Override
  public Map<String, PCollection<?>> extractOutputs(PCollectionRowTuple output) {
    if (output.getAll().size() == 0) {
      return Collections.emptyMap();
    } else if (output.getAll().size() == 1) {
      return ImmutableMap.of("output", output.getAll().values().iterator().next());
    } else {
      ImmutableMap.Builder<String, PCollection<?>> pCollectionMap = ImmutableMap.builder();
      for (String key : output.getAll().keySet()) {
        pCollectionMap.put(key, output.get(key));
      }
      return pCollectionMap.build();
    }
  }

  @Override
  public PTransform getTransform(FunctionSpec spec) {
    SchemaTransformPayload payload;
    try {
      payload = SchemaTransformPayload.parseFrom(spec.getPayload());
      String identifier = payload.getIdentifier();
      if (!schemaTransformProviders.containsKey(identifier)) {
        throw new RuntimeException(
            "Did not find a SchemaTransformProvider with the identifier " + identifier);
      }

    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Invalid payload type for URN " + getUrn(ExpansionMethods.Enum.SCHEMA_TRANSFORM), e);
    }

    String identifier = payload.getIdentifier();
    org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider provider =
        schemaTransformProviders.get(identifier);
    if (provider == null) {
      throw new IllegalArgumentException(
          "Could not find a SchemaTransform with identifier " + identifier);
    }

    Schema configSchemaFromRequest =
        SchemaTranslation.schemaFromProto((payload.getConfigurationSchema()));
    Schema configSchemaFromProvider = provider.configurationSchema();

    if (!configSchemaFromRequest.assignableTo(configSchemaFromProvider)) {
      throw new IllegalArgumentException(
          String.format(
              "Config schema provided with the expansion request %s is not compatible with the "
                  + "config of the Schema transform %s.",
              configSchemaFromRequest, configSchemaFromProvider));
    }

    Row configRow;
    try {
      configRow =
          RowCoder.of(provider.configurationSchema())
              .decode(payload.getConfigurationRow().newInput());
    } catch (IOException e) {
      throw new RuntimeException("Error decoding payload", e);
    }

    return provider.from(configRow).buildTransform();
  }

  Iterable<org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider> getAllProviders() {
    return schemaTransformProviders.values();
  }
}
