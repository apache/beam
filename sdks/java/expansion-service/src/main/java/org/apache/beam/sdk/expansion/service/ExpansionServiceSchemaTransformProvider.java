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
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.expansion.service.ExpansionService.TransformProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings({"rawtypes"})
public class ExpansionServiceSchemaTransformProvider implements TransformProvider {

  static final String DEFAULT_INPUT_TAG = "INPUT";

  private Map<String, org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider>
      schemaTransformProviders = new HashMap<>();
  private static ExpansionServiceSchemaTransformProvider transformProvider = null;

  private void loadSchemaTransforms() {
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

  private ExpansionServiceSchemaTransformProvider() {
    loadSchemaTransforms();
  }

  public static ExpansionServiceSchemaTransformProvider of() {
    if (transformProvider == null) {
      transformProvider = new ExpansionServiceSchemaTransformProvider();
    }

    return transformProvider;
  }

  static class RowTransform extends PTransform {

    private PTransform<PCollectionRowTuple, PCollectionRowTuple> rowTuplePTransform;

    public RowTransform(PTransform<PCollectionRowTuple, PCollectionRowTuple> rowTuplePTransform) {
      this.rowTuplePTransform = rowTuplePTransform;
    }

    @Override
    public POutput expand(PInput input) {
      PCollectionRowTuple inputRowTuple;

      if (input instanceof PCollectionRowTuple) {
        inputRowTuple = (PCollectionRowTuple) input;
      } else if (input instanceof PCollection) {
        inputRowTuple = PCollectionRowTuple.of(DEFAULT_INPUT_TAG, (PCollection) input);
      } else if (input instanceof PBegin) {
        inputRowTuple = PCollectionRowTuple.empty(input.getPipeline());
      } else if (input instanceof PCollectionTuple) {
        inputRowTuple = PCollectionRowTuple.empty(input.getPipeline());
        PCollectionTuple inputTuple = (PCollectionTuple) input;
        for (TupleTag<?> tag : inputTuple.getAll().keySet()) {
          inputRowTuple = inputRowTuple.and(tag.getId(), (PCollection<Row>) inputTuple.get(tag));
        }
      } else {
        throw new RuntimeException(String.format("Unsupported input type: %s", input));
      }
      PCollectionRowTuple output = inputRowTuple.apply(this.rowTuplePTransform);

      if (output.getAll().size() > 1) {
        PCollectionTuple pcTuple = PCollectionTuple.empty(input.getPipeline());
        for (String key : output.getAll().keySet()) {
          pcTuple = pcTuple.and(key, output.get(key));
        }
        return pcTuple;
      } else if (output.getAll().size() == 1) {
        return output.getAll().values().iterator().next();
      } else {
        return PDone.in(input.getPipeline());
      }
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
          "Invalid payload type for URN " + getUrn(ExpansionMethods.Enum.SCHEMATRANSFORM), e);
    }

    String identifier = payload.getIdentifier();
    org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider provider =
        schemaTransformProviders.get(identifier);

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

    return new RowTransform(provider.from(configRow).buildTransform());
  }

  Iterable<org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider> getAllProviders() {
    return schemaTransformProviders.values();
  }
}
