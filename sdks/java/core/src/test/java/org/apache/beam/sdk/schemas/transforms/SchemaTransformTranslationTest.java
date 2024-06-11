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
package org.apache.beam.sdk.schemas.transforms;

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.sdk.schemas.transforms.SchemaTransformTranslation.SchemaTransformPayloadTranslator;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

/** Base class for standard {@link SchemaTransform} translation tests. */
public abstract class SchemaTransformTranslationTest {
  protected abstract SchemaTransformProvider provider();

  protected abstract Row configurationRow();

  /** Input used for this SchemaTransform. Used to build a pipeline to test proto translation. */
  protected PCollectionRowTuple input(Pipeline p) {
    return PCollectionRowTuple.empty(p);
  };

  @Test
  public void testRecreateTransformFromRow() {
    SchemaTransformProvider provider = provider();
    SchemaTransformPayloadTranslator translator = new SchemaTransformPayloadTranslator(provider);
    SchemaTransform originalTransform = provider.from(configurationRow());

    Row translatedConfigRow = translator.toConfigRow(originalTransform);
    SchemaTransform translatedTransform =
        translator.fromConfigRow(translatedConfigRow, PipelineOptionsFactory.create());

    assertEquals(configurationRow(), translatedTransform.getConfigurationRow());
  }

  @Test
  public void testTransformProtoTranslation() throws InvalidProtocolBufferException, IOException {
    SchemaTransformProvider provider = provider();
    Row configurationRow = configurationRow();

    // Infer if it's a read or write SchemaTransform and build pipeline accordingly
    Pipeline p = Pipeline.create();
    SchemaTransform schemaTransform = provider.from(configurationRow);
    input(p).apply(schemaTransform);

    // Then translate the pipeline to a proto and extract the SchemaTransform's proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> schemaTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr -> {
                  RunnerApi.FunctionSpec spec = tr.getSpec();
                  try {
                    return spec.getUrn().equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))
                        && ExternalTransforms.SchemaTransformPayload.parseFrom(spec.getPayload())
                            .getIdentifier()
                            .equals(provider.identifier());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    assertEquals(1, schemaTransformProto.size());
    RunnerApi.FunctionSpec spec = schemaTransformProto.get(0).getSpec();

    // Check that the proto contains correct values
    ExternalTransforms.SchemaTransformPayload payload =
        ExternalTransforms.SchemaTransformPayload.parseFrom(spec.getPayload());
    Schema translatedSchema = SchemaTranslation.schemaFromProto(payload.getConfigurationSchema());
    assertEquals(provider.configurationSchema(), translatedSchema);
    Row translatedConfigRow =
        RowCoder.of(translatedSchema).decode(payload.getConfigurationRow().newInput());

    assertEquals(configurationRow, translatedConfigRow);

    // Use the information in the proto to recreate the transform
    SchemaTransform translatedTransform =
        new SchemaTransformPayloadTranslator(provider)
            .fromConfigRow(translatedConfigRow, PipelineOptionsFactory.create());

    assertEquals(configurationRow, translatedTransform.getConfigurationRow());
  }
}
