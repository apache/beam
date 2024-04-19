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
package org.apache.beam.sdk.managed;

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedSchemaTransform;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;

import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ManagedSchemaTransformTranslation {
  static class ManagedSchemaTransformTranslator
      implements TransformPayloadTranslator<ManagedSchemaTransform> {
    static final ManagedSchemaTransformProvider PROVIDER = new ManagedSchemaTransformProvider(null);
    static final Schema SCHEMA = PROVIDER.configurationSchema();

    @Override
    public String getUrn() {
      return BeamUrns.getUrn(SCHEMA_TRANSFORM);
    }

    @Override
    @SuppressWarnings("argument")
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, ManagedSchemaTransform> application, SdkComponents components)
        throws IOException {
      // TODO(https://github.com/apache/beam/issues/31061): Remove conversion when
      // TypedSchemaTransformProvider starts generating with snake_case convention
      Schema snakeCaseSchema = SCHEMA.toSnakeCase();
      SchemaApi.Schema expansionSchema = SchemaTranslation.schemaToProto(snakeCaseSchema, true);
      Row configRow = toConfigRow(application.getTransform());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      RowCoder.of(snakeCaseSchema).encode(configRow, os);

      return FunctionSpec.newBuilder()
          .setUrn(getUrn())
          .setPayload(
              SchemaTransformPayload.newBuilder()
                  .setIdentifier(PROVIDER.identifier())
                  .setConfigurationSchema(expansionSchema)
                  .setConfigurationRow(ByteString.copyFrom(os.toByteArray()))
                  .build()
                  .toByteString())
          .build();
    }

    @Override
    public Row toConfigRow(ManagedSchemaTransform transform) {
      // Return with snake_case naming convention!
      // TODO(https://github.com/apache/beam/issues/31061): Remove conversion when
      // TypedSchemaTransformProvider starts generating with snake_case convention
      return transform.getConfigurationRow().toSnakeCase();
    }

    @Override
    public ManagedSchemaTransform fromConfigRow(Row configRow, PipelineOptions options) {
      // Will retrieve a Row with snake_case naming convention.
      // Transform expects camelCase convention, so convert back
      // TODO(https://github.com/apache/beam/issues/31061): Remove conversion when
      // TypedSchemaTransformProvider starts generating with snake_case convention
      return (ManagedSchemaTransform) PROVIDER.from(configRow.toCamelCase());
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ManagedTransformRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(ManagedSchemaTransform.class, new ManagedSchemaTransformTranslator())
          .build();
    }
  }
}
