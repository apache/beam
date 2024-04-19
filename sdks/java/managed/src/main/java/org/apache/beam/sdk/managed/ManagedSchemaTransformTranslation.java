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

import static org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedConfig;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedSchemaTransform;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;

import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaAwareTransforms.ManagedSchemaTransformPayload;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
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
      return PROVIDER.identifier();
    }

    @Override
    @SuppressWarnings("argument")
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, ManagedSchemaTransform> application, SdkComponents components)
        throws IOException {
      SchemaApi.Schema expansionSchema = SchemaTranslation.schemaToProto(SCHEMA, true);
      ManagedConfig managedConfig = application.getTransform().getManagedConfig();
      Row configRow = toConfigRow(application.getTransform());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      RowCoder.of(SCHEMA).encode(configRow, os);

      return FunctionSpec.newBuilder()
          .setUrn(getUrn())
          .setPayload(
              ManagedSchemaTransformPayload.newBuilder()
                  .setUnderlyingTransformIdentifier(managedConfig.getTransformIdentifier())
                  .setYamlConfig(managedConfig.resolveUnderlyingConfig())
                  .setExpansionSchema(expansionSchema)
                  .setExpansionPayload(ByteString.copyFrom(os.toByteArray()))
                  .build()
                  .toByteString())
          .build();
    }

    @Override
    public Row toConfigRow(ManagedSchemaTransform transform) {
      ManagedConfig managedConfig = transform.getManagedConfig();
      Row configRow;
      try {
        configRow =
            SchemaRegistry.createDefault()
                .getToRowFunction(ManagedConfig.class)
                .apply(managedConfig);
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
      // Sanity check: sort fields according to the configuration schema
      return SCHEMA.getFields().stream()
          .map(field -> configRow.getValue(field.getName()))
          .collect(Row.toRow(SCHEMA));
    }

    @Override
    public ManagedSchemaTransform fromConfigRow(Row configRow, PipelineOptions options) {
      return (ManagedSchemaTransform) PROVIDER.from(configRow);
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
