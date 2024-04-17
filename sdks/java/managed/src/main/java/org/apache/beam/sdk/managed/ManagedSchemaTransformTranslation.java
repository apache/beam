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
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaAwareTransforms.ManagedSchemaTransformPayload;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
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
    private final ManagedSchemaTransformProvider provider;
    static final Schema SCHEMA =
        Schema.builder()
            .addStringField("transform_identifier")
            .addNullableStringField("config")
            .addNullableStringField("config_url")
            .build();

    public ManagedSchemaTransformTranslator() {
      provider = new ManagedSchemaTransformProvider(null);
    }

    @Override
    public String getUrn() {
      return provider.identifier();
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
                  .setUnderlyingTransformUrn(managedConfig.getTransformIdentifier())
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
      Map<String, Object> fieldValues = new HashMap<>();

      if (managedConfig.getTransformIdentifier() != null) {
        fieldValues.put("transform_identifier", managedConfig.getTransformIdentifier());
      }
      String config = managedConfig.getConfig();
      if (config != null) {
        fieldValues.put("config", config);
      }
      String configUrl = managedConfig.getConfigUrl();
      if (configUrl != null) {
        fieldValues.put("config_url", configUrl);
      }

      return Row.withSchema(SCHEMA).withFieldValues(fieldValues).build();
    }

    @Override
    public ManagedSchemaTransform fromConfigRow(Row configRow, PipelineOptions options) {
      ManagedConfig.Builder configBuilder = ManagedConfig.builder();

      String transformIdentifier = configRow.getValue("transform_identifier");
      if (transformIdentifier != null) {
        configBuilder = configBuilder.setTransformIdentifier(transformIdentifier);
      }
      String config = configRow.getValue("config");
      if (config != null) {
        configBuilder = configBuilder.setConfig(config);
      }
      String configUrl = configRow.getValue("config_url");
      if (configUrl != null) {
        configBuilder = configBuilder.setConfigUrl(configUrl);
      }

      return (ManagedSchemaTransform) provider.from(configBuilder.build());
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
