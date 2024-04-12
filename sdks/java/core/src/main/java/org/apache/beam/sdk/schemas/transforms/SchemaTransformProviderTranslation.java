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

import static org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SchemaTransformProviderTranslation {
  public static class SchemaTransformTranslator
      implements TransformPayloadTranslator<SchemaTransform<?>> {
    private final String identifier;
    private SchemaTransformProvider provider;

    public SchemaTransformTranslator(String identifier) {
      this.identifier = identifier;
      try {
        for (SchemaTransformProvider schemaTransformProvider :
            ServiceLoader.load(SchemaTransformProvider.class)) {
          if (schemaTransformProvider.identifier().equalsIgnoreCase(identifier)) {
            if (this.provider != null) {
              throw new IllegalArgumentException(
                  "Found multiple SchemaTransformProvider implementations with the same identifier "
                      + identifier);
            }
            this.provider = schemaTransformProvider;
          }
        }
        if (this.provider == null) {
          throw new IllegalArgumentException(
              "Could not find SchemaTransformProvider implementation for identifier " + identifier);
        }
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }

    @Override
    public String getUrn() {
      return identifier;
    }

    @Override
    @SuppressWarnings("argument")
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, SchemaTransform<?>> application, SdkComponents components)
        throws IOException {
      return FunctionSpec.newBuilder().setUrn(getUrn()).setPayload(ByteString.empty()).build();
    }

    @Override
    public Row toConfigRow(SchemaTransform<?> transform) {
      return transform.getConfigurationRow();
    }

    @Override
    public SchemaTransform<?> fromConfigRow(Row configRow, PipelineOptions options) {
      return provider.from(configRow);
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class SchemaTransformRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      Map<Class<SchemaTransform>, SchemaTransformTranslator> translators = new HashMap<>();

      try {
        for (SchemaTransformProvider schemaTransformProvider :
            ServiceLoader.load(SchemaTransformProvider.class)) {
          translators.put(
              SchemaTransform.class,
              new SchemaTransformTranslator(schemaTransformProvider.identifier()));
        }
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }

      return translators;
    }
  }
}
