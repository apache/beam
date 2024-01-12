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
package org.apache.beam.sdk.extensions.schemaio.expansion;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@AutoService(ExternalTransformRegistrar.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ExternalSchemaIOTransformRegistrar implements ExternalTransformRegistrar {

  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    Map<String, ExternalTransformBuilder<?, ?, ?>> providers = new HashMap<>();
    try {
      for (SchemaIOProvider provider : ServiceLoader.load(SchemaIOProvider.class)) {
        // Avro provider is treated as a special case since two Avro providers may want to be loaded
        // from "core" (deprecated) and from "extensions/avro" (actual) - but only one must succeed.
        // TODO: we won't need this check once all Avro providers from "core" will be
        // removed
        if (provider.identifier().equals("avro")) {
          // Avro provider from "extensions/avro" must have a priority.
          if (provider.getClass().getName().startsWith("org.apache.beam.sdk.extensions.avro")) {
            // Load Avro provider from "extensions/avro" by any case.
            registerProvider(providers, provider);
          } else {
            // Load Avro provider from "core" if it was not loaded from Avro extension before.
            registerProviderOptionally(providers, provider);
          }
        } else {
          final String identifier =
              "beam:transform:org.apache.beam:schemaio_" + provider.identifier() + "_read:v1";
          checkState(
              !providers.containsKey(identifier),
              "Duplicate providers exist with identifier `%s` for class %s.",
              identifier,
              SchemaIOProvider.class);
          registerProvider(providers, provider);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return ImmutableMap.copyOf(providers);
  }

  private void registerProvider(
      Map<String, ExternalTransformBuilder<?, ?, ?>> providers, SchemaIOProvider provider) {
    providers.put(
        "beam:transform:org.apache.beam:schemaio_" + provider.identifier() + "_read:v1",
        new ReaderBuilder(provider));
    providers.put(
        "beam:transform:org.apache.beam:schemaio_" + provider.identifier() + "_write:v1",
        new WriterBuilder(provider));
  }

  private void registerProviderOptionally(
      Map<String, ExternalTransformBuilder<?, ?, ?>> providers, SchemaIOProvider provider) {
    providers.putIfAbsent(
        "beam:transform:org.apache.beam:schemaio_" + provider.identifier() + "_read:v1",
        new ReaderBuilder(provider));
    providers.putIfAbsent(
        "beam:transform:org.apache.beam:schemaio_" + provider.identifier() + "_write:v1",
        new WriterBuilder(provider));
  }

  public static class Configuration {
    String location = "";
    byte[] config = new byte[0];
    @Nullable byte[] dataSchema = null;

    public void setLocation(String location) {
      this.location = location;
    }

    public void setConfig(byte[] config) {
      this.config = config;
    }

    public void setDataSchema(@Nullable byte[] dataSchema) {
      this.dataSchema = dataSchema;
    }
  }

  @Nullable
  private static Schema translateSchema(@Nullable byte[] schemaBytes) {
    if (schemaBytes == null) {
      return null;
    }

    try {
      SchemaApi.Schema protoSchema = SchemaApi.Schema.parseFrom(schemaBytes);
      return SchemaTranslation.schemaFromProto(protoSchema);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to infer data schema from configuration proto.", e);
    }
  }

  private static Row translateRow(byte[] rowBytes, Schema configSchema) {
    RowCoder rowCoder = RowCoder.of(configSchema);
    InputStream stream = new ByteArrayInputStream(rowBytes);

    try {
      return rowCoder.decode(stream);
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to infer configuration row from configuration proto and schema.", e);
    }
  }

  @VisibleForTesting
  static class ReaderBuilder
      implements ExternalTransformBuilder<Configuration, PBegin, PCollection<Row>> {
    SchemaIOProvider schemaIOProvider;

    ReaderBuilder(SchemaIOProvider schemaIOProvider) {
      this.schemaIOProvider = schemaIOProvider;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildExternal(Configuration configuration) {
      return schemaIOProvider
          .from(
              configuration.location,
              translateRow(configuration.config, schemaIOProvider.configurationSchema()),
              translateSchema(configuration.dataSchema))
          .buildReader();
    }
  }

  @VisibleForTesting
  static class WriterBuilder
      implements ExternalTransformBuilder<Configuration, PCollection<Row>, PDone> {
    SchemaIOProvider schemaIOProvider;

    WriterBuilder(SchemaIOProvider schemaIOProvider) {
      this.schemaIOProvider = schemaIOProvider;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildExternal(Configuration configuration) {
      return (PTransform<PCollection<Row>, PDone>)
          schemaIOProvider
              .from(
                  configuration.location,
                  translateRow(configuration.config, schemaIOProvider.configurationSchema()),
                  translateSchema(configuration.dataSchema))
              .buildWriter();
    }
  }
}
