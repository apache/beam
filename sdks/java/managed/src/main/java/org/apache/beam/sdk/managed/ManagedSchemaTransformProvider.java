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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

@AutoService(SchemaTransformProvider.class)
public class ManagedSchemaTransformProvider
    extends TypedSchemaTransformProvider<ManagedSchemaTransformProvider.ManagedConfig> {

  @Override
  public String identifier() {
    return "beam:transform:managed:v1";
  }

  private final Map<String, SchemaTransformProvider> schemaTransformProviders = new HashMap<>();

  public ManagedSchemaTransformProvider() {}

  ManagedSchemaTransformProvider(@Nullable Collection<String> supportedIdentifiers) {
    try {
      for (SchemaTransformProvider schemaTransformProvider :
          ServiceLoader.load(SchemaTransformProvider.class)) {
        if (schemaTransformProviders.containsKey(schemaTransformProvider.identifier())) {
          throw new IllegalArgumentException(
              "Found multiple SchemaTransformProvider implementations with the same identifier "
                  + schemaTransformProvider.identifier());
        }
        if (supportedIdentifiers == null
            || supportedIdentifiers.contains(schemaTransformProvider.identifier())) {
          schemaTransformProviders.put(
              schemaTransformProvider.identifier(), schemaTransformProvider);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  @VisibleForTesting
  public abstract static class ManagedConfig {
    public static Builder builder() {
      return new AutoValue_ManagedSchemaTransformProvider_ManagedConfig.Builder();
    }

    @SchemaFieldDescription("Identifier of the underlying IO to instantiate.")
    public abstract String getTransformIdentifier();

    @SchemaFieldDescription("URL path to the YAML config file used to build the underlying IO.")
    public abstract @Nullable String getConfigUrl();

    @SchemaFieldDescription("YAML string config used to build the underlying IO.")
    public abstract @Nullable String getConfig();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTransformIdentifier(String identifier);

      public abstract Builder setConfigUrl(@Nullable String configUrl);

      public abstract Builder setConfig(@Nullable String yamlConfig);

      public abstract ManagedConfig build();
    }

    protected void validate() {
      boolean configExists = !Strings.isNullOrEmpty(getConfig());
      boolean configUrlExists = !Strings.isNullOrEmpty(getConfigUrl());
      List<Boolean> configs = Arrays.asList(configExists, configUrlExists);
      checkArgument(
          1 == configs.stream().filter(Predicates.equalTo(true)).count(),
          "Please specify a config or a config URL, but not both.");
    }

    public @Nullable String resolveUnderlyingConfig() {
      String yamlTransformConfig = getConfig();
      // If YAML string is empty, then attempt to read from YAML file
      if (Strings.isNullOrEmpty(yamlTransformConfig)) {
        try {
          MatchResult.Metadata fileMetaData =
              FileSystems.matchSingleFileSpec(Preconditions.checkNotNull(getConfigUrl()));
          ByteBuffer buffer = ByteBuffer.allocate((int) fileMetaData.sizeBytes());
          FileSystems.open(fileMetaData.resourceId()).read(buffer);
          yamlTransformConfig = new String(buffer.array(), StandardCharsets.UTF_8);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return yamlTransformConfig;
    }
  }

  @Override
  protected SchemaTransform from(ManagedConfig managedConfig) {
    managedConfig.validate();
    SchemaTransformProvider schemaTransformProvider =
        Preconditions.checkNotNull(
            schemaTransformProviders.get(managedConfig.getTransformIdentifier()),
            "Could not find transform with identifier %s, or it may not be supported",
            managedConfig.getTransformIdentifier());

    return new ManagedSchemaTransform(managedConfig, schemaTransformProvider);
  }

  public static class ManagedSchemaTransform extends SchemaTransform {
    private final Row transformConfig;
    private final ManagedConfig managedConfig;
    private final SchemaTransformProvider underlyingTransformProvider;

    ManagedSchemaTransform(
        ManagedConfig managedConfig, SchemaTransformProvider underlyingTransformProvider) {
      // parse config before expansion to check if it matches underlying transform's config schema
      Schema transformConfigSchema = underlyingTransformProvider.configurationSchema();
      Row transformConfig;
      try {
        transformConfig = getRowConfig(managedConfig, transformConfigSchema);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Encountered an error when retrieving a Row configuration", e);
      }

      this.transformConfig = transformConfig;
      this.managedConfig = managedConfig;
      this.underlyingTransformProvider = underlyingTransformProvider;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      SchemaTransform underlyingTransform = underlyingTransformProvider.from(transformConfig);

      return input.apply(underlyingTransform);
    }

    public ManagedConfig getManagedConfig() {
      return this.managedConfig;
    }
  }

  @VisibleForTesting
  static Row getRowConfig(ManagedConfig config, Schema transformSchema) {
    Row configRow = YamlUtils.toBeamRow(config.resolveUnderlyingConfig(), transformSchema, true);
    // If our config is still null (perhaps the underlying transform doesn't have any required
    // parameters), then return an empty row.
    return configRow != null ? configRow : Row.nullRow(transformSchema);
  }

  Map<String, SchemaTransformProvider> getAllProviders() {
    return schemaTransformProviders;
  }
}
