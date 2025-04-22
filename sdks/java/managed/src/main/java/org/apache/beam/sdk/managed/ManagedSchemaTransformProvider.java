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

import static org.apache.beam.sdk.managed.ManagedTransformConstants.CONFIG_NAME_OVERRIDES;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class ManagedSchemaTransformProvider
    extends TypedSchemaTransformProvider<ManagedSchemaTransformProvider.ManagedConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedSchemaTransformProvider.class);

  @Override
  public String identifier() {
    return "beam:transform:managed:v1";
  }

  // Use 'getAllProviders()' instead of this cache.
  private final Map<String, SchemaTransformProvider> schemaTransformProvidersCache =
      new HashMap<>();
  private boolean providersCached = false;

  private @Nullable Collection<String> supportedIdentifiers;

  public ManagedSchemaTransformProvider() {
    this(null);
  }

  ManagedSchemaTransformProvider(@Nullable Collection<String> supportedIdentifiers) {
    this.supportedIdentifiers = supportedIdentifiers;
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  @VisibleForTesting
  abstract static class ManagedConfig {
    public static Builder builder() {
      return new AutoValue_ManagedSchemaTransformProvider_ManagedConfig.Builder();
    }

    @SchemaFieldDescription(
        "Identifier of the underlying SchemaTransform to discover and instantiate.")
    public abstract String getTransformIdentifier();

    @SchemaFieldDescription(
        "URL path to the YAML config file used to build the underlying SchemaTransform.")
    public abstract @Nullable String getConfigUrl();

    @SchemaFieldDescription("YAML string config used to build the underlying SchemaTransform.")
    public abstract @Nullable String getConfig();

    @SchemaFieldDescription(
        "Skips configuration validation. If unset, the pipeline will fail at construction "
            + "time if the configuration includes unknown fields or missing required fields.")
    public abstract @Nullable Boolean getSkipConfigValidation();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTransformIdentifier(String identifier);

      public abstract Builder setConfigUrl(@Nullable String configUrl);

      public abstract Builder setConfig(@Nullable String yamlConfig);

      public abstract Builder setSkipConfigValidation(boolean skip);

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

    private Map<String, Object> resolveUnderlyingConfig() {
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

      return YamlUtils.yamlStringToMap(yamlTransformConfig);
    }
  }

  @Override
  protected SchemaTransform from(ManagedConfig managedConfig) {
    managedConfig.validate();
    SchemaTransformProvider schemaTransformProvider =
        Preconditions.checkNotNull(
            getAllProviders().get(managedConfig.getTransformIdentifier()),
            "Could not find a transform with the identifier "
                + "%s. This could be either due to the dependency with the "
                + "transform not being available in the classpath or due to "
                + "the specified transform not being supported.",
            managedConfig.getTransformIdentifier());

    return new ManagedSchemaTransform(managedConfig, schemaTransformProvider);
  }

  static class ManagedSchemaTransform extends SchemaTransform {
    private final ManagedConfig managedConfig;
    private final SchemaTransformProvider underlyingTransformProvider;

    ManagedSchemaTransform(
        ManagedConfig managedConfig, SchemaTransformProvider underlyingTransformProvider) {
      this.underlyingTransformProvider = underlyingTransformProvider;
      this.managedConfig = managedConfig;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Row underlyingRowConfig =
          getRowConfig(
              managedConfig,
              underlyingTransformProvider.configurationSchema(),
              input.getPipeline().getOptions());
      LOG.debug(
          "Building transform \"{}\" with configuration: {}",
          underlyingTransformProvider.identifier(),
          underlyingRowConfig);

      return input.apply(underlyingTransformProvider.from(underlyingRowConfig));
    }

    @VisibleForTesting
    public ManagedConfig getManagedConfig() {
      return this.managedConfig;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
        return SchemaRegistry.createDefault()
            .getToRowFunction(ManagedConfig.class)
            .apply(managedConfig)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // May return an empty row (perhaps the underlying transform doesn't have any required
  // parameters)
  @VisibleForTesting
  static Row getRowConfig(
      ManagedConfig config, Schema transformConfigSchema, PipelineOptions options) {
    Map<String, Object> configMap = config.resolveUnderlyingConfig();
    // Build a config Row that will be used to build the underlying SchemaTransform.
    // If a mapping for the SchemaTransform exists, we use it to update parameter names to align
    // with the underlying SchemaTransform config schema
    Map<String, String> namingOverride = CONFIG_NAME_OVERRIDES.get(config.getTransformIdentifier());
    if (namingOverride != null && configMap != null) {
      Map<String, Object> remappedConfig = new HashMap<>();
      for (Map.Entry<String, Object> entry : configMap.entrySet()) {
        String paramName = entry.getKey();
        if (namingOverride.containsKey(paramName)) {
          paramName = namingOverride.get(paramName);
        }
        remappedConfig.put(paramName, entry.getValue());
      }
      configMap = remappedConfig;
    }

    @Nullable Boolean skipValidation = config.getSkipConfigValidation();
    if (skipValidation == null || !skipValidation) {
      validateUserConfig(
          config.getTransformIdentifier(),
          new HashSet<>(configMap.keySet()),
          transformConfigSchema);
    }

    return YamlUtils.toBeamRow(configMap, transformConfigSchema, false);
  }

  static void validateUserConfig(
      String transformId, Set<String> userParams, Schema transformConfigSchema) {
    List<String> missingRequiredFields = new ArrayList<>();
    for (Schema.Field field : transformConfigSchema.getFields()) {
      boolean inUserConfig = userParams.remove(field.getName());
      if (!field.getType().getNullable() && !inUserConfig) {
        missingRequiredFields.add(field.getName());
      }
    }

    if (!missingRequiredFields.isEmpty() || !userParams.isEmpty()) {
      String msg = "Invalid config for transform '" + transformId + "':";
      if (!missingRequiredFields.isEmpty()) {
        msg += " Missing required fields: " + missingRequiredFields + ".";
      }
      if (!userParams.isEmpty()) {
        msg += " Contains unknown fields: " + userParams + ".";
      }

      throw new IllegalArgumentException(msg);
    }
  }

  // We load providers separately, after construction, to prevent the
  // 'ManagedSchemaTransformProvider' from being initialized in a recursive loop
  // when being loaded using 'AutoValue'.
  synchronized Map<String, SchemaTransformProvider> getAllProviders() {
    if (this.providersCached) {
      return schemaTransformProvidersCache;
    }
    try {
      for (SchemaTransformProvider schemaTransformProvider :
          ServiceLoader.load(SchemaTransformProvider.class)) {
        if (schemaTransformProvidersCache.containsKey(schemaTransformProvider.identifier())) {
          throw new IllegalArgumentException(
              "Found multiple SchemaTransformProvider implementations with the same identifier "
                  + schemaTransformProvider.identifier());
        }
        if (supportedIdentifiers == null
            || supportedIdentifiers.contains(schemaTransformProvider.identifier())) {
          if (schemaTransformProvider.identifier().equals("beam:transform:managed:v1")) {
            // Prevent recursively adding the 'ManagedSchemaTransformProvider'.
            continue;
          }
          schemaTransformProvidersCache.put(
              schemaTransformProvider.identifier(), schemaTransformProvider);
        }
      }
      this.providersCached = true;
      return schemaTransformProvidersCache;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
