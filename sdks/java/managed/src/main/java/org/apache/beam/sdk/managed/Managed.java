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

import com.google.auto.value.AutoValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

public class Managed {
  public static Read read() {
    return new AutoValue_Managed_Read.Builder().build();
  }

  @AutoValue
  public abstract static class Read extends SchemaTransform {
    public static Map<String, String> TRANSFORMS =
        ImmutableMap.<String, String>builder()
            .put("iceberg", "beam:schematransform:org.apache.beam:iceberg_read:v1")
            .build();

    abstract String getSource();

    abstract @Nullable String getConfig();

    abstract @Nullable String getConfigUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSource(String source);

      abstract Builder setConfig(String config);

      abstract Builder setConfigUrl(String configUrl);

      abstract Read build();
    }

    public Read from(String source) {
      Preconditions.checkArgument(
          TRANSFORMS.containsKey(source.toLowerCase()),
          "An unsupported source was specified: '%s'. Please specify one of the following source: %s",
          source,
          TRANSFORMS.keySet());
      return toBuilder().setSource(TRANSFORMS.get(source.toLowerCase())).build();
    }

    public Read withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    public Read withConfig(String config) {
      return toBuilder().setConfig(config).build();
    }

    public Read withConfig(Map<String, Object> config) {
      return toBuilder().setConfig(YamlUtils.yamlStringFromMap(config)).build();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      ManagedSchemaTransformProvider.ManagedConfig managedConfig =
          ManagedSchemaTransformProvider.ManagedConfig.builder()
              .setTransformIdentifier(getSource())
              .setConfig(getConfig())
              .setConfigUrl(getConfigUrl())
              .build();

      SchemaTransform underlyingTransform =
          ManagedSchemaTransformProvider.of(TRANSFORMS.values()).from(managedConfig);

      return input.apply(underlyingTransform);
    }
  }

  public static Write write() {
    return new AutoValue_Managed_Write.Builder().build();
  }

  @AutoValue
  public abstract static class Write extends SchemaTransform {
    public static Map<String, String> TRANSFORMS =
        ImmutableMap.<String, String>builder()
            .put("iceberg", "beam:schematransform:org.apache.beam:iceberg_write:v1")
            .build();

    abstract String getSink();

    abstract @Nullable String getConfig();

    abstract @Nullable String getConfigUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSink(String source);

      abstract Builder setConfig(String config);

      abstract Builder setConfigUrl(String configUrl);

      abstract Write build();
    }

    public Write to(String sink) {
      Preconditions.checkArgument(
          TRANSFORMS.containsKey(sink.toLowerCase()),
          "An unsupported sink was specified: '%s'. Please specify one of the following sinks: %s",
          sink,
          TRANSFORMS.keySet());
      return toBuilder().setSink(TRANSFORMS.get(sink.toLowerCase())).build();
    }

    public Write withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    public Write withConfig(String config) {
      return toBuilder().setConfig(config).build();
    }

    public Write withConfig(Map<String, Object> config) {
      return toBuilder().setConfig(YamlUtils.yamlStringFromMap(config)).build();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      ManagedSchemaTransformProvider.ManagedConfig managedConfig =
          ManagedSchemaTransformProvider.ManagedConfig.builder()
              .setTransformIdentifier(getSink())
              .setConfig(getConfig())
              .setConfigUrl(getConfigUrl())
              .build();

      SchemaTransform underlyingTransform =
          ManagedSchemaTransformProvider.of(TRANSFORMS.values()).from(managedConfig);

      return input.apply(underlyingTransform);
    }
  }
}
