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
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

public class Managed {
  public static final String READ = "READ";
  public static final String WRITE = "WRITE";

  public enum IO {
    ICEBERG
  }

  public static Read read() {
    return new AutoValue_Managed_Read.Builder().build();
  }

  @AutoValue
  public abstract static class Read extends SchemaTransform {
    private final Map<IO, String> identifiers =
        ImmutableMap.of(IO.ICEBERG, "beam:schematransform:org.apache.beam:iceberg_read:v1");

    abstract IO getSource();

    abstract @Nullable String getConfig();

    abstract @Nullable String getConfigUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSource(IO source);

      abstract Builder setConfig(String config);

      abstract Builder setConfigUrl(String configUrl);

      abstract Read build();
    }

    public Read from(IO source) {
      return toBuilder().setSource(source).build();
    }

    public Read withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    public Read withConfig(String config) {
      return toBuilder().setConfigUrl(config).build();
    }

    public Read withConfig(Map<String, Object> config) {
      return toBuilder().setConfigUrl(mapToYamlString(config)).build();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      String underlyingTransformIdentifier = identifiers.get(getSource());

      ManagedSchemaTransformProvider.ManagedConfig managedConfig =
          ManagedSchemaTransformProvider.ManagedConfig.builder()
              .setIdentifier(underlyingTransformIdentifier)
              .setType(READ)
              .setConfig(getConfig())
              .setConfigUrl(getConfigUrl())
              .build();

      SchemaTransform underlyingTransform = ManagedSchemaTransformProvider.of().from(managedConfig);

      return input.apply(underlyingTransform);
    }
  }

  public static Write write() {
    return new AutoValue_Managed_Write.Builder().build();
  }

  @AutoValue
  public abstract static class Write extends SchemaTransform {
    private final Map<IO, String> identifiers =
        ImmutableMap.of(IO.ICEBERG, "beam:schematransform:org.apache.beam:iceberg_write:v1");

    abstract IO getSink();

    abstract @Nullable String getConfig();

    abstract @Nullable String getConfigUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSink(IO source);

      abstract Builder setConfig(String config);

      abstract Builder setConfigUrl(String configUrl);

      abstract Write build();
    }

    public Write to(IO source) {
      return toBuilder().setSink(source).build();
    }

    public Write withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    public Write withConfig(String config) {
      return toBuilder().setConfigUrl(config).build();
    }

    public Write withConfig(Map<String, Object> config) {
      return toBuilder().setConfigUrl(mapToYamlString(config)).build();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      String underlyingTransformIdentifier = identifiers.get(getSink());

      ManagedSchemaTransformProvider.ManagedConfig managedConfig =
          ManagedSchemaTransformProvider.ManagedConfig.builder()
              .setIdentifier(underlyingTransformIdentifier)
              .setType(WRITE)
              .setConfig(getConfig())
              .setConfigUrl(getConfigUrl())
              .build();

      SchemaTransform underlyingTransform = ManagedSchemaTransformProvider.of().from(managedConfig);

      return input.apply(underlyingTransform);
    }
  }

  // TODO: implement this
  private static String mapToYamlString(Map<String, Object> map) {
    return "";
  }
}
