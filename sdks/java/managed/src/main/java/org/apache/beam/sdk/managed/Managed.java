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
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;

public class Managed {
  public static Read read() {
    return new AutoValue_Managed_Read.Builder().setPattern(Read.PATTERN).build();
  }

  @AutoValue
  public abstract static class Read extends SchemaTransform {
    protected static final Pattern PATTERN =
        Pattern.compile("beam:schematransform:org.apache.beam:[\\w-]+_read[\\w-]*:[\\w-]+");

    abstract String getSource();

    abstract Pattern getPattern();

    abstract @Nullable String getConfig();

    abstract @Nullable String getConfigUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSource(String source);

      abstract Builder setPattern(Pattern pattern);

      abstract Builder setConfig(String config);

      abstract Builder setConfigUrl(String configUrl);

      abstract Read build();
    }

    public Read from(String identifier) {
      return toBuilder().setSource(identifier).build();
    }

    public Read withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    public Read withConfig(String config) {
      return toBuilder().setConfig(config).build();
    }

    public Read withConfig(Map<String, Object> config) {
      return toBuilder().setConfig(mapToYamlString(config)).build();
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
          ManagedSchemaTransformProvider.of(getPattern()).from(managedConfig);

      return input.apply(underlyingTransform);
    }
  }

  public static Write write() {
    return new AutoValue_Managed_Write.Builder().setPattern(Write.PATTERN).build();
  }

  @AutoValue
  public abstract static class Write extends SchemaTransform {
    protected static final Pattern PATTERN =
        Pattern.compile("beam:schematransform:org.apache.beam:[\\w-]+_write[\\w-]*:[\\w-]+");

    abstract String getSink();

    abstract Pattern getPattern();

    abstract @Nullable String getConfig();

    abstract @Nullable String getConfigUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSink(String source);

      abstract Builder setPattern(Pattern pattern);

      abstract Builder setConfig(String config);

      abstract Builder setConfigUrl(String configUrl);

      abstract Write build();
    }

    public Write to(String source) {
      return toBuilder().setSink(source).build();
    }

    public Write withConfigUrl(String configUrl) {
      return toBuilder().setConfigUrl(configUrl).build();
    }

    public Write withConfig(String config) {
      return toBuilder().setConfig(config).build();
    }

    public Write withConfig(Map<String, Object> config) {
      return toBuilder().setConfig(mapToYamlString(config)).build();
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
          ManagedSchemaTransformProvider.of(getPattern()).from(managedConfig);

      return input.apply(underlyingTransform);
    }
  }

  // TODO: implement this
  private static String mapToYamlString(Map<String, Object> map) {
    return "";
  }
}
