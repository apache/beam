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
package org.apache.beam.sdk.io.jdbc;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class JdbcPartitionedReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        JdbcPartitionedReadSchemaTransformProvider
            .JdbcPartitionedReadSchemaTransformConfiguration> {

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<
          JdbcPartitionedReadSchemaTransformConfiguration>
      configurationClass() {
    return JdbcPartitionedReadSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcPartitionedReadSchemaTransformConfiguration configuration) {
    return new JdbcPartitionedReadSchemaTransform(configuration);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:jdbc_partitioned_read:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("output");
  }

  public static class JdbcPartitionedReadSchemaTransform implements SchemaTransform {

    JdbcPartitionedReadSchemaTransformConfiguration config;

    public JdbcPartitionedReadSchemaTransform(
        JdbcPartitionedReadSchemaTransformConfiguration config) {
      this.config = config;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PTransform<
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
        buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          return input;
        }
      };
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class JdbcPartitionedReadSchemaTransformConfiguration
      implements Serializable {

    public abstract String getDriverClassName();

    public abstract String getJdbcUrl();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    @Nullable
    public abstract String getConnectionProperties();

    @Nullable
    public abstract String getLocation();

    public abstract String getPartitionColumn();

    @Nullable
    public abstract Short getPartitions();

    public void validate() {}

    public static Builder builder() {
      return new AutoValue_JdbcPartitionedReadSchemaTransformProvider_JdbcPartitionedReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDriverClassName(String value);

      public abstract Builder setJdbcUrl(String value);

      public abstract Builder setUsername(String value);

      public abstract Builder setPassword(String value);

      public abstract Builder setConnectionProperties(String value);

      public abstract Builder setLocation(String value);

      public abstract Builder setPartitionColumn(String value);

      public abstract Builder setPartitions(Short value);

      public abstract JdbcPartitionedReadSchemaTransformConfiguration build();
    }
  }
}
