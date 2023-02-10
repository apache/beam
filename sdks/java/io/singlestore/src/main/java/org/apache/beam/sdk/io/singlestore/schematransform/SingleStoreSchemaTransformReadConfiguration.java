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
package org.apache.beam.sdk.io.singlestore.schematransform;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Configuration for reading from SingleStoreDB.
 *
 * <p>This class is meant to be used with {@link SingleStoreSchemaTransformReadProvider}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SingleStoreSchemaTransformReadConfiguration {

  /** Instantiates a {@link SingleStoreSchemaTransformReadConfiguration.Builder}. */
  public static Builder builder() {
    return new AutoValue_SingleStoreSchemaTransformReadConfiguration.Builder();
  }

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<SingleStoreSchemaTransformReadConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(SingleStoreSchemaTransformReadConfiguration.class);
  private static final SerializableFunction<SingleStoreSchemaTransformReadConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  /** Serializes configuration to a {@link Row}. */
  public Row toBeamRow() {
    return ROW_SERIALIZABLE_FUNCTION.apply(this);
  }

  @Nullable
  public abstract SingleStoreIO.DataSourceConfiguration getDataSourceConfiguration();

  @Nullable
  public abstract String getQuery();

  @Nullable
  public abstract String getTable();

  @Nullable
  public abstract Boolean getOutputParallelization();

  @Nullable
  public abstract Boolean getWithPartitions();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDataSourceConfiguration(SingleStoreIO.DataSourceConfiguration value);

    public abstract Builder setTable(String value);

    public abstract Builder setQuery(String value);

    public abstract Builder setOutputParallelization(Boolean value);

    public abstract Builder setWithPartitions(Boolean value);

    /** Builds the {@link SingleStoreSchemaTransformReadConfiguration} configuration. */
    public abstract SingleStoreSchemaTransformReadConfiguration build();
  }
}
