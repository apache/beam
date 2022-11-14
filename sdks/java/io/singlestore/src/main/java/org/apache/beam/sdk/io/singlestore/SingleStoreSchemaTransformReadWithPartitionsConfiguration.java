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
package org.apache.beam.sdk.io.singlestore;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Configuration for parallel reading from SignleStoreDB.
 *
 * <p>This class is meant to be used with {@link
 * SingleStoreSchemaTransformReadWithPartitionsProvider}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SingleStoreSchemaTransformReadWithPartitionsConfiguration {

  /** Instantiates a {@link SingleStoreSchemaTransformReadWithPartitionsConfiguration.Builder}. */
  public static Builder builder() {
    return new AutoValue_SingleStoreSchemaTransformReadWithPartitionsConfiguration.Builder();
  }

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<SingleStoreSchemaTransformReadWithPartitionsConfiguration>
      TYPE_DESCRIPTOR =
          TypeDescriptor.of(SingleStoreSchemaTransformReadWithPartitionsConfiguration.class);
  private static final SerializableFunction<
          SingleStoreSchemaTransformReadWithPartitionsConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  /** Serializes configuration to a {@link Row}. */
  Row toBeamRow() {
    return ROW_SERIALIZABLE_FUNCTION.apply(this);
  }

  @Nullable
  public abstract SingleStoreIO.DataSourceConfiguration getDataSourceConfiguration();

  @Nullable
  public abstract String getQuery();

  @Nullable
  public abstract String getTable();

  @Nullable
  public abstract Integer getInitialNumReaders();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDataSourceConfiguration(SingleStoreIO.DataSourceConfiguration value);

    public abstract Builder setTable(String value);

    public abstract Builder setQuery(String value);

    public abstract Builder setInitialNumReaders(Integer value);

    /**
     * Builds the {@link SingleStoreSchemaTransformReadWithPartitionsConfiguration} configuration.
     */
    public abstract SingleStoreSchemaTransformReadWithPartitionsConfiguration build();
  }
}
