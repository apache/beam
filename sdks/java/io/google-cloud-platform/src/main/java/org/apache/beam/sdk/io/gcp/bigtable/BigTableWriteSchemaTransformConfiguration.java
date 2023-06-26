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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.auto.value.AutoValue;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigTableWriteSchemaTransformConfiguration {
  @SchemaFieldDescription("The Google Cloud project that the Bigtable instance is in.")
  public abstract String getProjectId();

  @SchemaFieldDescription("The ID of the Bigtable instance to write to.")
  public abstract String getInstanceId();

  @SchemaFieldDescription("The ID of the Bigtable table to write to.")
  public abstract String getTableId();

  @SchemaFieldDescription("Columns that make up a row's key.")
  public abstract List<String> getKeyColumns();

  public abstract @Nullable String getEndpoint();

  @SchemaFieldDescription("The ID of the app profile used to connect to Bigtable.")
  public abstract @Nullable String getAppProfileId();

  public static Builder builder() {
    return new AutoValue_BigTableWriteSchemaTransformConfiguration.Builder();
  }

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigTableWriteSchemaTransformConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigTableWriteSchemaTransformConfiguration.class);
  private static final SerializableFunction<BigTableWriteSchemaTransformConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  /** Transform configuration to a {@link Row}. */
  public Row toBeamRow() {
    return ROW_SERIALIZABLE_FUNCTION.apply(this);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setProjectId(String value);

    public abstract Builder setInstanceId(String value);

    public abstract Builder setTableId(String value);

    public abstract Builder setKeyColumns(List<String> value);

    public abstract Builder setEndpoint(@Nullable String endpoint);

    public abstract Builder setAppProfileId(@Nullable String appProfile);

    public abstract BigTableWriteSchemaTransformConfiguration build();
  }
}
