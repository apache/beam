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
package org.apache.beam.sdk.io.iceberg;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.checkerframework.checker.nullness.qual.Nullable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SchemaTransformConfiguration {
  public static Builder builder() {
    return new AutoValue_SchemaTransformConfiguration.Builder();
  }

  @SchemaFieldDescription("Identifier of the Iceberg table.")
  public abstract String getTable();

  @SchemaFieldDescription("Name of the catalog containing the table.")
  @Nullable
  public abstract String getCatalogName();

  @SchemaFieldDescription("Properties used to set up the Iceberg catalog.")
  @Nullable
  public abstract Map<String, String> getCatalogProperties();

  @SchemaFieldDescription("Properties passed to the Hadoop Configuration.")
  @Nullable
  public abstract Map<String, String> getConfigProperties();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTable(String table);

    public abstract Builder setCatalogName(String catalogName);

    public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

    public abstract Builder setConfigProperties(Map<String, String> confProperties);

    public abstract SchemaTransformConfiguration build();
  }

  public IcebergCatalogConfig getIcebergCatalog() {
    return IcebergCatalogConfig.builder()
        .setCatalogName(getCatalogName())
        .setCatalogProperties(getCatalogProperties())
        .setConfigProperties(getConfigProperties())
        .build();
  }
}
