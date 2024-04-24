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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Set;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.iceberg.CatalogUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class IcebergSchemaTransformCatalogConfig {
  public static Builder builder() {
    return new AutoValue_IcebergSchemaTransformCatalogConfig.Builder();
  }

  public abstract String getCatalogName();

  @SchemaFieldDescription("Valid types are: {hadoop, hive, rest}")
  public abstract @Nullable String getCatalogType();

  public abstract @Nullable String getCatalogImplementation();

  public abstract @Nullable String getWarehouseLocation();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCatalogName(String catalogName);

    public abstract Builder setCatalogType(String catalogType);

    public abstract Builder setCatalogImplementation(String catalogImplementation);

    public abstract Builder setWarehouseLocation(String warehouseLocation);

    public abstract IcebergSchemaTransformCatalogConfig build();
  }

  public static final Schema SCHEMA;

  static {
    try {
      // To stay consistent with our SchemaTransform configuration naming conventions,
      // we sort lexicographically and convert field names to snake_case
      SCHEMA =
          SchemaRegistry.createDefault()
              .getSchema(IcebergSchemaTransformCatalogConfig.class)
              .sorted()
              .toSnakeCase();
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("argument")
  public Row toRow() {
    return Row.withSchema(SCHEMA)
        .withFieldValue("catalog_name", getCatalogName())
        .withFieldValue("catalog_type", getCatalogType())
        .withFieldValue("catalog_implementation", getCatalogImplementation())
        .withFieldValue("warehouse_location", getWarehouseLocation())
        .build();
  }

  public static final Set<String> VALID_CATALOG_TYPES =
      Sets.newHashSet(
          CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
          CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
          CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

  public void validate() {
    if (Strings.isNullOrEmpty(getCatalogType())) {
      checkArgument(
          VALID_CATALOG_TYPES.contains(Preconditions.checkArgumentNotNull(getCatalogType())),
          "Invalid catalog type. Please pick one of %s",
          VALID_CATALOG_TYPES);
    }
  }
}
