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
package org.apache.beam.io.iceberg;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.*;
import org.apache.beam.io.iceberg.IcebergWriteSchemaTransformProvider.Config;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoService(SchemaTransformProvider.class)
public class IcebergWriteSchemaTransformProvider extends TypedSchemaTransformProvider<Config> {

  static final String INPUT_TAG = "input";
  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(Config configuration) {
    configuration.validate();
    return new IcebergWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:iceberg_write:v1";
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Config {
    public static Builder builder() {
      return new AutoValue_IcebergWriteSchemaTransformProvider_Config.Builder();
    }

    public abstract String getTable();

    public abstract CatalogConfig getCatalogConfig();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String tables);

      public abstract Builder setCatalogConfig(CatalogConfig catalogConfig);

      public abstract Config build();
    }

    public void validate() {
      getCatalogConfig().validate();
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class CatalogConfig {
    public static Builder builder() {
      return new AutoValue_IcebergWriteSchemaTransformProvider_CatalogConfig.Builder();
    }

    public abstract String getCatalogName();

    public abstract @Nullable String getCatalogType();

    public abstract @Nullable String getCatalogImplementation();

    public abstract @Nullable String getWarehouseLocation();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setCatalogName(String catalogName);

      public abstract Builder setCatalogType(String catalogType);

      public abstract Builder setCatalogImplementation(String catalogImplementation);

      public abstract Builder setWarehouseLocation(String warehouseLocation);

      public abstract CatalogConfig build();
    }

    Set<String> validTypes =
        Sets.newHashSet(
            CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
            CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    public void validate() {
      if (Strings.isNullOrEmpty(getCatalogType())) {
        checkArgument(
            validTypes.contains(Preconditions.checkArgumentNotNull(getCatalogType())),
            "Invalid catalog type. Please pick one of %s",
            validTypes);
      }
    }
  }

  @VisibleForTesting
  static class IcebergWriteSchemaTransform extends SchemaTransform {
    private final Config configuration;

    IcebergWriteSchemaTransform(Config configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {

      PCollection<Row> rows = input.get(INPUT_TAG);

      CatalogConfig catalogConfig = configuration.getCatalogConfig();

      IcebergCatalogConfig.Builder catalogBuilder =
          IcebergCatalogConfig.builder()
              .setName(catalogConfig.getCatalogName())
              .setIcebergCatalogType(catalogConfig.getCatalogType())
              .setWarehouseLocation(catalogConfig.getWarehouseLocation());

      if (!Strings.isNullOrEmpty(catalogConfig.getCatalogType())) {
        catalogBuilder = catalogBuilder.setIcebergCatalogType(catalogConfig.getCatalogType());
      }
      if (!Strings.isNullOrEmpty(catalogConfig.getWarehouseLocation())) {
        catalogBuilder = catalogBuilder.setWarehouseLocation(catalogConfig.getWarehouseLocation());
      }

      // TODO: support dynamic destinations
      DynamicDestinations dynamicDestinations =
          DynamicDestinations.singleTable(TableIdentifier.parse(configuration.getTable()));

      IcebergWriteResult result =
          rows.apply(
              IcebergIO.writeToDynamicDestinations(catalogBuilder.build(), dynamicDestinations));

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new SnapshotToRow()))
              .setRowSchema(SnapshotToRow.SNAPSHOT_SCHEMA);

      return PCollectionRowTuple.of(OUTPUT_TAG, snapshots);
    }

    @VisibleForTesting
    static class SnapshotToRow extends SimpleFunction<KV<String, Snapshot>, Row> {
      static final Schema SNAPSHOT_SCHEMA =
          Schema.builder()
              .addStringField("table")
              .addStringField("operation")
              .addMapField("summary", Schema.FieldType.STRING, Schema.FieldType.STRING)
              .addStringField("manifestListLocation")
              .build();

      @Override
      public Row apply(KV<String, Snapshot> input) {
        Snapshot snapshot = input.getValue();
        Row row =
            Row.withSchema(SNAPSHOT_SCHEMA)
                .addValues(
                    input.getKey(),
                    snapshot.operation(),
                    snapshot.summary(),
                    snapshot.manifestListLocation())
                .build();
        System.out.println("SNAPSHOT: " + snapshot);
        System.out.println("ROW: " + row);
        return row;
      }
    }
  }
}
