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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.Config;
import org.apache.beam.sdk.managed.ManagedTransformConstants;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * SchemaTransform implementation for {@link IcebergIO#writeRows}. Writes Beam Rows to Iceberg and
 * outputs a {@code PCollection<Row>} representing snapshots created in the process.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergWriteSchemaTransformProvider extends TypedSchemaTransformProvider<Config> {

  static final String INPUT_TAG = "input";
  static final String OUTPUT_TAG = "output";

  static final Schema OUTPUT_SCHEMA =
      Schema.builder().addStringField("table").addFields(SnapshotInfo.SCHEMA.getFields()).build();

  @Override
  public String description() {
    return "Writes Beam Rows to Iceberg.\n"
        + "Returns a PCollection representing the snapshots produced in the process, with the following schema:\n"
        + "{\"table\" (str), \"operation\" (str), \"summary\" (map[str, str]), \"manifestListLocation\" (str)}";
  }

  @Override
  protected SchemaTransform from(Config configuration) {
    configuration.validate();
    return new IcebergWriteSchemaTransform(configuration)
        .register(configuration, Config.class, identifier());
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
    return ManagedTransformConstants.ICEBERG_WRITE;
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Config {
    public static Builder builder() {
      return new AutoValue_IcebergWriteSchemaTransformProvider_Config.Builder();
    }

    @SchemaFieldDescription("Identifier of the Iceberg table to write to.")
    public abstract String getTable();

    @SchemaFieldDescription("Configuration parameters used to set up the Iceberg catalog.")
    public abstract IcebergSchemaTransformCatalogConfig getCatalogConfig();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String tables);

      public abstract Builder setCatalogConfig(IcebergSchemaTransformCatalogConfig catalogConfig);

      public abstract Config build();
    }

    public void validate() {
      getCatalogConfig().validate();
    }
  }

  static class IcebergWriteSchemaTransform extends SchemaTransform {
    private final Config configuration;

    IcebergWriteSchemaTransform(Config configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {

      PCollection<Row> rows = input.get(INPUT_TAG);

      IcebergSchemaTransformCatalogConfig catalogConfig = configuration.getCatalogConfig();

      IcebergCatalogConfig.Builder catalogBuilder =
          IcebergCatalogConfig.builder().setName(catalogConfig.getCatalogName());

      if (!Strings.isNullOrEmpty(catalogConfig.getCatalogType())) {
        catalogBuilder = catalogBuilder.setIcebergCatalogType(catalogConfig.getCatalogType());
      }
      if (!Strings.isNullOrEmpty(catalogConfig.getWarehouseLocation())) {
        catalogBuilder = catalogBuilder.setWarehouseLocation(catalogConfig.getWarehouseLocation());
      }

      // TODO: support dynamic destinations
      IcebergWriteResult result =
          rows.apply(
              IcebergIO.writeRows(catalogBuilder.build())
                  .to(TableIdentifier.parse(configuration.getTable())));

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new SnapshotToRow()))
              .setRowSchema(OUTPUT_SCHEMA);

      return PCollectionRowTuple.of(OUTPUT_TAG, snapshots);
    }

    @VisibleForTesting
    static class SnapshotToRow extends SimpleFunction<KV<String, SnapshotInfo>, Row> {
      @Override
      public Row apply(KV<String, SnapshotInfo> input) {
        SnapshotInfo snapshot = input.getValue();

        return Row.withSchema(OUTPUT_SCHEMA)
            .addValue(input.getKey())
            .addValues(snapshot.toRow().getValues())
            .build();
      }
    }
  }
}
