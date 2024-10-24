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

import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
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
import org.apache.iceberg.FileFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * SchemaTransform implementation for {@link IcebergIO#writeRows}. Writes Beam Rows to Iceberg and
 * outputs a {@code PCollection<Row>} representing snapshots created in the process.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<Configuration> {

  static final String INPUT_TAG = "input";
  static final String SNAPSHOTS_TAG = "snapshots";

  static final Schema OUTPUT_SCHEMA =
      Schema.builder().addStringField("table").addFields(SnapshotInfo.SCHEMA.getFields()).build();

  @Override
  public String description() {
    return "Writes Beam Rows to Iceberg.\n"
        + "Returns a PCollection representing the snapshots produced in the process, with the following schema:\n"
        + "{\"table\" (str), \"operation\" (str), \"summary\" (map[str, str]), \"manifestListLocation\" (str)}";
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_IcebergWriteSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Identifier of the Iceberg table.")
    public abstract String getTable();

    @SchemaFieldDescription("Name of the catalog containing the table.")
    public abstract @Nullable String getCatalogName();

    @SchemaFieldDescription("Properties used to set up the Iceberg catalog.")
    public abstract @Nullable Map<String, String> getCatalogProperties();

    @SchemaFieldDescription("Properties passed to the Hadoop Configuration.")
    public abstract @Nullable Map<String, String> getConfigProperties();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the frequency at which snapshots are produced.")
    public abstract @Nullable Integer getTriggeringFrequencySeconds();

    @SchemaFieldDescription(
        "A list of field names to keep in the input record. All other fields are dropped before writing. "
            + "Is mutually exclusive with 'drop' and 'only'.")
    public abstract @Nullable List<String> getKeep();

    @SchemaFieldDescription(
        "A list of field names to drop from the input record before writing. "
            + "Is mutually exclusive with 'keep' and 'only'.")
    public abstract @Nullable List<String> getDrop();

    @SchemaFieldDescription(
        "The name of a single record field that should be written. "
            + "Is mutually exclusive with 'keep' and 'drop'.")
    public abstract @Nullable String getOnly();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCatalogName(String catalogName);

      public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      public abstract Builder setConfigProperties(Map<String, String> confProperties);

      public abstract Builder setTriggeringFrequencySeconds(Integer triggeringFrequencySeconds);

      public abstract Builder setKeep(List<String> keep);

      public abstract Builder setDrop(List<String> drop);

      public abstract Builder setOnly(String only);

      public abstract Configuration build();
    }

    public IcebergCatalogConfig getIcebergCatalog() {
      return IcebergCatalogConfig.builder()
          .setCatalogName(getCatalogName())
          .setCatalogProperties(getCatalogProperties())
          .setConfigProperties(getConfigProperties())
          .build();
    }
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new IcebergWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(SNAPSHOTS_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_WRITE);
  }

  static class IcebergWriteSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    IcebergWriteSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
        return SchemaRegistry.createDefault()
            .getToRowFunction(Configuration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);

      IcebergIO.WriteRows writeTransform =
          IcebergIO.writeRows(configuration.getIcebergCatalog())
              .to(
                  new PortableIcebergDestinations(
                      configuration.getTable(),
                      FileFormat.PARQUET.toString(),
                      rows.getSchema(),
                      configuration.getDrop(),
                      configuration.getKeep(),
                      configuration.getOnly()));

      Integer trigFreq = configuration.getTriggeringFrequencySeconds();
      if (trigFreq != null) {
        writeTransform = writeTransform.withTriggeringFrequency(Duration.standardSeconds(trigFreq));
      }

      // TODO: support dynamic destinations
      IcebergWriteResult result = rows.apply(writeTransform);

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new SnapshotToRow()))
              .setRowSchema(OUTPUT_SCHEMA);

      return PCollectionRowTuple.of(SNAPSHOTS_TAG, snapshots);
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
