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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider.BigtableReadSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Bigtable Read jobs configured via
 * {@link BigtableReadSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class BigtableReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigtableReadSchemaTransformConfiguration> {
  private static final String OUTPUT_TAG = "output";

  public static final Schema CELL_SCHEMA =
      Schema.builder().addByteArrayField("value").addInt64Field("timestamp_micros").build();

  public static final Schema ROW_SCHEMA =
      Schema.builder()
          .addByteArrayField("key")
          .addMapField(
              "column_families",
              Schema.FieldType.STRING,
              Schema.FieldType.map(
                  Schema.FieldType.STRING,
                  Schema.FieldType.array(Schema.FieldType.row(CELL_SCHEMA))))
          .build();
  public static final Schema FLATTENED_ROW_SCHEMA =
      Schema.builder()
          .addByteArrayField("key")
          .addStringField("family_name")
          .addByteArrayField("column_qualifier")
          .addArrayField("cells", Schema.FieldType.row(CELL_SCHEMA))
          .build();

  @Override
  protected SchemaTransform from(BigtableReadSchemaTransformConfiguration configuration) {
    return new BigtableReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigtable_read:v1";
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /** Configuration for reading from Bigtable. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigtableReadSchemaTransformConfiguration implements Serializable {
    /** Instantiates a {@link BigtableReadSchemaTransformConfiguration.Builder} instance. */
    public void validate() {
      String emptyStringMessage =
          "Invalid Bigtable Read configuration: %s should not be a non-empty String";
      checkArgument(!this.getTableId().isEmpty(), String.format(emptyStringMessage, "table"));
      checkArgument(!this.getInstanceId().isEmpty(), String.format(emptyStringMessage, "instance"));
      checkArgument(!this.getProjectId().isEmpty(), String.format(emptyStringMessage, "project"));
    }

    public static Builder builder() {
      return new AutoValue_BigtableReadSchemaTransformProvider_BigtableReadSchemaTransformConfiguration
              .Builder()
          .setFlatten(true);
    }

    public abstract String getTableId();

    public abstract String getInstanceId();

    public abstract String getProjectId();

    public abstract @Nullable Boolean getFlatten();

    /** Builder for the {@link BigtableReadSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTableId(String tableId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setProjectId(String projectId);

      public abstract Builder setFlatten(Boolean flatten);

      /** Builds a {@link BigtableReadSchemaTransformConfiguration} instance. */
      public abstract BigtableReadSchemaTransformConfiguration build();
    }
  }

  /**
   * A {@link SchemaTransform} for Bigtable reads, configured with {@link
   * BigtableReadSchemaTransformConfiguration} and instantiated by {@link
   * BigtableReadSchemaTransformProvider}.
   */
  private static class BigtableReadSchemaTransform extends SchemaTransform {
    private final BigtableReadSchemaTransformConfiguration configuration;

    BigtableReadSchemaTransform(BigtableReadSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.getAll().isEmpty(),
          String.format(
              "Input to %s is expected to be empty, but is not.", getClass().getSimpleName()));

      PCollection<com.google.bigtable.v2.Row> bigtableRows =
          input
              .getPipeline()
              .apply(
                  BigtableIO.read()
                      .withTableId(configuration.getTableId())
                      .withInstanceId(configuration.getInstanceId())
                      .withProjectId(configuration.getProjectId()));

      Schema outputSchema =
          Boolean.FALSE.equals(configuration.getFlatten()) ? ROW_SCHEMA : FLATTENED_ROW_SCHEMA;

      PCollection<Row> beamRows =
          bigtableRows
              .apply("ConvertToBeamRows", ParDo.of(new BigtableRowConverterDoFn(configuration)))
              .setRowSchema(outputSchema);

      return PCollectionRowTuple.of(OUTPUT_TAG, beamRows);
    }
  }

  /**
   * A {@link DoFn} that converts a Bigtable {@link com.google.bigtable.v2.Row} to a Beam {@link
   * Row}. It supports both a nested representation and a flattened representation where each column
   * becomes a separate output element.
   */
  private static class BigtableRowConverterDoFn extends DoFn<com.google.bigtable.v2.Row, Row> {
    private final BigtableReadSchemaTransformConfiguration configuration;

    BigtableRowConverterDoFn(BigtableReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    private List<Row> convertCells(List<Cell> bigtableCells) {
      List<Row> beamCells = new ArrayList<>();
      for (Cell cell : bigtableCells) {
        Row cellRow =
            Row.withSchema(CELL_SCHEMA)
                .withFieldValue("value", cell.getValue().toByteArray())
                .withFieldValue("timestamp_micros", cell.getTimestampMicros())
                .build();
        beamCells.add(cellRow);
      }
      return beamCells;
    }

    @ProcessElement
    public void processElement(
        @Element com.google.bigtable.v2.Row bigtableRow, OutputReceiver<Row> out) {
      // The builder defaults flatten to true. We check for an explicit false setting to disable it.

      if (Boolean.FALSE.equals(configuration.getFlatten())) {
        // Non-flattening logic (original behavior): one output row per Bigtable row.
        Map<String, Map<String, List<Row>>> families = new HashMap<>();
        for (Family fam : bigtableRow.getFamiliesList()) {
          Map<String, List<Row>> columns = new HashMap<>();
          for (Column col : fam.getColumnsList()) {

            List<Cell> bigTableCells = col.getCellsList();

            List<Row> cells = convertCells(bigTableCells);

            columns.put(col.getQualifier().toStringUtf8(), cells);
          }
          families.put(fam.getName(), columns);
        }
        Row beamRow =
            Row.withSchema(ROW_SCHEMA)
                .withFieldValue("key", bigtableRow.getKey().toByteArray())
                .withFieldValue("column_families", families)
                .build();
        out.output(beamRow);
      } else {
        // Flattening logic (new behavior): one output row per column qualifier.
        byte[] key = bigtableRow.getKey().toByteArray();
        for (Family fam : bigtableRow.getFamiliesList()) {
          String familyName = fam.getName();
          for (Column col : fam.getColumnsList()) {
            ByteString qualifierName = col.getQualifier();
            List<Row> cells = new ArrayList<>();
            for (Cell cell : col.getCellsList()) {
              Row cellRow =
                  Row.withSchema(CELL_SCHEMA)
                      .withFieldValue("value", cell.getValue().toByteArray())
                      .withFieldValue("timestamp_micros", cell.getTimestampMicros())
                      .build();
              cells.add(cellRow);
            }

            Row flattenedRow =
                Row.withSchema(FLATTENED_ROW_SCHEMA)
                    .withFieldValue("key", key)
                    .withFieldValue("family_name", familyName)
                    .withFieldValue("column_qualifier", qualifierName.toByteArray())
                    .withFieldValue("cells", cells)
                    .build();
            out.output(flattenedRow);
          }
        }
      }
    }
  }
}
