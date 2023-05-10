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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigtable.BigTableReadSchemaTransformProvider.BigTableReadSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigTable Read jobs configured via
 * {@link BigTableReadSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class BigTableReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigTableReadSchemaTransformConfiguration> {

  private static final String OUTPUT_TAG = "output";

  public static final Schema CELL_SCHEMA =
      Schema.builder()
          .addStringField("value")
          .addInt64Field("timestamp")
          .addArrayField("labels", Schema.FieldType.array(Schema.FieldType.STRING))
          .build();

  public static final Schema ROW_SCHEMA =
      Schema.builder()
          .addStringField("key")
          .addMapField(
              "families",
              Schema.FieldType.STRING,
              Schema.FieldType.map(
                  Schema.FieldType.STRING,
                  Schema.FieldType.array(Schema.FieldType.row(CELL_SCHEMA))))
          .build();

  @Override
  protected Class<BigTableReadSchemaTransformConfiguration> configurationClass() {
    return BigTableReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(BigTableReadSchemaTransformConfiguration configuration) {
    return new BigTableReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigtable_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /** Configuration for reading from BigTable. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigTableReadSchemaTransformConfiguration {
    /** Instantiates a {@link BigTableReadSchemaTransformConfiguration.Builder} instance. */
    public static Builder builder() {
      return new AutoValue_BigTableReadSchemaTransformProvider_BigTableReadSchemaTransformConfiguration
          .Builder();
    }

    public abstract String getTable();

    public abstract String getInstance();

    public abstract String getProject();

    /** Builder for the {@link BigTableReadSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setInstance(String instance);

      public abstract Builder setProject(String project);

      abstract BigTableReadSchemaTransformConfiguration autoBuild();

      /** Builds a {@link BigTableReadSchemaTransformConfiguration} instance. */
      public BigTableReadSchemaTransformConfiguration build() {
        BigTableReadSchemaTransformConfiguration config = autoBuild();

        String invalidConfigMessage =
            "Invalid BigTable Read configuration: %s should not be a non-empty String";
        checkArgument(!config.getTable().isEmpty(), String.format(invalidConfigMessage, "table"));
        checkArgument(
            !config.getInstance().isEmpty(), String.format(invalidConfigMessage, "instance"));
        checkArgument(
            !config.getProject().isEmpty(), String.format(invalidConfigMessage, "project"));

        return config;
      };
    }
  }

  /**
   * A {@link SchemaTransform} for Bigtable reads, configured with {@link
   * BigTableReadSchemaTransformConfiguration} and instantiated by {@link
   * BigTableReadSchemaTransformProvider}.
   */
  private static class BigTableReadSchemaTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> implements SchemaTransform {
    private final BigTableReadSchemaTransformConfiguration configuration;

    BigTableReadSchemaTransform(BigTableReadSchemaTransformConfiguration configuration) {
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
                      .withTableId(configuration.getTable())
                      .withInstanceId(configuration.getInstance())
                      .withProjectId(configuration.getProject()));

      PCollection<Row> beamRows =
          bigtableRows.apply(MapElements.via(new BigTableRowToBeamRow())).setRowSchema(ROW_SCHEMA);

      return PCollectionRowTuple.of(OUTPUT_TAG, beamRows);
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return this;
    }
  }

  public static class BigTableRowToBeamRow extends SimpleFunction<com.google.bigtable.v2.Row, Row> {
    @Override
    public Row apply(com.google.bigtable.v2.Row bigtableRow) {
      // The collection of families is represented as a Map of column families.
      // Each column family is represented as a Map of columns.
      // Each column is represented as a List of cells
      // Each cell is represented as a Beam Row consisting of value, timestamp, and labels
      Map<String, Map<String, List<Row>>> families = new HashMap<>();

      for (Family fam : bigtableRow.getFamiliesList()) {
        // Map of column qualifier to list of cells
        Map<String, List<Row>> columns = new HashMap<>();
        for (Column col : fam.getColumnsList()) {
          List<Row> cells = new ArrayList<>();
          for (Cell cell : col.getCellsList()) {
            Row cellRow =
                Row.withSchema(CELL_SCHEMA)
                    .withFieldValue("value", cell.getValue().toStringUtf8())
                    .withFieldValue("timestamp", cell.getTimestampMicros())
                    .withFieldValue("labels", cell.getLabelsList())
                    .build();
            cells.add(cellRow);
          }
          columns.put(col.getQualifier().toStringUtf8(), cells);
        }
        families.put(fam.getName(), columns);
      }
      Row beamRow =
          Row.withSchema(ROW_SCHEMA)
              .withFieldValue("key", bigtableRow.getKey().toStringUtf8())
              .withFieldValue("families", families)
              .build();
      return beamRow;
    }
  }
}
