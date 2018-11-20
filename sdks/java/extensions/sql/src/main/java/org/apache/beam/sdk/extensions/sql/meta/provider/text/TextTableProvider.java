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
package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.csvLines2BeamRows;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;

/**
 * Text table provider.
 *
 * <p>A sample of text table is:
 *
 * <pre>{@code
 * CREATE TABLE ORDERS(
 *   ID INT COMMENT 'this is the primary key',
 *   NAME VARCHAR(127) COMMENT 'this is the name'
 * )
 * TYPE 'text'
 * COMMENT 'this is the table orders'
 * LOCATION '/home/admin/orders'
 * TBLPROPERTIES '{"format":"csv", "csvformat": "Excel"}' -- format of each text line(csv format)
 * }</pre>
 */
@AutoService(TableProvider.class)
public class TextTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "text";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    Schema schema = table.getSchema();

    String filePattern = table.getLocation();
    JSONObject properties = table.getProperties();
    String format = MoreObjects.firstNonNull(properties.getString("format"), "csv");

    // Backwards compatibility: previously "type": "text" meant CSV and "format" was where the
    // CSV format went. So assume that any other format is the CSV format.
    @Nullable String legacyCsvFormat = null;
    if (!ImmutableSet.of("csv", "lines").contains(format)) {
      legacyCsvFormat = format;
      format = "csv";
    }

    switch (format) {
      case "csv":
        String specifiedCsvFormat = properties.getString("csvformat");
        CSVFormat csvFormat =
            specifiedCsvFormat != null
                ? CSVFormat.valueOf(specifiedCsvFormat)
                : (legacyCsvFormat != null
                    ? CSVFormat.valueOf(legacyCsvFormat)
                    : CSVFormat.DEFAULT);
        return new TextTable(
            schema, filePattern, new CsvToRow(schema, csvFormat), new RowToCsv(csvFormat));
      case "lines":
        checkArgument(
            schema.getFieldCount() == 1
                && schema.getField(0).getType().equals(Schema.FieldType.STRING),
            "Table with type 'text' and format 'lines' "
                + "must have exactly one STRING/VARCHAR/CHAR column");
        return new TextTable(
            schema, filePattern, new LinesReadConverter(), new LinesWriteConverter());
      default:
        throw new IllegalArgumentException(
            "Table with type 'text' must have format 'csv' or 'lines'");
    }
  }

  /** Write-side converter for for {@link TextTable} with format {@code 'lines'}. */
  public static class LinesWriteConverter extends PTransform<PCollection<Row>, PCollection<String>>
      implements Serializable {
    private static final Schema SCHEMA = Schema.builder().addStringField("line").build();

    public LinesWriteConverter() {}

    @Override
    public PCollection<String> expand(PCollection<Row> input) {
      return input.apply(
          "rowsToLines",
          MapElements.into(TypeDescriptors.strings()).via((Row row) -> row.getString(0) + "\n"));
    }
  }

  /** Read-side converter for {@link TextTable} with format {@code 'lines'}. */
  public static class LinesReadConverter extends PTransform<PCollection<String>, PCollection<Row>>
      implements Serializable {

    private static final Schema SCHEMA = Schema.builder().addStringField("line").build();

    public LinesReadConverter() {}

    @Override
    public PCollection<Row> expand(PCollection<String> input) {
      return input
          .apply(
              "linesToRows",
              MapElements.into(TypeDescriptors.rows())
                  .via(s -> Row.withSchema(SCHEMA).addValue(s).build()))
          .setRowSchema(SCHEMA);
    }
  }

  /** Write-side converter for {@link TextTable} with format {@code 'csv'}. */
  @VisibleForTesting
  static class RowToCsv extends PTransform<PCollection<Row>, PCollection<String>>
      implements Serializable {

    private CSVFormat csvFormat;

    public RowToCsv(CSVFormat csvFormat) {
      this.csvFormat = csvFormat;
    }

    @VisibleForTesting
    public CSVFormat getCsvFormat() {
      return csvFormat;
    }

    @Override
    public PCollection<String> expand(PCollection<Row> input) {
      return input.apply(
          "rowToCsv",
          MapElements.into(TypeDescriptors.strings()).via(row -> beamRow2CsvLine(row, csvFormat)));
    }
  }

  /** Read-side converter for {@link TextTable} with format {@code 'csv'}. */
  @VisibleForTesting
  public static class CsvToRow extends PTransform<PCollection<String>, PCollection<Row>>
      implements Serializable {

    private Schema schema;
    private CSVFormat csvFormat;

    @VisibleForTesting
    public CSVFormat getCsvFormat() {
      return csvFormat;
    }

    public CsvToRow(Schema schema, CSVFormat csvFormat) {
      this.schema = schema;
      this.csvFormat = csvFormat;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> input) {
      return input
          .apply(
              "csvToRow",
              FlatMapElements.into(TypeDescriptors.rows())
                  .via(s -> csvLines2BeamRows(csvFormat, s, schema)))
          .setRowSchema(schema);
    }
  }
}
